import configparser
import os
import sqlite3
from dotenv import load_dotenv
import pymysql
from datetime import datetime, timedelta
from core.logger import logger

load_dotenv()
current_dir = os.path.dirname(os.path.abspath(__file__))
# 加载 config.ini
config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.ini')
config = configparser.ConfigParser()
config.read(config_file, encoding='utf-8')

# 获取字段映射
FIELD_MAPPING = {}
for section in config.sections():
    if section.startswith('fields'):
        for key, value in config.items(section):
            FIELD_MAPPING[value] = key  # 中文 -> 英文


def sync_explore_data_merge_to_remote(table_name_list=None,
                                      merged_table_name="s_xhs_merged_data_ocr",
                                      merge_type="related",
                                      business_time_filter=None,
                                      target_db="remote",
                                      related_key=None):
    """
    将多个表的数据融合后同步到数据库中
    
    参数:
    table_name_list: 要融合的表名列表
    merged_table_name: 融合后数据存储的目标表名
    merge_type: 融合类型，可选"related"(关联融合)或"unrelated"(非关联融合)
    business_time_filter: 业务时间筛选条件，格式为字典{"column": "采集时间", "days": 3}表示最近3天数据
    target_db: 目标数据库，可选"remote"(远程MySQL)或"local"(本地SQLite)
    related_key: 关联融合时使用的关联键字段名，可以是字符串或字符串列表，默认为None
    
    融合规则：
    1. 关联融合：以指定字段作为关联键进行行合并
    2. 非关联融合：简单地将所有表的列合并，行数据分别保留
    """
    # 修复可变默认参数问题
    if table_name_list is None:
        table_name_list = []

    try:
        # 获取ExploreData.db路径
        db_path = os.path.join(current_dir, 'ocr_data.db')
        logger.debug(f"使用数据库文件路径: {db_path}")

        # 检查数据库文件是否存在
        if not os.path.exists(db_path):
            logger.info("ocr_data.db 文件不存在，跳过数据同步")
            return

        # 连接本地SQLite数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 检查所有表是否存在，如果其中任何一张表不存在，则不进行融合同步
        existing_tables = []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for row in cursor.fetchall():
            existing_tables.append(row[0])

        logger.debug(f"数据库中已存在的表: {existing_tables}")

        for table_name in table_name_list:
            # 检查表名是否在现有表中（不区分大小写）
            table_exists = any(table_name.lower() == existing_table.lower() for existing_table in existing_tables)
            if not table_exists:
                logger.warning(f"表 {table_name} 不存在，中止融合同步")
                conn.close()
                return

        # 收集所有表的数据
        all_table_data = {}
        all_columns = set()

        for table_name in table_name_list:
            # 构建查询语句
            if business_time_filter and business_time_filter.get("column") and business_time_filter.get("days"):
                # 如果有时间筛选条件，则只查询最近N天的数据
                time_column = business_time_filter["column"]
                days = business_time_filter["days"]
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
                query = f"SELECT * FROM {table_name} WHERE {time_column} >= ?"
                cursor.execute(query, (cutoff_date,))
                logger.debug(f"执行查询: {query} 参数: {cutoff_date}")
            else:
                # 查询所有数据
                query = f"SELECT * FROM {table_name}"
                cursor.execute(query)
                logger.debug(f"执行查询: {query}")

            rows = cursor.fetchall()
            logger.debug(f"查询结果行数: {len(rows)}")

            # 获取列名
            column_names = [description[0] for description in cursor.description]
            logger.debug(f"列名: {column_names}")

            # 存储表数据
            all_table_data[table_name] = {
                'columns': column_names,
                'rows': rows
            }

            # 收集所有列名
            all_columns.update(column_names)

            # 记录每张表的数据行数
            logger.info(f"表 {table_name} 查询到 {len(rows)} 行数据")

            # 验证查询结果
            if rows:
                logger.debug(f"表 {table_name} 前3行数据示例: {rows[:3]}")
            else:
                # 检查表是否真的存在但没有数据
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_count = cursor.fetchone()[0]
                logger.debug(f"表 {table_name} 总行数: {total_count}")

        # 根据融合类型构建数据
        merged_columns = list(all_columns)

        # 记录融合前的信息
        related_key_desc = related_key if isinstance(related_key, str) else ", ".join(related_key) if isinstance(
            related_key, list) else str(related_key) if related_key else "无"
        logger.info(f"开始进行 {merge_type} 类型的数据融合，关联键: {related_key_desc}")
        logger.info(f"融合后的表结构包含 {len(merged_columns)} 列: {merged_columns}")

        if merge_type == "related":
            merged_rows = merge_table_data_related(all_table_data, merged_columns, related_key)
            logger.info(f"使用关联融合方式处理数据，融合后共有 {len(merged_rows)} 行数据")
        else:  # unrelated
            merged_rows = merge_table_data_unrelated(all_table_data, merged_columns)
            logger.info(f"使用非关联融合方式处理数据，融合后共有 {len(merged_rows)} 行数据")

        # 记录融合后的数据示例
        if merged_rows:
            logger.debug(f"融合后前3行数据示例: {merged_rows[:3]}")

        # 根据目标数据库类型进行同步
        if target_db == "remote":
            # 同步到远程MySQL数据库
            db_config = {
                "host": os.getenv("MYSQL_HOST", "localhost"),
                "port": int(os.getenv("MYSQL_PORT", 3306)),
                "user": os.getenv("MYSQL_USER", ""),
                "password": os.getenv("MYSQL_PASSWORD", ""),
                "database": os.getenv("MYSQL_DATABASE", "")
            }

            # 如果没有配置数据库，则跳过同步
            if not all([db_config["host"], db_config["user"], db_config["password"], db_config["database"]]):
                logger.warning("未配置远程数据库，跳过数据同步")
                conn.close()
                return

            sync_to_mysql(db_config, merged_table_name, merged_columns, merged_rows, related_key)
            logger.info(f"融合数据已同步到远程MySQL数据库，表名: {merged_table_name}，融合类型: {merge_type}")
        else:
            # 保存到本地SQLite数据库
            sync_to_local_sqlite(db_path, merged_table_name, merged_columns, merged_rows, related_key)
            logger.info(f"融合数据已保存到本地SQLite数据库，表名: {merged_table_name}，融合类型: {merge_type}")

        # 关闭本地数据库连接
        conn.close()

    except Exception as e:
        logger.error(f"融合同步数据时出错: {str(e)}")


def merge_table_data_related(all_table_data, merged_columns, related_key):
    """
    关联融合：根据指定字段合并多个表的数据，并生成新的数据ID
    related_key 可以是字符串（单个字段）或字符串列表（多个字段组合作为关联键）
    """
    # 创建一个字典来存储合并后的数据，以关联键为键
    merged_data = {}
    related_items = {}  # 记录哪些关联键进行了关联

    # 确保 related_key 是列表格式
    if isinstance(related_key, str):
        related_key_fields = [related_key]
    else:
        related_key_fields = related_key

    # 遍历每个表的数据
    for table_name, table_data in all_table_data.items():
        columns = table_data['columns']
        rows = table_data['rows']

        # 找到所有关联键列的索引
        key_indices = []
        for key_field in related_key_fields:
            key_index = columns.index(key_field) if key_field in columns else -1
            key_indices.append(key_index)

        # 检查是否所有关联键字段都存在
        if -1 in key_indices:
            missing_fields = [related_key_fields[i] for i, idx in enumerate(key_indices) if idx == -1]
            logger.warning(f"表 {table_name} 中未找到关联键列 {missing_fields}，跳过该表的合并")
            continue

        # 遍历每一行数据
        for row in rows:
            # 使用多个字段的值组合作为关联键
            related_value = tuple(row[idx] for idx in key_indices) if len(key_indices) > 1 else row[key_indices[0]]

            # 如果该关联值还没有记录，则创建新记录
            if related_value not in merged_data:
                merged_data[related_value] = [''] * len(merged_columns)
                related_items[related_value] = False  # 初始化为未关联
            else:
                # 如果已经存在记录，标记为已关联
                if not related_items[related_value]:
                    related_items[related_value] = True

            # 将当前行的数据填充到合并后的记录中
            for i, col_name in enumerate(columns):
                if col_name in merged_columns:
                    col_index = merged_columns.index(col_name)
                    # 只有当目标位置为空或者当前值不为空时才更新
                    if merged_data[related_value][col_index] == '' or row[i] != '':
                        merged_data[related_value][col_index] = row[i]

    # 统计实际关联的项目数
    actual_related_count = sum(1 for is_related in related_items.values() if is_related)

    logger.info(f"关联融合完成，共关联 {actual_related_count} 个项目（具有来自多个表的数据）")
    # 将字典转换为列表格式
    return list(merged_data.values())


def merge_table_data_unrelated(all_table_data, merged_columns):
    """
    非关联融合：将所有表的行数据简单合并，不进行关联
    """
    merged_rows = []

    # 遍历每个表的数据
    for table_name, table_data in all_table_data.items():
        columns = table_data['columns']
        rows = table_data['rows']

        # 为每一行创建完整列的记录
        for row in rows:
            # 创建一个新的完整行记录
            full_row = [''] * len(merged_columns)

            # 将当前行的数据填充到完整记录中
            for i, col_name in enumerate(columns):
                if col_name in merged_columns:
                    col_index = merged_columns.index(col_name)
                    full_row[col_index] = row[i]

            merged_rows.append(full_row)

    logger.info(f"非关联融合完成，共处理 {len(merged_rows)} 行数据，其中包括所有原始数据行（即使无关联）")
    return merged_rows


def sync_to_mysql(db_config, table_name, column_names, rows, related_key=None):
    """
    同步数据到MySQL数据库
    支持表不存在时创建表，字段不存在时新增字段
    """
    try:
        # 创建MySQL连接
        mysql_conn = pymysql.connect(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 3306),
            user=db_config.get("user", ""),
            password=db_config.get("password", ""),
            database=db_config.get("database", ""),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with mysql_conn.cursor() as cursor:
                # 检查表是否存在
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                """, (db_config.get("database", ""), table_name))

                table_exists = cursor.fetchone()['COUNT(*)'] > 0

                # 如果表不存在，则创建表
                if not table_exists:
                    logger.info(f"表 {table_name} 不存在，正在创建...")
                    create_table_if_not_exists(cursor, table_name, column_names, related_key)
                else:
                    # 如果表存在，检查是否有新增字段需要添加
                    logger.info(f"表 {table_name} 已存在，检查是否需要新增字段...")
                    add_missing_columns(cursor, table_name, column_names, db_config.get("database", ""))

                if rows:
                    # 构建插入语句，排除id字段，因为它是自增的
                    filtered_columns = [col for col in column_names if col != 'id']
                    placeholders = ", ".join(["%s"] * len(filtered_columns))

                    # 映射列名为英文名
                    mapped_column_names = []
                    for col in filtered_columns:
                        if col in FIELD_MAPPING:
                            mapped_column_names.append(FIELD_MAPPING[col])
                        else:
                            mapped_column_names.append(col)

                    columns_str = ", ".join([f"`{col}`" for col in mapped_column_names])

                    # 构建ON DUPLICATE KEY UPDATE部分
                    update_fields = []
                    for i, col in enumerate(filtered_columns):
                        if col not in ("id", "数据ID"):
                            eng_col = mapped_column_names[i] if col in FIELD_MAPPING else col
                            update_fields.append(f"`{eng_col}` = VALUES(`{eng_col}`)")

                    insert_sql = " ".join(f"""
                                    INSERT INTO {table_name} ({columns_str})
                                    VALUES ({placeholders})
                                    ON DUPLICATE KEY UPDATE {", ".join(update_fields)}
                                    """.split())

                    logger.debug(f'MySQL插入SQL:\n{insert_sql}')
                    # 准备数据用于插入，过滤掉id列的数据
                    insert_data = []
                    for row in rows:
                        # 过滤掉id列的数据
                        filtered_row = []
                        for i, col in enumerate(column_names):
                            if col in filtered_columns:
                                filtered_row.append(row[i])
                        insert_data.append(tuple(filtered_row))

                    cursor.executemany(insert_sql, insert_data)

            # 提交事务
            mysql_conn.commit()
            logger.info(f"成功同步 {len(rows)} 条记录到MySQL数据库")

        finally:
            mysql_conn.close()

    except ImportError:
        logger.error("缺少 pymysql 库，请安装: pip install pymysql")
    except Exception as e:
        logger.error(f"同步到 MySQL 数据库时出错: {str(e)}")


def sync_to_local_sqlite(db_path, table_name, column_names, rows, related_key=None):
    """
    同步数据到本地SQLite数据库
    支持表不存在时创建表，字段不存在时新增字段
    """
    try:
        # 连接本地SQLite数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        logger.debug(f"连接到SQLite数据库: {db_path}")

        # 检查表是否存在
        cursor.execute("""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (table_name,))

        table_exists = cursor.fetchone() is not None
        logger.debug(f"表 {table_name} 是否存在: {table_exists}")

        # 如果表不存在，则创建表
        if not table_exists:
            logger.info(f"表 {table_name} 不存在，正在创建...")
            create_table_if_not_exists_sqlite(cursor, table_name, column_names, related_key)
        else:
            # 如果表存在，检查是否有新增字段需要添加
            logger.info(f"表 {table_name} 已存在，检查是否需要新增字段...")
            add_missing_columns_sqlite(cursor, table_name, column_names)

        if rows:
            # 构建插入语句，排除id字段，因为它是自增的
            filtered_columns = [col for col in column_names if col != 'id']
            placeholders = ", ".join(["?" for _ in filtered_columns])
            columns_str = ", ".join([f'"{col}"' for col in filtered_columns])

            # 构建ON CONFLICT子句，如果related_key存在则使用IGNORE策略避免重复插入
            if related_key:
                # 确保related_key是列表格式
                if isinstance(related_key, str):
                    related_key_fields = [related_key]
                else:
                    related_key_fields = related_key

                # 检查related_key字段是否都在column_names中
                if all(key in filtered_columns for key in related_key_fields):
                    # 构造唯一约束字段
                    unique_columns = ", ".join([f'"{col}"' for col in related_key_fields])
                    insert_sql = f"""
                        INSERT OR IGNORE INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                    """
                    logger.debug(f"使用唯一键约束字段: {unique_columns}")
                else:
                    # 如果related_key字段不全在列中，则使用普通插入
                    insert_sql = f"""
                        INSERT INTO {table_name} ({columns_str})
                        VALUES ({placeholders})
                    """
            else:
                insert_sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                """

            logger.debug(f'SQLite插入SQL:\n{insert_sql}')
            # 准备数据用于插入，过滤掉id列的数据
            insert_data = []
            for row in rows:
                # 过滤掉id列的数据
                filtered_row = []
                for i, col in enumerate(column_names):
                    if col in filtered_columns:
                        filtered_row.append(row[i])
                insert_data.append(tuple(filtered_row))

            cursor.executemany(insert_sql, insert_data)
            logger.debug(f"准备插入 {len(insert_data)} 行数据")

            # 检查实际插入了多少行
            inserted_rows = cursor.rowcount
            logger.debug(f"SQL语句影响的行数: {inserted_rows}")

        # 提交事务
        conn.commit()
        logger.info(f"成功同步 {len(rows)} 条记录到本地SQLite数据库，表名: {table_name}")

        # 验证数据是否已插入
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.debug(f"验证: 表 {table_name} 中现有 {count} 行数据")

        # 如果有数据，显示前几行作为示例
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            sample_rows = cursor.fetchall()
            logger.debug(f"表 {table_name} 数据示例: {sample_rows}")

        # 关闭连接
        conn.close()

    except Exception as e:
        logger.error(f"同步到本地SQLite数据库时出错: {str(e)}")


def create_table_if_not_exists(cursor, table_name, column_names, related_key=None):
    """
    如果表不存在则创建表 (MySQL)
    """
    columns_definitions = []
    
    # 添加自增ID字段作为主键
    columns_definitions.append('`id` BIGINT AUTO_INCREMENT PRIMARY KEY')

    # 处理related_key作为唯一约束
    primary_keys = []
    if related_key:
        # 确保related_key是列表格式
        if isinstance(related_key, str):
            primary_keys = [related_key]
        else:
            primary_keys = related_key

    for col in column_names:
        if col in FIELD_MAPPING:
            eng_col = FIELD_MAPPING[col]
            columns_definitions.append(f"`{col}` TEXT COMMENT '{col}'")
        else:
            columns_definitions.append(f"`{col}` TEXT")

    # 如果有related_key，则将其设置为唯一约束
    unique_constraint = ""
    if primary_keys:
        # 确保所有主键字段都在列定义中
        existing_primary_keys = [key for key in primary_keys if (key in column_names) or (
                key in FIELD_MAPPING and FIELD_MAPPING[key] in [col.split()[0].strip('`') for col in
                                                                columns_definitions])]
        if existing_primary_keys:
            # 映射中文字段名为英文名
            mapped_primary_keys = []
            for key in existing_primary_keys:
                if key in FIELD_MAPPING:
                    mapped_primary_keys.append(FIELD_MAPPING[key])
                else:
                    mapped_primary_keys.append(key)
            unique_constraint = f", UNIQUE KEY unique_constraint ({', '.join([f'`{key}`' for key in mapped_primary_keys])})"

    create_table_sql = " ".join(f"""
    CREATE TABLE {table_name} (
        {", ".join(columns_definitions)}
        {unique_constraint}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """.split())

    logger.debug(f"MySQL创建表SQL: {create_table_sql}")
    cursor.execute(create_table_sql)


def create_table_if_not_exists_sqlite(cursor, table_name, column_names, related_key=None):
    """
    如果表不存在则创建表 (SQLite)
    """
    columns_definitions = []
    #
    # 添加自增ID字段作为主键
    columns_definitions.append('"id" INTEGER PRIMARY KEY AUTOINCREMENT')

    # 处理related_key作为主键或唯一约束
    primary_keys = []
    if related_key:
        # 确保related_key是列表格式
        if isinstance(related_key, str):
            primary_keys = [related_key]
        else:
            primary_keys = related_key

    for col in column_names:
        # 所有字段都作为普通字段处理（除了已经在上面添加的id字段）
        columns_definitions.append(f'"{col}" TEXT')

    # 如果有related_key，则将其设置为唯一约束而不是主键
    unique_constraint = ""
    if primary_keys:
        # 确保所有主键字段都在列定义中
        existing_primary_keys = [key for key in primary_keys if key in column_names]
        if existing_primary_keys:
            unique_constraint = f", UNIQUE ({', '.join([f'\"{key}\"' for key in existing_primary_keys])})"

    create_table_sql = f"""
    CREATE TABLE {table_name} (
        {", ".join(columns_definitions)}
        {unique_constraint}
    )
    """

    logger.debug(f"SQLite创建表SQL: {create_table_sql}")
    cursor.execute(create_table_sql)


def add_missing_columns(cursor, table_name, column_names, database_name):
    """
    为已存在的表添加缺失的字段 (MySQL)
    """
    # 获取表中已存在的字段
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
    """, (database_name, table_name))

    existing_columns = [row['column_name'] for row in cursor.fetchall()]

    # 检查是否有新增字段
    added_columns = []
    for col in column_names:
        # 映射中文字段名为英文名
        eng_col = FIELD_MAPPING.get(col, col)

        # 检查英文字段名是否已存在
        if eng_col not in existing_columns:
            # 添加缺失的字段
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT"

            logger.info(f"添加缺失字段: {alter_sql}")
            try:
                cursor.execute(alter_sql)
                added_columns.append(eng_col)
            except Exception as e:
                logger.error(f"添加字段 {eng_col} 时出错: {str(e)}")

    if added_columns:
        logger.info(f"MySQL表 {table_name} 新增了 {len(added_columns)} 个字段: {added_columns}")


def add_missing_columns_sqlite(cursor, table_name, column_names):
    """
    为已存在的表添加缺失的字段 (SQLite)
    """
    # 获取表中已存在的字段
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]

    # 检查是否有新增字段
    added_columns = []
    for col in column_names:
        if col not in existing_columns:
            # 添加缺失的字段
            alter_sql = f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT'

            logger.info(f"添加缺失字段: {alter_sql}")
            try:
                cursor.execute(alter_sql)
                added_columns.append(col)
            except Exception as e:
                logger.error(f"添加字段 {col} 时出错: {str(e)}")

    if added_columns:
        logger.info(f"SQLite表 {table_name} 新增了 {len(added_columns)} 个字段: {added_columns}")

