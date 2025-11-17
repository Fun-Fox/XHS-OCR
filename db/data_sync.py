import configparser
import os
from dotenv import load_dotenv
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


# 添加数据库同步功能
def sync_explore_data_to_remote(table_name_list=None, time_filter=None, unique_constraints=None):
    """
    将Download文件夹下的ExploreData.db sqlite数据同步到远程MySQL数据库中
    
    参数:
    table_name_list: 要同步的表名列表
    time_filter: 时间筛选条件，格式为字典{"column": "采集时间", "days": 3}表示最近3天数据
    unique_constraints: 唯一约束定义，格式为字典{表名: [约束字段列表]}
                        例如: {"table1": ["采集时间"], "table2": [["字段1", "字段2"]]}
    """
    try:
        # 从环境变量获取数据库配置
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
            return

        # 获取ExploreData.db路径
        db_path = os.path.join(current_dir, 'ocr_data.db')

        # 检查数据库文件是否存在
        if not os.path.exists(db_path):
            logger.info("ocr_data.db 文件不存在，跳过数据同步")
            return

        # 连接本地SQLite数据库
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for table_name in table_name_list:
            # 构建查询语句
            if time_filter and time_filter.get("column") and time_filter.get("days"):
                # 如果有时间筛选条件，则只查询最近N天的数据
                time_column = time_filter["column"]
                days = time_filter["days"]
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
                query = f"SELECT * FROM {table_name} WHERE {time_column} >= ?"
                cursor.execute(query, (cutoff_date,))
                logger.info(f"执行查询: {query} 参数: {cutoff_date}")
            else:
                # 查询所有数据
                cursor.execute(f"SELECT * FROM {table_name}")
                logger.info(f"执行查询: SELECT * FROM {table_name}")

            rows = cursor.fetchall()
            logger.info(f"表 {table_name} 查询到 {len(rows)} 行数据")

            # 获取列名
            column_names = [description[0] for description in cursor.description]
            # 替换 '采集日期' 为 '采集时间'
            for i, col in enumerate(column_names):
                if col == "采集日期":
                    column_names[i] = "采集时间"

            logger.debug(f"列名: {column_names}")

            # 同步到MySQL数据库
            table_unique_constraints = unique_constraints.get(table_name, []) if unique_constraints else []
            sync_to_mysql(db_config, table_name, column_names, rows, table_unique_constraints)
            logger.info(f"表 {table_name} 数据已同步到远程MySQL数据库")

        # 关闭本地数据库连接
        conn.close()

    except Exception as e:
        logger.error(f"同步数据到远程数据库时出错: {str(e)}")


def sync_to_mysql(db_config, table_name, column_names, rows, unique_constraints=None):
    """
    同步数据到MySQL数据库
    支持表不存在时创建表，字段不存在时新增字段
    """
    try:
        import pymysql
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
                try:
                    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                    table_result = cursor.fetchall()
                    table_exists = len(table_result) > 0
                    logger.debug(f"检查表 {table_name} 是否存在: {table_exists}, 查询结果: {table_result}")
                except Exception as e:
                    logger.warning(f"检查表 {table_name} 是否存在时出错: {str(e)}")
                    table_exists = False

                # 如果表不存在，则创建表
                if not table_exists:
                    logger.info(f"表 {table_name} 不存在，正在创建...")
                    create_table_if_not_exists(cursor, table_name, column_names, unique_constraints)
                else:
                    # 如果表存在，检查是否有新增字段需要添加
                    logger.info(f"表 {table_name} 已存在，检查是否需要新增字段...")
                    add_missing_columns(cursor, table_name, column_names, db_config.get("database", ""))

                if rows:
                    # 映射列名为英文名
                    mapped_column_names = []
                    for col in column_names:
                        if col in FIELD_MAPPING:
                            mapped_column_names.append(FIELD_MAPPING[col])
                        else:
                            mapped_column_names.append(col)

                    columns_str = ", ".join([f"`{col}`" for col in mapped_column_names])

                    # 构建ON DUPLICATE KEY UPDATE部分
                    update_fields = []
                    for i, col in enumerate(column_names):
                        if col not in ("id",):
                            eng_col = mapped_column_names[i] if col in FIELD_MAPPING else col
                            update_fields.append(f"`{eng_col}` = VALUES(`{eng_col}`)")

                    placeholders = ", ".join(["%s"] * len(column_names))
                    insert_sql = " ".join(f"""
                                    INSERT INTO {table_name} ({columns_str})
                                    VALUES ({placeholders})
                                    ON DUPLICATE KEY UPDATE {", ".join(update_fields)}
                                    """.split())

                    logger.debug(f'insert_sql:\n{insert_sql}')
                    cursor.executemany(insert_sql, rows)

            # 提交事务
            mysql_conn.commit()
            logger.info(f"成功同步 {len(rows)} 条记录到MySQL数据库")

        finally:
            mysql_conn.close()

    except ImportError:
        logger.error("缺少 pymysql 库，请安装: pip install pymysql")
    except Exception as e:
        logger.error(f"同步到 MySQL 数据库时出错: {str(e)}")


def create_table_if_not_exists(cursor, table_name, column_names, unique_constraints=None):
    """
    如果表不存在则创建表
    """
    columns_definitions = []
    columns_definitions.append('`id` BIGINT AUTO_INCREMENT PRIMARY KEY')
    unique_keys = []
    
    # 如果传入了唯一约束参数，则使用传入的约束，否则使用默认逻辑
    if unique_constraints:
        # 使用传入的唯一约束
        for constraint in unique_constraints:
            if isinstance(constraint, str):  # 单字段唯一约束
                eng_col = FIELD_MAPPING.get(constraint, constraint)
                unique_keys.append(f"`{eng_col}`")
            elif isinstance(constraint, list):  # 多字段组合唯一约束
                composite_keys = [f"`{FIELD_MAPPING.get(col, col)}`" for col in constraint]
                columns_definitions.append(f"UNIQUE KEY `unique_constraint_{'_'.join(composite_keys)}` ({", ".join(composite_keys)})")
    else:
        # 默认逻辑：将日期和时间字段作为唯一约束
        for col in column_names:
            if col in FIELD_MAPPING:
                eng_col = FIELD_MAPPING[col]

                if col == "采集日期":
                    columns_definitions.append(f"`{eng_col}` DATE COMMENT '{col}'")
                    unique_keys.append(f"`{eng_col}`")
                elif col == "采集时间":
                    columns_definitions.append(f"`{eng_col}` DATETIME COMMENT '{col}'")
                    unique_keys.append(f"`{eng_col}`")
                else:
                    columns_definitions.append(f"`{eng_col}` TEXT COMMENT '{col}'")
            else:
                columns_definitions.append(f"`{col}` TEXT")

    # 添加唯一索引以支持ON DUPLICATE KEY UPDATE
    if len(unique_keys) > 1:  # 只有当有多个字段时才创建组合唯一索引
        columns_definitions.append(f"UNIQUE KEY `unique_constraint` ({", ".join(unique_keys)})")
    elif len(unique_keys) == 1:  # 单个字段设置为唯一
        # 找到对应字段的索引并修改定义
        for i, definition in enumerate(columns_definitions):
            if unique_keys[0] in definition and not definition.startswith('`id'):
                columns_definitions[i] = definition + " UNIQUE"
                break
    
    create_table_sql = " ".join(f"""
    CREATE TABLE {table_name} (
        {", ".join(columns_definitions)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """.split())

    logger.debug(f"Create table SQL: {create_table_sql}")
    cursor.execute(create_table_sql)


def add_missing_columns(cursor, table_name, column_names, database_name):
    """
    为已存在的表添加缺失的字段
    使用 SHOW COLUMNS 作为系统表查询的替代方案
    """
    try:
        # 使用 SHOW COLUMNS 查询表结构
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        existing_columns = [row[0] for row in cursor.fetchall()]
        logger.debug(f"表 {table_name} 的现有字段: {existing_columns}")
    except Exception as e:
        logger.error(f"无法获取表 {table_name} 的字段信息: {str(e)} (数据库: {database_name})")
        return

    # 检查是否有新增字段
    for col in column_names:
        # 映射中文字段名为英文名
        eng_col = FIELD_MAPPING.get(col, col)

        # 检查英文字段名是否已存在
        if eng_col not in existing_columns:
            # 添加缺失的字段
            if col in FIELD_MAPPING:
                if col == "采集日期":
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` DATE COMMENT '{col}'"
                elif col == "采集时间":
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` DATETIME COMMENT '{col}'"
                else:
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` TEXT COMMENT '{col}'"
            else:
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT"

            logger.info(f"添加缺失字段: {alter_sql}")
            try:
                cursor.execute(alter_sql)
            except Exception as e:
                logger.error(f"添加字段 {eng_col} 时出错: {str(e)} (SQL: {alter_sql})")

