import configparser
import os
import sqlite3
from dotenv import load_dotenv
import pymysql
from datetime import datetime, timedelta

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


def sync_explore_data_merge_to_remote(table_name_list=['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr'], 
                                      merged_table_name="s_xhs_merged_data_ocr",
                                      merge_type="related",
                                      business_time_filter=None):
    """
    将多个表的数据融合后同步到远程MySQL数据库中
    
    参数:
    table_name_list: 要融合的表名列表
    merged_table_name: 融合后数据存储的目标表名
    merge_type: 融合类型，可选"related"(关联融合)或"unrelated"(非关联融合)
    business_time_filter: 业务时间筛选条件，格式为字典{"column": "采集时间", "days": 3}表示最近3天数据
    
    融合规则：
    1. 关联融合：以"数据ID"作为关联键进行行合并
    2. 非关联融合：简单地将所有表的列合并，行数据分别保留
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
            print("未配置远程数据库，跳过数据同步")
            return

        # 获取ExploreData.db路径
        db_path = os.path.join(current_dir, 'ocr_data.db')

        # 检查数据库文件是否存在
        if not os.path.exists(db_path):
            print("ocr_data.db 文件不存在，跳过数据同步")
            return

        # 连接本地SQLite数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查所有表是否存在，如果其中任何一张表不存在，则不进行融合同步
        existing_tables = []
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for row in cursor.fetchall():
            existing_tables.append(row[0])
        
        for table_name in table_name_list:
            if table_name not in existing_tables:
                print(f"表 {table_name} 不存在，中止融合同步")
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
                cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
                query = f"SELECT * FROM {table_name} WHERE {time_column} >= ?"
                cursor.execute(query, (cutoff_date,))
            else:
                # 查询所有数据
                cursor.execute(f"SELECT * FROM {table_name}")
            
            rows = cursor.fetchall()

            # 获取列名
            column_names = [description[0] for description in cursor.description]
            
            # 存储表数据
            all_table_data[table_name] = {
                'columns': column_names,
                'rows': rows
            }
            
            # 收集所有列名
            all_columns.update(column_names)
        
        # 根据融合类型构建数据
        merged_columns = list(all_columns)
        if merge_type == "related":
            merged_rows = merge_table_data_related(all_table_data, merged_columns)
            print(f"使用关联融合方式处理数据")
        else:  # unrelated
            merged_rows = merge_table_data_unrelated(all_table_data, merged_columns)
            print(f"使用非关联融合方式处理数据")
        
        # 同步到MySQL数据库
        sync_to_mysql(db_config, merged_table_name, merged_columns, merged_rows)
        print(f"融合数据已同步到远程MySQL数据库，表名: {merged_table_name}，融合类型: {merge_type}")
        
        # 关闭本地数据库连接
        conn.close()

    except Exception as e:
        print(f"融合同步数据到远程数据库时出错: {str(e)}")


def merge_table_data_related(all_table_data, merged_columns):
    """
    关联融合：根据数据ID合并多个表的数据
    """
    # 创建一个字典来存储合并后的数据，以数据ID为键
    merged_data = {}
    
    # 遍历每个表的数据
    for table_name, table_data in all_table_data.items():
        columns = table_data['columns']
        rows = table_data['rows']
        
        # 找到数据ID列的索引
        id_index = columns.index('数据ID') if '数据ID' in columns else -1
        
        if id_index == -1:
            print(f"表 {table_name} 中未找到数据ID列，跳过该表的合并")
            continue
            
        # 遍历每一行数据
        for row in rows:
            data_id = row[id_index]
            
            # 如果该数据ID还没有记录，则创建新记录
            if data_id not in merged_data:
                merged_data[data_id] = [''] * len(merged_columns)
                # 设置数据ID
                id_col_index = merged_columns.index('数据ID')
                merged_data[data_id][id_col_index] = data_id
            
            # 将当前行的数据填充到合并后的记录中
            for i, col_name in enumerate(columns):
                if col_name in merged_columns:
                    col_index = merged_columns.index(col_name)
                    # 只有当目标位置为空或者当前值不为空时才更新
                    if merged_data[data_id][col_index] == '' or row[i] != '':
                        merged_data[data_id][col_index] = row[i]
    
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
    
    return merged_rows


def sync_to_mysql(db_config, table_name, column_names, rows):
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
                    print(f"表 {table_name} 不存在，正在创建...")
                    create_table_if_not_exists(cursor, table_name, column_names)
                else:
                    # 如果表存在，检查是否有新增字段需要添加
                    print(f"表 {table_name} 已存在，检查是否需要新增字段...")
                    add_missing_columns(cursor, table_name, column_names, db_config.get("database", ""))
                
                if rows:
                    placeholders = ", ".join(["%s"] * len(column_names))

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
                        if col not in ("id", "数据ID"):
                            eng_col = mapped_column_names[i] if col in FIELD_MAPPING else col
                            update_fields.append(f"`{eng_col}` = VALUES(`{eng_col}`)")

                    insert_sql = " ".join(f"""
                                    INSERT INTO {table_name} ({columns_str})
                                    VALUES ({placeholders})
                                    ON DUPLICATE KEY UPDATE {", ".join(update_fields)}
                                    """.split())

                    print(f'insert_sql:\n{insert_sql}')
                    cursor.executemany(insert_sql, rows)

            # 提交事务
            mysql_conn.commit()
            print(f"成功同步 {len(rows)} 条记录到MySQL数据库")

        finally:
            mysql_conn.close()

    except ImportError:
        print("缺少 pymysql 库，请安装: pip install pymysql")
    except Exception as e:
        print(f"同步到 MySQL 数据库时出错: {str(e)}")


def create_table_if_not_exists(cursor, table_name, column_names):
    """
    如果表不存在则创建表
    """
    columns_definitions = []
    for col in column_names:
        if col in FIELD_MAPPING:
            eng_col = FIELD_MAPPING[col]
            if col == "数据ID":
                columns_definitions.append(f"`{eng_col}` VARCHAR(191) PRIMARY KEY COMMENT '{col}'")
            elif col == "采集日期":
                columns_definitions.append(f"`{eng_col}` DATE COMMENT '{col}'")
            elif col == "采集时间":
                columns_definitions.append(f"`{eng_col}` DATETIME COMMENT '{col}'")
            else:
                columns_definitions.append(f"`{eng_col}` TEXT COMMENT '{col}'")
        else:
            columns_definitions.append(f"`{col}` TEXT")
    
    create_table_sql = " ".join(f"""
    CREATE TABLE {table_name} (
        {", ".join(columns_definitions)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """.split())
    
    print(f"Create table SQL: {create_table_sql}")
    cursor.execute(create_table_sql)


def add_missing_columns(cursor, table_name, column_names, database_name):
    """
    为已存在的表添加缺失的字段
    """
    # 获取表中已存在的字段
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
    """, (database_name, table_name))
    
    existing_columns = [row['column_name'] for row in cursor.fetchall()]
    
    # 检查是否有新增字段
    for col in column_names:
        # 映射中文字段名为英文名
        eng_col = FIELD_MAPPING.get(col, col)
        
        # 检查英文字段名是否已存在
        if eng_col not in existing_columns:
            # 添加缺失的字段
            if col in FIELD_MAPPING:
                if col == "数据ID":
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` VARCHAR(191) PRIMARY KEY COMMENT '{col}'"
                elif col == "采集日期":
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` DATE COMMENT '{col}'"
                elif col == "采集时间":
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` DATETIME COMMENT '{col}'"
                else:
                    alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` TEXT COMMENT '{col}'"
            else:
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT"
            
            print(f"添加缺失字段: {alter_sql}")
            try:
                cursor.execute(alter_sql)
            except Exception as e:
                print(f"添加字段 {eng_col} 时出错: {str(e)}")


if __name__ == "__main__":
    # 示例用法 - 关联融合，只同步最近3天的数据
    sync_explore_data_merge_to_remote(
        table_name_list=['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr'],
        merged_table_name="s_xhs_merged_data_ocr",
        merge_type="related",
        business_time_filter={"column": "采集时间", "days": 3}
    )
    
    # 示例用法 - 非关联融合，同步所有数据
    # sync_explore_data_merge_to_remote(
    #     table_name_list=['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr'],
    #     merged_table_name="s_xhs_merged_data_ocr",
    #     merge_type="unrelated"
    # )