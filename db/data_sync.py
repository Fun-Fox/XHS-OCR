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
def sync_explore_data_to_remote(table_name_list=None, time_filter=None):
    """
    将Download文件夹下的ExploreData.db sqlite数据同步到远程MySQL数据库中
    
    参数:
    table_name_list: 要同步的表名列表
    time_filter: 时间筛选条件，格式为字典{"column": "采集时间", "days": 3}表示最近3天数据
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
            sync_to_mysql(db_config, table_name, column_names, rows)
            logger.info(f"表 {table_name} 数据已同步到远程MySQL数据库")

        # 关闭本地数据库连接
        conn.close()

    except Exception as e:
        logger.error(f"同步数据到远程数据库时出错: {str(e)}")


def sync_to_mysql(db_config, table_name, column_names, rows):
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
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                """, (db_config.get("database", ""), table_name))

                table_exists = cursor.fetchone()['COUNT(*)'] > 0

                # 如果表不存在，则创建表
                if not table_exists:
                    logger.info(f"表 {table_name} 不存在，正在创建...")
                    create_table_if_not_exists(cursor, table_name, column_names)
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


def create_table_if_not_exists(cursor, table_name, column_names):
    """
    如果表不存在则创建表
    """
    columns_definitions = []
    columns_definitions.append('`id` BIGINT AUTO_INCREMENT PRIMARY KEY')
    for col in column_names:
        if col in FIELD_MAPPING:
            eng_col = FIELD_MAPPING[col]

            if col == "采集日期":
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

    logger.debug(f"Create table SQL: {create_table_sql}")
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
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{eng_col}` TEXT COMMENT '{col}'"
            else:
                alter_sql = f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT"

            logger.info(f"添加缺失字段: {alter_sql}")
            try:
                cursor.execute(alter_sql)
            except Exception as e:
                logger.error(f"添加字段 {eng_col} 时出错: {str(e)}")
