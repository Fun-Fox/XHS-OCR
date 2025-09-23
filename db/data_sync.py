import configparser
import os
from dotenv import load_dotenv

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
def sync_explore_data_to_remote(table_name_list=['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr']):
    """
    将Download文件夹下的ExploreData.db sqlite数据全量同步到远程MySQL数据库中
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
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for table_name in table_name_list:
            # 读取所有数据
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()

            # 获取列名
            column_names = [description[0] for description in cursor.description]

            # 同步到MySQL数据库
            sync_to_mysql(db_config, table_name, column_names, rows)
            print("数据已同步到远程MySQL数据库")
        # 关闭本地数据库连接
        conn.close()

    except Exception as e:
        print(f"同步数据到远程数据库时出错: {str(e)}")


def sync_to_mysql(db_config, table_name, column_names, rows):
    """
    同步数据到MySQL数据库
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
                # 创建表（如果不存在）
                # 修改表结构，将TEXT类型的主键改为VARCHAR(255)
                columns_definitions = []
                for col in column_names:
                    if col in FIELD_MAPPING:
                        eng_col = FIELD_MAPPING[col]
                        if col == "作品ID":
                            columns_definitions.append(f"`{eng_col}` VARCHAR(191) PRIMARY KEY COMMENT '{col}'")
                        else:
                            columns_definitions.append(f"`{eng_col}` TEXT COMMENT '{col}'")
                    else:
                        columns_definitions.append(f"`{col}` TEXT")

                # 使用单行字符串格式，避免潜在的多行字符串问题
                create_table_sql = " ".join(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {", ".join(columns_definitions)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """.split())

                # 添加调试输出
                print(f"Create table SQL: {create_table_sql}")
                cursor.execute(create_table_sql)

                if rows:
                    # 修复：使用英文字段名而不是中文字段名
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
                        if col not in ("id", "作品ID"):
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
            print(f"成功同步 {len(rows)} 条记录到MySQL数据库，ID冲突的不同步")

        finally:
            mysql_conn.close()

    except ImportError:
        print("缺少 pymysql 库，请安装: pip install pymysql")
    except Exception as e:
        print(f"同步到 MySQL 数据库时出错: {str(e)}")
