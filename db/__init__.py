import sqlite3
import os
from typing import Dict, List

# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 数据库文件路径
db_path = os.path.join(current_dir, 'ocr_data.db')


def save_ocr_data(tag, post_title: str, collect_time: str, ocr_data: List[str], index_mapping_data):
    """
    保存OCR识别数据到数据库
    :param filename: 文件名（作品标题）
    :param collect_time: 采集时间
    :param ocr_data: OCR识别的数据列表
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    table_len = len(index_mapping_data) + 2
    # 插入数据

    # 对字段名进行转义，避免特殊字符导致SQL语法错误
    escaped_fields = [f'"{field}"' for field in index_mapping_data]

    cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {tag}_ocr (
                作品ID INTEGER PRIMARY KEY AUTOINCREMENT,
                "作品标题" TEXT,
                "采集时间" TEXT,
                {(' TEXT, '.join(escaped_fields)) + ' TEXT' if escaped_fields else ''}
            )
        ''')
    conn.commit()

    cursor.execute(f'''
        INSERT INTO {tag}_ocr (
            "作品标题", "采集时间", {','.join(escaped_fields)}
        ) VALUES ({','.join(['?' for _ in range(table_len)])})
    ''', (
        post_title, collect_time,
        *[ocr_data[i] if len(ocr_data) > i else '' for i in range(len(ocr_data))]
    ))

    conn.commit()
    conn.close()