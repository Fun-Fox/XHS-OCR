import re
import sqlite3
import os
from typing import Dict, List

# 获取当前文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 数据库文件路径
db_path = os.path.join(current_dir, 'ocr_data.db')


def save_ocr_data(tag, post_title: str, collect_time: str, ocr_data: List[str], index_mapping_data, date_dir,
                  ip_port_dir):
    """
    保存OCR识别数据到数据库
    :param tag: 标签名称
    :param post_title: 文件名（作品标题）
    :param collect_time: OCR采集时间
    :param ocr_data: OCR识别的数据列表
    :param index_mapping_data: 字段名列表
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 对字段名进行转义，避免特殊字符导致SQL语法错误
    escaped_fields = [f'"{field}"' for field in index_mapping_data]

    # print(escaped_fields)

    # 创建表，使用作品标题和OCR采集时间作为联合主键
    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS s_xhs_{tag}_ocr (
            "作品ID" INTEGER PRIMARY KEY AUTOINCREMENT,
            "设备IP" TEXT,
            "作品标题" TEXT,
            "截图采集日期" TEXT,
            "OCR采集时间" TEXT,
            {(' TEXT, '.join(escaped_fields)) + ' TEXT' if escaped_fields else ''},
            UNIQUE("作品标题", "OCR采集时间")
        )
    '''

    cursor.execute(create_table_sql)
    conn.commit()

    # 使用 INSERT OR IGNORE 语句，当作品标题和OCR采集时间都相同时不插入
    table_len = len(index_mapping_data) + 4 # 4 是指设备IP、作品标题、截图采集日期、OCR采集时间（这个4个字段）
    cursor.execute(f'''
        INSERT OR IGNORE INTO s_xhs_{tag}_ocr (
            "设备IP","作品标题", "截图采集日期","OCR采集时间", {','.join(escaped_fields)}
        ) VALUES ({','.join(['?' for _ in range(table_len)])})
    ''', (
        ip_port_dir, post_title, date_dir, collect_time,
        *[ocr_data[i] if len(ocr_data) > i else '' for i in range(len(ocr_data))]
    ))

    conn.commit()
    conn.close()
