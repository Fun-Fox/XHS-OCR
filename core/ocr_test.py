import os
import re

import cv2
import numpy as np
from PIL import Image
from dotenv import load_dotenv

from core.ocr import sort_text_lines_by_paddle_position

# 配置日志文件
load_dotenv()

from core.ppocr_api import GetOcrApi

# OCR 引擎路径
ocr_engine_path = r'D:\PaddleOCR-json_v1.4.1\PaddleOCR-json.exe'

# 初始化 OCR 引擎
ocr = GetOcrApi(ocr_engine_path)


def imread_with_pil(path):
    try:
        pil_image = Image.open(path)
        # 转换为OpenCV格式
        if pil_image.mode == "RGB":
            # RGB to BGR
            open_cv_image = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
        elif pil_image.mode == "RGBA":
            # RGBA to BGRA
            open_cv_image = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGBA2BGRA)
        else:
            # 灰度图或其他格式
            open_cv_image = np.array(pil_image)
        return open_cv_image
    except Exception as e:
        return None
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    file_path = r'D:\PycharmProjects\XHS-OCR\images\xhs\aibox\20251217\note_traffic_analysis#这种球形的dimoo造景大家会喜欢吗.png'
    original_img = imread_with_pil(file_path)
    mask_path = r'D:\PycharmProjects\XHS-OCR\images\xhs\aibox\20251217\note_traffic_analysis#1.png'
    mask_img = imread_with_pil(mask_path)  # 读取带Alpha通道的蒙版图

    # 确保蒙版图与原图尺寸一致

    # 使用蒙版图合成新图片（保留蒙版区域，其他区域变黑）
    alpha = mask_img[:, :, 3] / 255.0  # 提取Alpha通道并归一化
    result_img = original_img * alpha[:, :, np.newaxis]  # 应用Alpha混合
    result_img = result_img.astype(np.uint8)
    temp_output_path = os.path.join(root_dir, "tmp", "temp_ocr_input.png")
    cv2.imwrite(temp_output_path, result_img, [cv2.IMWRITE_PNG_COMPRESSION, 1])

    # 将结果保存为临时文件

    getObj = ocr.run(temp_output_path)
    print(getObj["data"])
    # print(getObj)

    # sorted_lines = getObj["data"]
    # 这里也增加从左到右 从上到下的排序功能
    # print("排序前:", getObj["data"])
    sorted_lines = sort_text_lines_by_paddle_position(getObj["data"])
    print("排序后:", sorted_lines)
    ocr_texts = []
    for line in sorted_lines:
        text = str(line['text'])

        # text = re.sub(r'[\u4e00-\u9fff]+', '', text)
        # text = (text.replace('秒', '')
        #         .replace(' ', '')
        #         .replace('o', '0')
        #         .replace('<b>', '')
        #         .replace('</b>', ''))
        if text:
            ocr_texts.append(text)

    print(ocr_texts)

