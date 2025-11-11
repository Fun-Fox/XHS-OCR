import asyncio
import datetime
import json
import os
import re
import cv2
import numpy as np
from PIL import Image
from core.logger import logger
from core.ocr import sort_text_lines_by_position, ocr

from core.user_profile import get_user_profile_data
# 引入数据库模块
from db import save_ocr_data, save_userinfo_data
import time
import configparser

from dotenv import load_dotenv

# 配置日志文件
load_dotenv()

# 遮罩图路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# OCR 图片目录
ocr_dir = os.getenv("OCR_IMAGES_PATH", os.path.join(root_dir, "images"))
ocr_engine = os.getenv("OCR_ENGINE", "surya")

if ocr_engine == "PaddleOCR":
    from core.ppocr_api import GetOcrApi

    # OCR 引擎路径
    ocr_engine_path = os.getenv("OCR_ENGINE_PATH")
    if not ocr_engine_path:
        logger.error("OCR_ENGINE_PATH 环境变量未设置")

    if not os.path.exists(ocr_engine_path):
        logger.error(f"OCR引擎路径不存在: {ocr_engine_path}")
    # 初始化 OCR 引擎
    ocr = GetOcrApi(ocr_engine_path)

# 读取配置文件
config = configparser.ConfigParser()
with open(os.path.join(root_dir, 'config.ini'), encoding='utf-8') as f:
    config.read_file(f)


def upscale_image(image, scale_factor=2):
    """
    放大图像

    Args:
        image: 输入图像
        scale_factor: 放大倍数，默认为2

    Returns:
        放大后的图像
    """
    return cv2.resize(image, None, fx=scale_factor, fy=scale_factor, interpolation=cv2.INTER_CUBIC)


def preprocess_image(image):
    """
    对图像进行预处理：去噪和二值化

    Args:
        image: 输入图像

    Returns:
        预处理后的图像
    """
    # 转换为灰度图
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # 应用高斯模糊去噪
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)

    # 使用自适应阈值进行二值化
    binary = cv2.adaptiveThreshold(
        blurred,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV,
        11,
        2
    )

    return binary


def enhance_image(image, alpha=1.5, beta=50):
    """
    增加图像的对比度和亮度

    Args:
        image: 输入图像
        alpha: 对比度控制参数，默认为1.5
        beta: 亮度控制参数，默认为50

    Returns:
        增强后的图像
    """
    # 使用公式：output = alpha * input + beta
    enhanced_img = cv2.convertScaleAbs(image, alpha=alpha, beta=beta)
    return enhanced_img


def process_images():
    try:
        import subprocess
        import sys
        # 安装 Chromium 浏览器（如果尚未安装）
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                                capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"Playwright 安装失败: {result.stderr}")
        logger.info(f"Playwright chromium 安装成功")
    except ImportError:
        logger.warning("playwright 模块未安装，某些功能可能不可用")
    """
    处理OCR目录下的所有图片
    """
    if ocr_engine == "PaddleOCR":
        if ocr.getRunningMode() == "local":
            logger.info(f"初始化OCR成功，进程号为{ocr.ret.pid}")
        elif ocr.getRunningMode() == "remote":
            logger.info(f"连接远程OCR引擎成功，ip：{ocr.ip}，port：{ocr.port}")

    # 遍历 OCR 目录下的所有图片

    # 获取最近3天的日期列表
    recent_dates = []
    for i in range(3):
        date_str = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y%m%d')
        recent_dates.append(date_str)
    logger.info(f"最近3天日期: {recent_dates}")
    # logger.info(f"开始扫描ocr目录：{ocr_dir}")

    # 检查ocr目录是否存在
    if not os.path.exists(ocr_dir):
        logger.error(f"OCR目录不存在: {ocr_dir}")
        return

    for root, dirs, files in os.walk(ocr_dir):
        # 只扫描ocr_dir下最近3天的目录文件夹(例如目录是20250902的)
        dir_contains_recent_date = any(date in root for date in recent_dates)

        logger.info(f"扫描目录: {root}, 包含最近日期: {dir_contains_recent_date}")

        # 如果是根目录，继续遍历子目录
        if root == ocr_dir:
            continue

        # 如果当前路径不包含最近3天的日期，则跳过
        if not dir_contains_recent_date:
            logger.info(f"跳过目录(非最近3天): {root}")
            continue

        logger.info(f"处理最近3天的目录: {root}")
        for filename in files:
            # 构建图片路径
            file_path = os.path.join(root, filename)

            if filename == "profile_url.txt":
                # 如果文件名是profile_url.txt 则读取文件，并且使用当前时间戳
                with open(file_path, 'r', encoding='utf-8') as f:
                    author_profile_url = f.read().strip()
                    # 获取文件最后修改时间
                    modified_time = os.path.getmtime(file_path)
                    collect_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modified_time))
                parent_dir = os.path.dirname(file_path)
                user_info = asyncio.run(get_user_profile_data(author_profile_url))

                ip_port_dir, account_id = os.path.basename(parent_dir).split('#')
                save_userinfo_data(user_info, ip_port_dir, account_id, collect_time,author_profile_url)
            elif ".png" in filename:
                tag, post_title = os.path.basename(filename).replace(".png", "").split('#')
                json_filename = f"{post_title}.json"
                json_file_path = os.path.join(root, json_filename)
                logger.info(f"处理文件: {json_file_path}")
                note_link = ""
                if os.path.exists(json_file_path):
                    try:
                        with open(json_file_path, 'r', encoding='utf-8') as f:
                            json_data = json.load(f)
                            note_link = json_data.get("note_link", "")
                            post_content = json_data.get("post_content", "")
                            clean_title = json_data.get("clean_title", "")
                    except Exception as e:
                        logger.error(f"读取JSON文件失败: {json_file_path}, 错误: {e}")
                else:
                    logger.warning(f"JSON文件不存在: {json_file_path}")
                # 获取post_title查找当前目录下的同名json文件，读取数据 note_link,post_content,clean_title
                # {
                #     "note_link": "http://xhslink.com/o/1wQYKbI86o4",
                #     "post_content": "",
                #     "title": "笔记,小香风女孩报道,来自吃土潮玩收藏家,4赞，141阅读",
                #     "clean_title": "小香风女孩报道",
                #     "timestamp": "2025-11-07 04:08:28"
                # }

                modified_time = os.path.getmtime(file_path)
                collect_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modified_time))

                # 解析路径获取日期和设备IP
                parent_dir = os.path.dirname(file_path)  # 获取图片所在目录
                if '#' in os.path.basename(parent_dir):
                    ip_and_account = os.path.basename(parent_dir).split('#')
                    ip_port_dir, account_id = ip_and_account[0], ip_and_account[1]  # 获取 IP:端口 名称
                else:
                    ip_port_dir, account_id = os.path.basename(parent_dir), '无'

                date_dir = os.path.basename(os.path.dirname(parent_dir))  # 获取日期文件夹名
                logger.info(f"处理图片: {filename}, 日期: {date_dir}, 设备: {ip_port_dir}")
                # 读取原图和遮罩图
                original_img = imread_with_pil(file_path)
                mask_path = os.path.join(root_dir, "mask", f"{tag}.png")
                mask_img = imread_with_pil(mask_path)  # 读取带Alpha通道的遮罩图

                # 检查原图是否有效
                if original_img is None:
                    logger.error(f"原图加载失败: {file_path}")
                    continue

                # 检查遮罩图是否有效
                if mask_img is None:
                    logger.error(f"遮罩图加载失败: {mask_path}")
                    continue

                # 确保遮罩图与原图尺寸一致
                if original_img.shape[:2] != mask_img.shape[:2]:
                    logger.warning(f"遮罩图尺寸不匹配: {mask_img.shape[:2]} vs {original_img.shape[:2]}")
                    continue

                # 使用遮罩图合成新图片（保留遮罩区域，其他区域变黑）
                alpha = mask_img[:, :, 3] / 255.0  # 提取Alpha通道并归一化
                result_img = original_img * alpha[:, :, np.newaxis]  # 应用Alpha混合
                result_img = result_img.astype(np.uint8)

                # 将结果保存为临时文件
                temp_output_path = os.path.join(root_dir, "tmp", "temp_ocr_input.png")
                # temp_output_path = os.path.join(root_dir, r"tmp", f"{time.time()}.png")
                # 放大
                # result_img = upscale_image(result_img, scale_factor=2)
                # result_img = enhance_image(result_img, alpha=1, beta=20)  # 增加对比度和亮度
                cv2.imwrite(temp_output_path, result_img)

                # 从配置文件中获取index_mapping_data
                index_mapping_data = []
                if config.has_section('tags') and config.has_option('tags', tag):
                    index_mapping_data_str = config.get('tags', tag)
                    index_mapping_data = [item.strip() for item in index_mapping_data_str.split(',')]
                # 执行 OCR 识别
                # 使用快速的蒙版识别方式，使用VLM OCR方式
                logger.info(f"正在处理: {filename}")

                if ocr_engine == "PaddleOCR":
                    getObj = ocr.run(temp_output_path)
                    if not getObj["code"] == 100:
                        logger.info(f"识别结果: {getObj}")
                        logger.error(f"识别失败: {filename}")
                        continue
                    sorted_lines = getObj["data"]
                else:
                    # 执行OCR
                    img = Image.open(temp_output_path)
                    img_pred = ocr(img, with_bboxes=True)
                    sorted_lines = sort_text_lines_by_position(img_pred.text_lines)
                #
                # # 提取OCR文本数据
                # ocr_texts = []
                # for index, line in enumerate(getObj["data"]):
                #     text = str(line['text'])
                #     if '秒' in text:
                #         text = text.replace('秒', '')
                #     elif 'o' in text:
                #         text = text.replace('o', '0')
                #
                #     # logger.info(f"{index}:{text}")
                #     # 判断text为数字或%的时候，才打印
                #     # 判断text为数字或%的时候，才打印
                #     if re.match(r'^\d+(\.\d+)?%?$', text.strip()):
                #         ocr_texts.append(text)
                #         logger.info(f"{index_mapping_data[index]}:{text}")
                #     # logger.info(f"{index_mapping_data[index]}:{text}")

                # if len(getObj["data"]) != len(index_mapping_data):
                #     logger.warning("识别到的数据个数不匹配，可能是截图位置发生变化或者截图不完整，可能需要重新制作蒙版")
                #     continue

                ocr_texts = []
                for line in sorted_lines:
                    if ocr_engine == "PaddleOCR":
                        text = str(line['text'])
                    else:
                        text = line.text
                    text = text.replace('秒', '').replace(' ', '').replace('o', '0').replace('<b>', '').replace('</b>',
                                                                                                                '')
                    ocr_texts.append(text)
                print(ocr_texts)

                if len(ocr_texts) != len(index_mapping_data):
                    logger.warning("识别到的数据个数不匹配，可能是截图位置发生变化或者截图不完整，可能需要重新制作蒙版")
                    continue

                # 保存数据到数据库
                tag = re.sub(r'\d+', '', tag)
                # if note_link:
                save_ocr_data(tag, post_title, note_link, collect_time, ocr_texts, index_mapping_data, date_dir,
                              ip_port_dir,
                              account_id)

        # textBlocks = getObj["data"]

        # 可视化结果
        # vis = visualize(textBlocks, temp_output_path)
        # # vis.show()
        #
        # # 保存可视化结果
        # output_result_path = os.path.join(root_dir, 'ocr_result', f"{filename}")
        # vis.save(output_result_path, isText=True)
        #
        # logger.info(f"OCR 结果已保存到: {output_result_path}")


# 结束 OCR 引擎
# ocr.exit()
logger.info("程序结束。")


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
        logger.error(f"使用PIL读取图片失败: {path}, 错误: {e}")
        return None


if __name__ == "__main__":
    process_images()
    # 开始同步识别后的数据
    # sync_explore_data_to_remote(['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr'])
