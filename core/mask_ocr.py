import datetime
import os
import cv2
import numpy as np
from PIL import Image
from core.logger import logger

from core.ppocr_api import GetOcrApi
from core.ppocr_visualize import visualize
# 引入数据库模块
from db import save_ocr_data
import time
import configparser

from dotenv import load_dotenv

# 配置日志文件
load_dotenv()

# OCR 引擎路径
ocr_engine_path = os.getenv("OCR_ENGINE_PATH")

# 遮罩图路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# OCR 图片目录
ocr_dir = os.getenv("OCR_IMAGES_PATH", os.path.join(root_dir, "images"))

# 初始化 OCR 引擎
ocr = GetOcrApi(ocr_engine_path)

# 读取配置文件
config = configparser.ConfigParser()
with open(os.path.join(root_dir, 'config.ini'), encoding='utf-8') as f:
    config.read_file(f)


def process_images():
    """
    处理OCR目录下的所有图片
    """
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
    logger.info(f"开始扫描ocr目录：{ocr_dir}")

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
            img_path = os.path.join(root, filename)
            post_info = os.path.basename(filename).split('#')

            tag = post_info[0]
            post_title = post_info[1]
            # 将时间戳转换为可读时间格式
            timestamp_str = post_info[2]
            if '.' in timestamp_str:
                timestamp = int(timestamp_str.split('.')[0])
            else:
                timestamp = int(timestamp_str)
            collect_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

            # 解析路径获取日期和设备IP
            parent_dir = os.path.dirname(img_path)  # 获取图片所在目录
            ip_port_dir = os.path.basename(parent_dir)  # 获取 IP:端口 名称
            date_dir = os.path.basename(os.path.dirname(parent_dir))  # 获取日期文件夹名
            logger.info(f"处理图片: {filename}, 日期: {date_dir}, 设备: {ip_port_dir}")
            # 读取原图和遮罩图
            original_img = imread_with_pil(img_path)
            mask_path = os.path.join(root_dir, "mask", f"{tag}.png")
            mask_img = imread_with_pil(mask_path)  # 读取带Alpha通道的遮罩图

            # 检查原图是否有效
            if original_img is None:
                logger.error(f"原图加载失败: {img_path}")
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
            cv2.imwrite(temp_output_path, result_img)

            # 执行 OCR 识别
            logger.info(f"正在处理: {filename}")
            getObj = ocr.run(temp_output_path)

            if not getObj["code"] == 100:
                logger.error(f"识别失败: {filename}，可能数据是空的")
                continue

            # 从配置文件中获取index_mapping_data
            index_mapping_data = []
            if config.has_section('tags') and config.has_option('tags', tag):
                index_mapping_data_str = config.get('tags', tag)
                index_mapping_data = [item.strip() for item in index_mapping_data_str.split(',')]

            if len(getObj["data"]) != len(index_mapping_data):
                logger.warning("识别到的数据个数不匹配，可能是截图位置发生变化或者截图不完整，可能需要重新制作蒙版")
                continue

            # 提取OCR文本数据
            ocr_texts = []
            for index, line in enumerate(getObj["data"]):
                text = str(line['text'])
                if '秒' in text:
                    text = text.replace('秒', '')
                ocr_texts.append(text)

                logger.info(f"{index_mapping_data[index]}:{text}")

            # 保存数据到数据库
            save_ocr_data(tag, post_title, collect_time, ocr_texts, index_mapping_data, date_dir, ip_port_dir)

            textBlocks = getObj["data"]

            # 可视化结果
            vis = visualize(textBlocks, temp_output_path)
            # vis.show()

            # 保存可视化结果
            output_result_path = os.path.join(root_dir, 'ocr_result', f"{filename}")
            vis.save(output_result_path, isText=True)

            logger.info(f"OCR 结果已保存到: {output_result_path}")

    # 结束 OCR 引擎
    ocr.exit()
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
