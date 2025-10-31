# -*- coding: utf-8 -*-

"""
XHS-OCR 主入口文件
支持定时任务和手动执行两种模式
"""

import os
import sys
import time
import argparse
from datetime import datetime
from core.logger import logger
# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.mask_ocr import process_images
from db.data_sync import sync_explore_data_to_remote

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def run_ocr_task():
    """
    执行OCR识别任务
    """
    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行OCR识别任务...")
    try:
        process_images()
        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] OCR识别任务执行完成")
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] OCR识别任务执行出错: {e}")


def run_sync_task():
    """
    执行数据同步任务
    """
    logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始执行数据同步任务...")
    try:
        sync_explore_data_to_remote(['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr','s_xhs_profile_page_ocr'])
        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 数据同步任务执行完成")
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 数据同步任务执行出错: {e}")


def run_all_tasks(sync_enabled=True):
    """
    执行所有任务：OCR识别 + 数据同步
    :param sync_enabled: 是否启用数据同步功能
    """
    run_ocr_task()
    if sync_enabled:
        run_sync_task()
    else:
        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 数据同步功能已禁用")


def manual_run(sync_enabled=True):
    """
    手动执行模式
    :param sync_enabled: 是否启用数据同步功能
    """
    logger.info("XHS-OCR 手动执行模式")
    run_all_tasks(sync_enabled)


def schedule_run(interval, at_time, sync_enabled=True):
    """
    定时任务模式
    :param interval: 时间间隔（分钟）
    :param at_time: 指定时间（如 "10:00"）
    :param sync_enabled: 是否启用数据同步功能
    """
    logger.info("XHS-OCR 定时任务模式")

    try:
        import schedule

        if at_time:
            # 在指定时间执行
            schedule.every().day.at(at_time).do(run_all_tasks, sync_enabled=sync_enabled)
            logger.info(f"默认配置：每天 {at_time} 执行一次任务")
        elif interval:
            # 按时间间隔执行
            schedule.every(interval).minutes.do(run_all_tasks, sync_enabled=sync_enabled)
            logger.info(f"默认配置：每 {interval} 分钟执行一次任务")
        else:
            # 默认每小时执行
            schedule.every().hour.do(run_all_tasks, sync_enabled=sync_enabled)
            logger.info("默认配置：每小时执行一次任务")

        logger.info("定时任务已启动，按 Ctrl+C 退出")

        while True:
            schedule.run_pending()
            time.sleep(1)

    except ImportError:
        logger.error("错误：缺少 schedule 库")
        logger.error("请安装: pip install schedule")
        logger.error("或者使用手动执行模式: python main.py --mode manual")


def main():
    """
    主函数
    """
    parser = argparse.ArgumentParser(description='XHS-OCR 主程序')
    parser.add_argument(
        '--mode',
        choices=['manual', 'schedule'],
        default='manual',
        help='运行模式: manual(手动执行) 或 schedule(定时任务)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        help='定时任务时间间隔（分钟）'
    )
    parser.add_argument(
        '--at-time',
        help='定时任务执行时间（如 10:00）'
    )
    parser.add_argument(
        '--sync',
        action='store_true',
        help='是否启用数据同步功能（默认启用）'
    )
    parser.add_argument(
        '--no-sync',
        action='store_false',
        dest='sync',
        help='禁用数据同步功能'
    )
    parser.set_defaults(sync=True)

    args = parser.parse_args()

    if args.mode == 'manual':
        manual_run(args.sync)
    elif args.mode == 'schedule':
        schedule_run(args.interval, args.at_time, args.sync)


if __name__ == "__main__":
    main()
