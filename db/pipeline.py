"""
数据处理流水线模块
提供完整的数据处理流水线，按顺序执行各种数据融合操作
"""
from db.data_sync import sync_explore_data_to_remote
from db.data_dms import sync_explore_data_merge_to_remote


def run_data_processing_pipeline(days=3):
    """
    运行完整的数据处理流水线
    
    参数:
    days: 业务时间筛选天数，默认为3天
    """
    print(f"开始执行数据处理流水线，时间范围：最近{days}天")

    # 步骤1: 视频总览数据处理
    # 将视频的顶部与底部数据进行关联合并，生成视频总览数据
    print("步骤1: 处理视频总览数据...")
    sync_explore_data_merge_to_remote(
        table_name_list=['s_xhs_video_data_overview_top_ocr', 's_xhs_video_data_overview_bottom_ocr'],
        merged_table_name="s_xhs_video_data_overview_ocr",
        merge_type="related",
        business_time_filter={"column": "采集日期", "days": days},
        target_db="local",
        related_key=["账号ID", "设备IP", "采集日期", "链接"]
    )

    # 步骤2: 总览数据处理
    # 将视频数据与图文数据进行非关联合并，生成总览数据
    print("步骤2: 处理总览数据...")
    sync_explore_data_merge_to_remote(
        table_name_list=['s_xhs_note_data_overview_ocr', 's_xhs_video_data_overview_ocr'],
        merged_table_name="s_xhs_data_overview_ocr",
        merge_type="unrelated",
        business_time_filter={"column": "采集日期", "days": days},
        target_db="local",
        related_key=["账号ID", "设备IP", "采集日期", "链接"]
    )

    # 步骤3: 趋势分析数据处理
    # 将视频数据与图文数据进行非关联合并，生成趋势分析数据
    print("步骤3: 处理趋势分析数据...")
    sync_explore_data_merge_to_remote(
        table_name_list=['s_xhs_note_traffic_analysis_ocr', 's_xhs_video_traffic_analysis_ocr'],
        merged_table_name="s_xhs_traffic_analysis_ocr",
        merge_type="unrelated",
        business_time_filter={"column": "采集日期", "days": days},
        target_db="local",
        related_key=["账号ID", "设备IP", "采集日期", "链接"]
    )

    # 步骤4: 远程数据库同步
    # 将数据分析与趋势分析进行关联合并，并同步到远程数据库
    print("步骤4: 同步数据到远程数据库...")
    sync_explore_data_merge_to_remote(
        table_name_list=['s_xhs_data_overview_ocr', 's_xhs_traffic_analysis_ocr'],
        merged_table_name="s_xhs_data_overview_traffic_analysis",
        merge_type="related",
        business_time_filter={"column": "采集日期", "days": days},
        target_db="local",
        related_key=["账号ID", "设备IP", "采集日期", "链接"]
    )

    print("数据处理流水线执行完成！")


if __name__ == "__main__":
    # 默认运行完整流水线
    run_data_processing_pipeline(days=1)

    sync_explore_data_to_remote(['s_xhs_user_info_ocr','s_xhs_data_overview_traffic_analysis']
                                ,{"column": "采集日期", "days": 1})
