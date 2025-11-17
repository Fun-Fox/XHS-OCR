from db.data_sync import sync_explore_data_to_remote
from db.pipeline import run_data_processing_pipeline

if __name__ == "__main__":
    # 默认运行完整流水线
    run_data_processing_pipeline(days=1)

    sync_explore_data_to_remote(['s_xhs_user_info_ocr', 's_xhs_data_overview_traffic_analysis']
                                , {"column": "采集日期", "days": 1},
                                unique_constraints={
                                    's_xhs_user_info_ocr': [ "account_id", "collection_time"],
                                    's_xhs_data_overview_traffic_analysis': [ "account_id", "collection_time","url"]})
