
from loguru import logger
# 配置日志文件
logger.add("logs/run_{time}.log", rotation="100 MB", encoding="utf-8", retention="3 days")

