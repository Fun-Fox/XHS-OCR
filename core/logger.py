import os
from loguru import logger

# 确保logs目录存在
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(log_dir, exist_ok=True)

# 配置日志文件
logger.add(os.path.join(log_dir, "run_{time}.log"), rotation="100 MB", encoding="utf-8", retention="3 days")