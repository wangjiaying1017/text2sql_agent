"""
日志配置模块

提供统一的日志配置，适用于生产环境。
"""
import logging
import sys
from typing import Optional


# 日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: str = "text2sql",
    level: int = logging.INFO,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    配置并返回一个日志记录器。
    
    Args:
        name: 日志记录器名称
        level: 日志级别（默认 INFO）
        log_file: 日志文件路径（可选）
    
    Returns:
        配置好的 Logger 实例
    """
    logger = logging.getLogger(name)
    
    # 避免重复配置
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logger.addHandler(console_handler)
    
    # 文件处理器（可选）
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        logger.addHandler(file_handler)
    
    return logger


# 默认日志记录器
logger = setup_logger()


def get_logger(name: str = None) -> logging.Logger:
    """
    获取日志记录器。
    
    Args:
        name: 模块名称，将作为子记录器
    
    Returns:
        Logger 实例
    """
    if name:
        return logging.getLogger(f"text2sql.{name}")
    return logger
