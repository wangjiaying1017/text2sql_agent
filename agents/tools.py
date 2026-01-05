"""
LangChain tools for Text2SQL Agent.
"""
from typing import Any
from langchain_core.tools import tool

from database import MySQLConnector, InfluxDBConnector
from intent import IntentRecognizer


@tool
def recognize_intent(question: str) -> dict[str, Any]:
    """
    识别用户问题的查询意图，生成查询计划。
    
    输入用户的自然语言问题，分析后返回查询策略和执行步骤。
    
    Args:
        question: 用户的自然语言问题
        
    Returns:
        包含analysis、strategy、steps和confidence的查询计划
    """
    recognizer = IntentRecognizer()
    plan = recognizer.recognize(question)
    return plan.model_dump()


@tool
def query_mysql(sql: str) -> list[dict[str, Any]]:
    """
    执行MySQL查询。
    
    输入SQL语句，返回查询结果。
    
    Args:
        sql: SQL查询语句
        
    Returns:
        查询结果列表
    """
    with MySQLConnector() as conn:
        return conn.execute(sql)


@tool
def query_influxdb(flux_query: str) -> list[dict[str, Any]]:
    """
    执行InfluxDB Flux查询。
    
    输入Flux查询语句，返回时序数据结果。
    
    Args:
        flux_query: Flux查询语句
        
    Returns:
        查询结果列表
    """
    with InfluxDBConnector() as conn:
        return conn.execute(flux_query)


@tool
def get_mysql_schema() -> list[dict[str, Any]]:
    """
    获取MySQL数据库的表结构信息。
    
    返回数据库中所有表的列信息，包括列名、数据类型等。
    
    Returns:
        数据库schema信息
    """
    with MySQLConnector() as conn:
        return conn.get_schema()


@tool
def get_influxdb_measurements() -> list[str]:
    """
    获取InfluxDB中的所有measurement名称。
    
    Returns:
        measurement名称列表
    """
    with InfluxDBConnector() as conn:
        return conn.get_measurements()


def get_tools() -> list:
    """
    获取所有可用的工具列表。
    
    Returns:
        LangChain工具列表
    """
    return [
        recognize_intent,
        query_mysql,
        query_influxdb,
        get_mysql_schema,
        get_influxdb_measurements,
    ]
