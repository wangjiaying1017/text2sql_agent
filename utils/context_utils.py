"""
上下文处理工具函数

统一管理上下文的提取、格式化和压缩逻辑。
"""
import json
from typing import Any, Optional
from langchain_core.messages import BaseMessage


def extract_context_from_messages(
    messages: list[BaseMessage],
    max_history: int = 3
) -> Optional[dict]:
    """
    从历史消息中提取有效上下文信息。
    
    优先提取：
    1. 设备序列号 (serial)
    2. 客户 ID (client_id)
    3. 查询结果样本
    
    Args:
        messages: 历史消息列表
        max_history: 最多回溯的消息数量
        
    Returns:
        提取的上下文字典，包含 serials, client_ids, result_sample
    """
    context = {
        "serials": [],
        "client_ids": [],
        "result_sample": None,
    }
    
    # 从最近的 AI 消息中提取
    for msg in reversed(messages[-max_history * 2:]):
        if msg.type != "ai":
            continue
        try:
            data = json.loads(msg.content)
            sample = data.get("result_sample", [])
            for row in sample:
                if row.get("serial"):
                    context["serials"].append(row["serial"])
                if row.get("client_id"):
                    context["client_ids"].append(row["client_id"])
            if sample and not context["result_sample"]:
                context["result_sample"] = sample
            break  # 只取最近一条
        except (json.JSONDecodeError, TypeError):
            continue
    
    # 去重
    context["serials"] = list(set(context["serials"]))
    context["client_ids"] = list(set(context["client_ids"]))
    
    return context if any(context.values()) else None


def format_extracted_context(context: dict) -> str:
    """
    将提取的上下文格式化为 prompt 友好的字符串。
    
    Args:
        context: 从 extract_context_from_messages 返回的上下文字典
        
    Returns:
        格式化的字符串
    """
    parts = []
    
    if context.get("serials"):
        parts.append(f"设备序列号: {', '.join(context['serials'])}")
    if context.get("client_ids"):
        parts.append(f"客户ID: {', '.join(str(x) for x in context['client_ids'])}")
    if context.get("result_sample"):
        parts.append(f"上一轮查询结果样本: {json.dumps(context['result_sample'], ensure_ascii=False)}")
    
    return "\n".join(parts) if parts else ""


def extract_key_fields(results: list[dict], max_rows: int = 5) -> list[dict]:
    """
    从结果中提取关键字段，用于存储到消息历史。
    
    只保留可能在后续查询中用作条件的字段。
    
    Args:
        results: 查询结果列表
        max_rows: 最多保留的行数
        
    Returns:
        只包含关键字段的结果列表
    """
    key_fields = {"id", "serial", "client_id", "name", "device_id", "node_id"}
    extracted = []
    
    for row in results[:max_rows]:
        key_values = {k: v for k, v in row.items() if k in key_fields}
        if key_values:
            extracted.append(key_values)
    
    return extracted
