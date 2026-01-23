"""
LangGraph 状态定义

定义 Text2SQL 工作流的状态类型。
"""
from typing import TypedDict, Annotated, Optional, Any, Sequence, Literal
from operator import add
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage


# 执行状态类型
StatusType = Literal["running", "success", "no_result", "error"]


def merge_dicts(a: dict, b: dict) -> dict:
    """合并两个字典，用于 timing 字段的累积更新。"""
    return {**a, **b}


def add_messages(left: Sequence[BaseMessage], right: Sequence[BaseMessage]) -> list[BaseMessage]:
    """
    合并消息列表，实现长短期记忆分离。
    
    规则：
    1. 正常累积直到 20 条
    2. 超过 20 条时触发修剪，存档超出部分到向量库，保留最新 10 条
    3. 确保第一条是 HumanMessage
    """
    result = list(left) + list(right)
    
    # 触发修剪阈值
    TRIM_THRESHOLD = 20
    # 修剪后保留的消息数
    KEEP_COUNT = 10
    
    if len(result) > TRIM_THRESHOLD:
        # 需要存档的消息
        to_archive = result[:-KEEP_COUNT]
        
        # 存档到向量库（异步处理，避免阻塞）
        _archive_messages_async(to_archive)
        
        # 保留最新的消息
        result = result[-KEEP_COUNT:]
        
        # 确保第一条是 HumanMessage
        while result and result[0].type != "human":
            result = result[1:]
    
    return result


def _archive_messages_async(messages: list[BaseMessage], thread_id: str = "default") -> None:
    """
    将消息存档到向量库（长期记忆）。
    
    Args:
        messages: 要存档的消息列表
        thread_id: 会话 ID
    """
    import logging
    import threading
    
    logger = logging.getLogger("text2sql.memory")
    
    def _do_archive():
        try:
            from memory import LongTermMemory
            memory = LongTermMemory()
            count = memory.archive(messages, thread_id)
            logger.info(f"[Memory] 成功存档 {count} 条对话到长期记忆")
        except Exception as e:
            logger.error(f"[Memory] 存档失败: {e}")
    
    # 异步执行，避免阻塞主流程
    thread = threading.Thread(target=_do_archive, daemon=True)
    thread.start()


class StepResult(TypedDict):
    """单步执行结果。"""
    step_id: int
    database: str
    query: str
    results: list[dict[str, Any]]
    error: Optional[str]


class Text2SQLState(TypedDict):
    """
    Text2SQL 工作流状态。
    
    所有节点共享此状态，通过 Command.update 更新字段。
    """
    # === 输入（主 Agent 传入）===
    question: str  # 用户问题
    serial: Optional[str]  # 设备序列号（主 Agent 通过 API 解析，可为空）
    client_id: Optional[str]  # 客户 ID（主 Agent 通过 API 解析，可为空）
    verbose: bool  # 是否显示详细日志信息
    
    # 会话记忆（使用 LangChain Message 格式，自动累积并限制 20 条）
    messages: Annotated[Sequence[BaseMessage], add_messages] # 会话记忆，存储历史对话 all
    
    # === 澄清机制 ===
    parsed_query: Optional[dict]  # 结构化问题（ParsedQuery.model_dump()）
    clarification_count: int  # 已澄清次数（最多2次）
    skip_clarification: bool  # 用户选择跳过澄清直接执行
    clarification_question: Optional[str]  # 当前需要问用户的澄清问题
    
    # 查询计划
    query_plan: Optional[dict] # 查询计划
    current_step: int # 当前步骤
    total_steps: int # 总步骤
    
    # 中间状态
    step_results: Annotated[list[StepResult], add]  # 累积各个步骤的执行结果
    current_schema: str #当前步骤RAG检索到的DDL/Schema
    current_context: str #上一步执行的结果摘要（传递给下一步）
    current_query: str #当前步骤生成的SQL/InfluxDBQL
    
    # 重试机制
    retry_count: int  # 当前步骤的已重试次数
    max_retries: int  # 最大重试次数（默认2）
    
    # 输出
    status: StatusType  # 执行状态：running/success/no_result/error
    final_results: list[dict[str, Any]] #最终查询结果数据
    error: Optional[str]  # 错误信息（status=error 时使用）
    timing: Annotated[dict[str, float], merge_dicts]  # 自动合并

