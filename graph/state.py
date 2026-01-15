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
    合并消息列表，保留最新的 20 条消息。
    """
    result = list(left) + list(right)
    # 限制最多 20 条消息
    if len(result) > 20:
        result = result[-20:]
    return result


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
    # 输入
    question: str # 用户问题
    verbose: bool # 是否显示详细日志信息
    
    # 会话记忆（使用 LangChain Message 格式，自动累积并限制 20 条）
    messages: Annotated[Sequence[BaseMessage], add_messages] # 会话记忆，存储历史对话 all
    
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

