"""
LangGraph 图构建器

构建 Text2SQL 工作流的 StateGraph。
"""
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from .state import Text2SQLState
from .nodes import (
    intent_node,
    plan_validator_node,
    rag_node,
    sql_gen_node,
    execute_node,
    aggregate_node,
    error_node,
    human_input_node,
)


# 全局 checkpointer 单例（用于会话记忆）
_memory_saver = None


def get_memory_saver():
    """获取 MemorySaver 单例。"""
    global _memory_saver
    if _memory_saver is None:
        _memory_saver = MemorySaver()
    return _memory_saver


def build_text2sql_graph(checkpointer=None):
    """
    构建 Text2SQL 工作流图（Command 模式 + 计划校验 + 多轮对话）。
    
    工作流:
        START → intent → plan_validator → rag → sql_gen → execute → aggregate → human_input → END
                   ↓                                         ↓
              error_handler ←────────────────────────────────┘
                   ↓
                  END
    
    Args:
        checkpointer: 可选的 checkpointer，用于会话记忆。默认使用 MemorySaver。
    
    Returns:
        编译后的 LangGraph 工作流
    """
    builder = StateGraph(Text2SQLState)
    
    # 添加节点
    builder.add_node("intent", intent_node)
    builder.add_node("plan_validator", plan_validator_node)
    builder.add_node("rag", rag_node)
    builder.add_node("sql_gen", sql_gen_node)
    builder.add_node("execute", execute_node)
    builder.add_node("aggregate", aggregate_node)
    builder.add_node("human_input", human_input_node)  # 🆕 等待用户输入的节点
    builder.add_node("error_handler", error_node)
    
    # 入口
    builder.add_edge(START, "intent")
    
    # intent → plan_validator → rag 的顺序执行链
    builder.add_edge("intent", "plan_validator")
    builder.add_edge("plan_validator", "rag")
    
    # 顺序执行链
    builder.add_edge("rag", "sql_gen")
    builder.add_edge("sql_gen", "execute")
    # execute 节点通过 Command.goto 决定跳转到 "rag" 或 "aggregate" 或 "error_handler"
    
    # aggregate → human_input → END
    builder.add_edge("aggregate", "human_input")
    builder.add_edge("human_input", END)
    builder.add_edge("error_handler", END)
    
    # 使用 checkpointer 编译（支持会话记忆）
    # 在 human_input 节点之前暂停，等待用户输入
    if checkpointer is None:
        checkpointer = get_memory_saver()
    
    return builder.compile(
        checkpointer=checkpointer,
        interrupt_before=["human_input"]  # 🆕 在此节点前暂停
    )


# 全局单例
_graph_instance = None


def get_text2sql_graph():
    """获取 Text2SQL 工作流图的单例实例。"""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = build_text2sql_graph()
    return _graph_instance


