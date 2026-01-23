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
    构建 Text2SQL 工作流图（简化版，无澄清机制）。
    
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
    
    # ============== 添加节点 ==============
    builder.add_node("intent", intent_node)
    builder.add_node("plan_validator", plan_validator_node)
    builder.add_node("rag", rag_node)
    builder.add_node("sql_gen", sql_gen_node)
    builder.add_node("execute", execute_node)
    builder.add_node("aggregate", aggregate_node)
    builder.add_node("human_input", human_input_node)
    builder.add_node("error_handler", error_node)
    
    # ============== 入口（直接进入 intent） ==============
    builder.add_edge(START, "intent")
    
    # ============== 主流程 ==============
    # intent → plan_validator → rag
    builder.add_edge("intent", "plan_validator")
    builder.add_edge("plan_validator", "rag")
    
    # rag → sql_gen → execute
    builder.add_edge("rag", "sql_gen")
    builder.add_edge("sql_gen", "execute")
    # execute 通过 Command.goto 决定跳转到 "rag" 或 "aggregate" 或 "error_handler"
    
    # aggregate → human_input → END
    builder.add_edge("aggregate", "human_input")
    builder.add_edge("human_input", END)
    builder.add_edge("error_handler", END)
    
    # ============== 编译 ==============
    if checkpointer is None:
        checkpointer = get_memory_saver()
    
    return builder.compile(
        checkpointer=checkpointer,
        # 在 human_input 节点前暂停，等待用户输入
        interrupt_before=["human_input"]
    )


# 全局单例
_graph_instance = None


def get_text2sql_graph():
    """获取 Text2SQL 工作流图的单例实例。"""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = build_text2sql_graph()
    return _graph_instance
