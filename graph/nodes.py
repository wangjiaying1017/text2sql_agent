"""
LangGraph 节点函数

实现 Text2SQL 工作流的各个节点，使用 Command 模式进行状态更新和流程控制。
"""
from typing import Any, Optional
import time
import json
import logging
from langgraph.types import Command
from langchain_core.runnables import RunnableConfig

from .state import Text2SQLState

# 获取日志记录器
logger = logging.getLogger("text2sql.nodes")


# ============== 日志辅助函数 ==============
def log_node_start(node_name: str, state: Text2SQLState, show_fields: list[str] = None):
    """记录节点开始执行（DEBUG 级别）。"""
    logger.info(f"[{node_name}] 开始执行")
    
    # DEBUG 级别才显示详细输入
    if logger.isEnabledFor(logging.DEBUG) and show_fields:
        for field in show_fields:
            value = state.get(field)
            if value is not None:
                str_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                logger.debug(f"  输入 {field}: {str_value}")






# ============== 单例缓存（避免重复初始化） ==============
_intent_recognizer = None
_mysql_retriever = None
_influxdb_retriever = None
_sql_generator = None


def get_intent_recognizer():
    """获取 IntentRecognizer 单例。"""
    global _intent_recognizer
    if _intent_recognizer is None:
        from intent import IntentRecognizer
        logger.debug("首次初始化 IntentRecognizer...")
        _intent_recognizer = IntentRecognizer()
    return _intent_recognizer


def get_hybrid_retriever(database_type: str):
    """获取 HybridRetriever 单例（按数据库类型）。"""
    global _mysql_retriever, _influxdb_retriever
    from retrieval import HybridRetriever
    
    if database_type == "mysql":
        if _mysql_retriever is None:
            logger.debug("首次初始化 MySQL HybridRetriever...")
            _mysql_retriever = HybridRetriever(database_type="mysql")
        return _mysql_retriever
    else:
        if _influxdb_retriever is None:
            logger.debug("首次初始化 InfluxDB HybridRetriever...")
            _influxdb_retriever = HybridRetriever(database_type="influxdb")
        return _influxdb_retriever


def get_sql_generator():
    """获取 SQLGenerator 单例。"""
    global _sql_generator
    if _sql_generator is None:
        from agents.sql_generator import SQLGenerator
        logger.debug("首次初始化 SQLGenerator...")
        _sql_generator = SQLGenerator()
    return _sql_generator


def warmup_all(database_types: list[str] = None) -> dict[str, float]:
    """
    预热所有组件的连接和 API。
    
    在应用启动时调用，可以显著减少首次查询的延迟。
    
    Args:
        database_types: 要预热的数据库类型列表，默认 ["mysql"]
        
    Returns:
        各组件预热耗时的字典
    """
    import time
    
    if database_types is None:
        database_types = ["mysql"]
    
    logger.info("[WARMUP] 开始预热...")
    
    timings = {}
    total_start = time.time()
    
    # 1. 预热 IntentRecognizer（LLM 客户端 + Qdrant）
    t0 = time.time()
    intent_recognizer = get_intent_recognizer()
    timings["intent_recognizer_init"] = time.time() - t0
    logger.info(f"  IntentRecognizer 初始化: {timings['intent_recognizer_init']:.2f}s")
    
    # 1.1 预热 IntentRecognizer 的 Qdrant 连接（并行）
    intent_warmup = intent_recognizer.warmup()
    timings["intent_qdrant_warmup"] = intent_warmup.get("total", 0)
    
    # 2. 预热 HybridRetriever（Qdrant + ES + Embedding API）
    for db_type in database_types:
        retriever = get_hybrid_retriever(db_type)
        retriever_timings = retriever.warmup()
        for key, value in retriever_timings.items():
            timings[f"retriever_{key}"] = value
    
    # 3. 预热 SQLGenerator（LLM 客户端）
    t0 = time.time()
    get_sql_generator()
    timings["sql_generator_init"] = time.time() - t0
    logger.info(f"  SQLGenerator 初始化: {timings['sql_generator_init']:.2f}s")
    
    total_time = time.time() - total_start
    timings["total"] = total_time
    
    logger.info(f"[WARMUP] 预热完成，总耗时: {total_time:.2f}s")
    
    return timings
# ======================================================


def format_context(
    results: list[dict[str, Any]], 
    max_rows: int = 20,
    max_tokens: int = 2000
) -> str:
    """
    将查询结果压缩为摘要上下文，避免上下文溢出。
    
    策略：
    1. 限制行数：最多保留 max_rows 条关键记录
    2. 限制 Token：估算 token 数，超限时进一步压缩
    3. 仅提取关键字段：如 id、name、serial 等可作为后续查询条件的字段
    """
    if not results:
        return "上一步查询无结果"
    
    total_count = len(results)
    
    # 策略1: 限制行数
    if total_count > max_rows:
        results = results[:max_rows]
        truncated = True
    else:
        truncated = False
    
    # 策略2: 提取关键字段（用于后续查询的 ID/标识符）
    key_fields = ["id", "serial", "client_id", "name", "device_id", "node_id"]
    summaries = []
    for row in results:
        key_values = {k: v for k, v in row.items() if k in key_fields}
        if key_values:
            summaries.append(str(key_values))
        else:
            # 兜底：取前 3 个字段
            summaries.append(str(dict(list(row.items())[:3])))
    
    context = f"上一步查询返回 {total_count} 条记录"
    if truncated:
        context += f"（仅显示前 {max_rows} 条）"
    context += ":\n" + "\n".join(summaries)
    
    # 策略3: Token 估算保护
    estimated_tokens = len(context) // 4  # 粗略估算
    if estimated_tokens > max_tokens:
        # 进一步压缩：只保留 ID 列表
        ids = [r.get("id") or r.get("serial") for r in results[:10] if r.get("id") or r.get("serial")]
        context = f"上一步查询返回 {total_count} 条记录，关键ID: {ids}"
    
    return context


def intent_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """意图识别节点 - 生成查询计划"""
    log_node_start("intent_node", state, ["question", "messages"])
    total_start = time.time()
    
    # 1. 获取 IntentRecognizer 单例
    recognizer = get_intent_recognizer()
    
    try:
        # 2. 构建会话上下文（结构化提取 + 最近问题）
        from utils.context_utils import extract_context_from_messages, format_extracted_context
        
        context_parts = []
        messages = state.get("messages", [])
        
        if messages:
            # 2.1 提取结构化上下文（设备/客户信息）
            extracted = extract_context_from_messages(messages, max_history=3)
            if extracted:
                context_parts.append("参考信息：\n" + format_extracted_context(extracted))
            
            # 2.2 保留最近的问题（不含 AI 回复的 JSON）
            recent_questions = [
                msg.content for msg in messages[-4:]
                if msg.type == "human"
            ]
            if recent_questions:
                context_parts.append("最近的问题：\n" + "\n".join(f"- {q}" for q in recent_questions))
        
        context = "\n\n".join(context_parts)
        if context:
            logger.debug(f"使用结构化会话上下文")
        
        # 3. 执行意图识别（开启 verbose 查看 prompt）
        plan = recognizer.recognize(state["question"], context=context, verbose=True)
        
        total_time = time.time() - total_start
        
        if not plan.steps:
            updates = {"error": "无法生成有效的查询计划", "timing": {"intent": round(total_time, 2)}}
            return Command(update=updates, goto="error_handler")
        
        # 输出生成的查询计划
        logger.info(f"[intent_node] 生成查询计划：共 {len(plan.steps)} 步")
        for step in plan.steps:
            logger.info(f"  Step {step.step}: [{step.database}] {step.purpose}")
        
        updates = {
            "query_plan": plan.model_dump(),
            "total_steps": len(plan.steps),
            "current_step": 0,
            "retry_count": 0,
            "timing": {"intent": round(total_time, 2)}
        }
        return Command(update=updates)
        
    except Exception as e:
        total_time = time.time() - total_start
        updates = {"error": f"意图识别失败: {str(e)}", "timing": {"intent": round(total_time, 2)}}
        return Command(update=updates, goto="error_handler")


def plan_validator_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """计划校验节点 - 验证生成的计划是否合理"""
    log_node_start("plan_validator_node", state, ["query_plan"])
    
    plan = state["query_plan"]
    errors = []
    
    # 校验1: 步骤不能为空
    if not plan.get("steps"):
        errors.append("查询计划没有执行步骤")
        updates = {"error": f"计划校验失败: {'; '.join(errors)}"}
        return Command(update=updates, goto="error_handler")
    
    steps = plan.get("steps", [])
    
    # 校验2: 步骤编号连续性
    if len(steps) > 1:
        step_ids = [s["step"] for s in steps]
        expected_from_0 = list(range(len(step_ids)))
        expected_from_1 = list(range(1, len(step_ids) + 1))
        if step_ids != expected_from_0 and step_ids != expected_from_1:
            errors.append(f"步骤编号不连续: {step_ids}")
    
    # 校验3: 数据库类型有效性
    valid_dbs = {"mysql", "influxdb"}
    for step in steps:
        if step.get("database") not in valid_dbs:
            errors.append(f"步骤{step['step']}的数据库类型无效: {step.get('database')}")
    
    # 校验4: 依赖关系合理性
    if len(steps) > 1:
        for step in steps:
            depends_on = step.get("depends_on")
            if depends_on is not None:
                if depends_on >= step["step"]:
                    errors.append(f"步骤{step['step']}依赖了后续步骤{depends_on}")
    
    if errors:
        updates = {"error": f"计划校验失败: {'; '.join(errors)}"}
        return Command(update=updates, goto="error_handler")
    
    return Command(update={})  # 校验通过，继续执行


def rag_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """RAG检索节点 - 获取相关 Schema"""
    log_node_start("rag_node", state, ["question", "current_step", "total_steps", "current_context"])
    total_start = time.time()
    step = state["query_plan"]["steps"][state["current_step"]]
    
    # 1. 获取 HybridRetriever 单例
    retriever = get_hybrid_retriever(step["database"])
    
    # 2. 构建增强检索查询
    search_query = state["question"]
    if state["current_step"] > 0 or state["total_steps"] > 1:
        search_query = f"{state['question']} {step['purpose']}"
        logger.debug(f"增强检索查询: {search_query}")
    
    # 3. 执行 RAG 检索
    schema = retriever.get_ddl_for_query(search_query)
    
    total_time = time.time() - total_start
    
    logger.info(f"rag耗时:{total_time}s")
    return Command(update={
        "current_schema": schema, 
        "timing": {f"rag_step{state['current_step']}": round(total_time, 2)},
        "retry_count": 0
    })


def sql_gen_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """SQL生成节点 - 根据 Schema 和上下文生成 SQL"""
    log_node_start("sql_gen_node", state, ["question", "current_step", "current_context", "retry_count"])
    total_start = time.time()
    step = state["query_plan"]["steps"][state["current_step"]]
    
    # 获取上下文：优先使用 current_context（当前步骤的结果），否则从 messages 历史提取
    from utils.context_utils import extract_context_from_messages, format_extracted_context
    
    context = state.get("current_context", "")
    
    # 如果没有当前上下文，从历史消息中提取
    if not context and state.get("messages"):
        extracted = extract_context_from_messages(state["messages"])
        if extracted:
            context = f"历史对话上下文：\n{format_extracted_context(extracted)}"
            logger.debug(f"从历史消息提取上下文: {extracted}")
    
    # 获取 SQLGenerator 单例
    generator = get_sql_generator()
    
    # 生成 SQL
    sql = generator.generate(
        question=state["question"],
        purpose=step["purpose"],
        database_type=step["database"],
        schema=state["current_schema"],
        context=context if context else "无"
    )
    
    total_time = time.time() - total_start
    
    # 记录生成的 SQL 语句
    logger.info(f"[sql_gen_node] 生成 SQL:\n{sql}")
    
    retry_count = state.get("retry_count", 0)
    timing_key = f"sql_gen_step{state['current_step']}"
    if retry_count > 0:
        timing_key += f"_retry{retry_count}"
    
    updates = {
        "current_query": sql,
        "timing": {timing_key: round(total_time, 2)}
    }
    
    return Command(update=updates)


def execute_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """执行查询节点 - 执行 SQL 并决定下一步（支持重试）"""
    from database import MySQLConnector, InfluxDBConnector
    
    log_node_start("execute_node", state, ["current_query", "current_step", "retry_count"])
    start_time = time.time()
    step = state["query_plan"]["steps"][state["current_step"]]
    retry_count = state.get("retry_count", 0)
    max_retries = state.get("max_retries", 2)
    
    try:
        # 根据数据库类型执行查询
        if step["database"] == "mysql":
            with MySQLConnector() as conn:
                result = conn.execute(state["current_query"])
        else:
            with InfluxDBConnector() as conn:
                result = conn.execute(state["current_query"])
        
        elapsed = time.time() - start_time
        step_timing = {f"execute_step{state['current_step']}": round(elapsed, 2)}
        
        new_step = state["current_step"] + 1
        
        if new_step >= state["total_steps"]:
            # 所有步骤完成
            # 判断是否有结果
            has_results = len(result) > 0
            
            updates = {
                "step_results": [{
                    "step_id": state["current_step"],
                    "database": step["database"],
                    "query": state["current_query"],
                    "results": f"<{len(result)} 条记录>",
                    "error": None
                }],
                "timing": step_timing
            }
            
            return Command(
                update={
                    "step_results": [{
                        "step_id": state["current_step"],
                        "database": step["database"],
                        "query": state["current_query"],
                        "results": result,
                        "error": None
                    }],
                    "timing": step_timing
                },
                goto="aggregate"
            )
        else:
            # 还有后续步骤
            # 如果中间步骤返回空结果，提前终止
            if not result:
                step_info = step.get("purpose", f"步骤 {state['current_step'] + 1}")
                error_msg = f"中间步骤无结果：{step_info}。请检查查询条件是否正确。"
                logger.warning(f"[execute_node] 中间步骤返回空结果，提前终止")
                
                return Command(
                    update={
                        "status": "no_result",
                        "error": error_msg,
                        "step_results": [{
                            "step_id": state["current_step"],
                            "database": step["database"],
                            "query": state["current_query"],
                            "results": [],
                            "error": error_msg
                        }],
                        "final_results": [],
                        "timing": step_timing
                    },
                    goto="error_handler"
                )
            
            context_summary = format_context(result, max_rows=20, max_tokens=2000)
            updates = {
                "step_results": [{
                    "step_id": state["current_step"],
                    "database": step["database"],
                    "results": f"<{len(result)} 条记录>"
                }],
                "current_step": new_step,
                "current_context": context_summary[:100] + "...",
                "timing": step_timing
            }
            
            return Command(
                update={
                    "step_results": [{
                        "step_id": state["current_step"],
                        "database": step["database"],
                        "query": state["current_query"],
                        "results": result,
                        "error": None
                    }],
                    "current_step": new_step,
                    "current_context": context_summary,
                    "timing": step_timing
                },
                goto="rag"
            )
    except Exception as e:
        elapsed = time.time() - start_time
        error_msg = str(e)
        step_timing = {f"execute_step{state['current_step']}_retry{retry_count}": round(elapsed, 2)}
        
        # 判断是否可重试的错误
        is_retryable = any(keyword in error_msg.lower() for keyword in [
            "syntax", "error in your sql", "unknown column", "doesn't exist",
            "table", "ambiguous", "parse",
            # InfluxDB 相关错误
            "not executed", "error parsing query", "undefined identifier",
            "invalid", "measurement not found"
        ])
        
        if is_retryable and retry_count < max_retries:
            updates = {
                "retry_count": retry_count + 1,
                "error_msg": error_msg[:100],
            }
            
            return Command(
                update={
                    "retry_count": retry_count + 1,
                    "current_context": f"上次 SQL 执行失败，错误信息: {error_msg}\n请修正 SQL 语法问题。",
                    "timing": step_timing
                },
                goto="sql_gen"
            )
        else:
            if retry_count >= max_retries:
                error_msg = f"已重试 {max_retries} 次仍失败: {error_msg}"
            updates = {"error": error_msg[:100]}
            
            return Command(
                update={
                    "error": f"查询执行失败: {error_msg}",
                    "timing": step_timing
                },
                goto="error_handler"
            )


def aggregate_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """结果聚合节点 - 汇总所有步骤的结果"""
    log_node_start("aggregate_node", state, ["step_results"])
    
    if state.get("step_results"):
        last_result = state["step_results"][-1]
        final_results = last_result.get("results", [])
    else:
        final_results = []
    
    # 🆕 使用 LangChain Message 格式保存对话历史
    from langchain_core.messages import HumanMessage, AIMessage
    import json
    
    new_messages = []
    
    # 添加用户原始问题
    new_messages.append(HumanMessage(content=state["question"]))
    
    # 构建精简的 AI 回复（只保留后续对话需要的关键信息）
    from utils.context_utils import extract_key_fields
    
    ai_response = {
        "question": state.get("question"),  # 原始问题（用于上下文理解）
        "databases_used": list(set(s.get("database") for s in state.get("step_results", []))),
        "result_count": len(final_results),
        "result_sample": extract_key_fields(final_results, max_rows=5)  # 精简的关键字段
    }
    new_messages.append(AIMessage(content=json.dumps(ai_response, ensure_ascii=False, default=str)))
    
    # 判断是否有结果
    has_results = len(final_results) > 0
    status = "success" if has_results else "no_result"
    
    updates = {
        "status": status,
        "final_results": f"<{len(final_results)} 条结果>",
        "messages": new_messages  # 追加到历史（使用 add_messages reducer）
    }
    
    return Command(update={
        "status": status,
        "final_results": final_results,
        "messages": new_messages
    })


def error_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """错误处理节点"""
    log_node_start("error_node", state, ["error", "status"])
    
    # 保持原有的 status（可能是 no_result），否则设为 error
    current_status = state.get("status")
    final_status = current_status if current_status in ["no_result"] else "error"
    
    updates = {
        "status": final_status,
        "final_results": [],
        "error": state.get("error", "未知错误")
    }
    
    return Command(update=updates)


def human_input_node(state: Text2SQLState, config: RunnableConfig) -> Command:
    """
    人工输入节点 - 等待用户下一轮输入的占位节点。
    
    配合 interrupt_before=["human_input"] 使用，工作流会在此节点前暂停。
    MemorySaver 会自动保存当前 state（包含 messages 历史）。
    用户下次 invoke 时会恢复状态并继续。
    """
    log_node_start("human_input_node", state, ["messages"])
    
    # 这个节点实际上不会被执行（因为 interrupt_before）
    # 但如果执行了，就返回空更新
    
    return Command(update={})

