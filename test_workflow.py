"""
测试脚本：模拟主 Agent 调用 Text2SQL Agent

测试场景：
1. 有预解析参数（serial/client_id）的情况
2. 无预解析参数的情况（触发澄清）
3. 长期记忆的存储和检索
"""
import uuid
import logging
from graph import build_text2sql_graph

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)

def test_with_preresolved_params():
    """测试：主 Agent 已提供 serial/client_id"""
    print("\n" + "="*60)
    print("测试 1: 有预解析参数")
    print("="*60)
    
    graph = build_text2sql_graph()
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    input_state = {
        "question": "这个设备最近3小时的上行流量",
        "serial": "ee38312e085b1a093ac7f40167a0c4d1",  # 主Agent预解析
        "client_id": "074864910636141238790144",  # 主Agent预解析
        "verbose": True,
        "status": "running",
        "query_plan": None,
        "current_step": 0,
        "total_steps": 0,
        "step_results": [],
        "current_schema": "",
        "current_context": "",
        "current_query": "",
        "retry_count": 0,
        "max_retries": 2,
        "final_results": [],
        "error": None,
        "timing": {},
        "parsed_query": None,
        "clarification_count": 0,
        "skip_clarification": False,
        "clarification_question": None,
    }
    
    result = graph.invoke(input_state, config)
    
    # 检查是否跳过了澄清
    next_tasks = list(graph.get_state(config).next) if graph.get_state(config).next else []
    print(f"\n下一个节点: {next_tasks}")
    print(f"澄清问题: {result.get('clarification_question')}")
    print(f"状态: {result.get('status')}")
    
    if "wait_clarification" not in next_tasks:
        print("✅ 测试通过：有预解析参数时跳过了实体相关澄清")
    else:
        print("⚠️ 测试警告：仍然触发了澄清")
    
    return result


def test_without_params():
    """测试：无预解析参数（可能触发澄清）"""
    print("\n" + "="*60)
    print("测试 2: 无预解析参数（模糊问题）")
    print("="*60)
    
    graph = build_text2sql_graph()
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    input_state = {
        "question": "查流量",  # 模糊问题
        "serial": None,
        "client_id": None,
        "verbose": True,
        "status": "running",
        "query_plan": None,
        "current_step": 0,
        "total_steps": 0,
        "step_results": [],
        "current_schema": "",
        "current_context": "",
        "current_query": "",
        "retry_count": 0,
        "max_retries": 2,
        "final_results": [],
        "error": None,
        "timing": {},
        "parsed_query": None,
        "clarification_count": 0,
        "skip_clarification": False,
        "clarification_question": None,
    }
    
    result = graph.invoke(input_state, config)
    
    next_tasks = list(graph.get_state(config).next) if graph.get_state(config).next else []
    print(f"\n下一个节点: {next_tasks}")
    print(f"澄清问题: {result.get('clarification_question')}")
    
    if "wait_clarification" in next_tasks:
        print("✅ 测试通过：模糊问题触发了澄清")
    else:
        print("ℹ️ 未触发澄清（可能问题解析为medium或high）")
    
    return result


def test_clear_question():
    """测试：清晰问题直接执行"""
    print("\n" + "="*60)
    print("测试 3: 清晰问题")
    print("="*60)
    
    graph = build_text2sql_graph()
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    
    input_state = {
        "question": "海底捞组网下有哪些设备",
        "serial": None,
        "client_id": None,
        "verbose": True,
        "status": "running",
        "query_plan": None,
        "current_step": 0,
        "total_steps": 0,
        "step_results": [],
        "current_schema": "",
        "current_context": "",
        "current_query": "",
        "retry_count": 0,
        "max_retries": 2,
        "final_results": [],
        "error": None,
        "timing": {},
        "parsed_query": None,
        "clarification_count": 0,
        "skip_clarification": False,
        "clarification_question": None,
    }
    
    result = graph.invoke(input_state, config)
    
    next_tasks = list(graph.get_state(config).next) if graph.get_state(config).next else []
    print(f"\n下一个节点: {next_tasks}")
    print(f"状态: {result.get('status')}")
    print(f"最终结果数: {len(result.get('final_results', []))}")
    
    if result.get('status') in ['success', 'no_result']:
        print("✅ 测试通过：清晰问题直接执行")
    elif "human_input" in next_tasks:
        print("✅ 测试通过：执行完成等待下一轮")
    else:
        print(f"状态: {result.get('status')}, 错误: {result.get('error')}")
    
    return result


if __name__ == "__main__":
    print("="*60)
    print("Text2SQL Agent 测试")
    print("="*60)
    
    # 运行测试
    test_with_preresolved_params()
    test_without_params()
    test_clear_question()
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)
