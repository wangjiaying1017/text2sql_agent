"""
意图识别耗时测试脚本

测量各阶段的耗时分布：
1. Prompt 构建时间
2. LLM API 调用时间
3. 响应解析时间
"""
import time
import sys
sys.path.insert(0, 'd:/code_project/text2sql_agent')

from langchain_core.prompts import ChatPromptTemplate
from llm.client import get_qwen_model
from intent.prompts import INTENT_RECOGNITION_SYSTEM_PROMPT, INTENT_RECOGNITION_USER_PROMPT
from intent.recognizer import QueryPlan


def measure_intent_recognition(question: str, context: str = ""):
    """测量意图识别各阶段耗时"""
    
    print(f"\n{'='*60}")
    print(f"测试问题: {question}")
    print(f"{'='*60}\n")
    
    timings = {}
    
    # 1. 模型初始化
    print("[1/5] 初始化模型...")
    t0 = time.time()
    base_llm = get_qwen_model(temperature=0)
    timings['model_init'] = time.time() - t0
    print(f"   ✓ 模型初始化完成: {timings['model_init']:.3f}s")
    
    # 2. 绑定结构化输出
    print("[2/5] 绑定结构化输出...")
    t0 = time.time()
    llm = base_llm.with_structured_output(QueryPlan)
    timings['structured_output_binding'] = time.time() - t0
    print(f"   ✓ 结构化输出绑定完成: {timings['structured_output_binding']:.3f}s")
    
    # 3. 构建 Prompt
    print("[3/5] 构建 Prompt...")
    t0 = time.time()
    prompt = ChatPromptTemplate.from_messages([
        ("system", INTENT_RECOGNITION_SYSTEM_PROMPT),
        ("human", INTENT_RECOGNITION_USER_PROMPT),
    ])
    
    # 格式化 prompt
    formatted_prompt = prompt.format_messages(
        question=question,
        context=context if context else "无历史上下文"
    )
    timings['prompt_construction'] = time.time() - t0
    
    # 计算 prompt 长度
    total_chars = sum(len(msg.content) for msg in formatted_prompt)
    print(f"   ✓ Prompt 构建完成: {timings['prompt_construction']:.3f}s")
    print(f"   📝 Prompt 总字符数: {total_chars:,}")
    
    # 4. LLM API 调用
    print("[4/5] 调用 LLM API...")
    t0 = time.time()
    chain = prompt | llm
    result = chain.invoke({
        "question": question,
        "context": context if context else "无历史上下文",
    })
    timings['llm_api_call'] = time.time() - t0
    print(f"   ✓ LLM API 调用完成: {timings['llm_api_call']:.3f}s")
    
    # 5. 后处理
    print("[5/5] 后处理...")
    t0 = time.time()
    for step in result.steps:
        step.database = step.database.lower()
    timings['post_processing'] = time.time() - t0
    print(f"   ✓ 后处理完成: {timings['post_processing']:.3f}s")
    
    # 汇总
    total_time = sum(timings.values())
    
    print(f"\n{'='*60}")
    print("📊 耗时分布汇总")
    print(f"{'='*60}")
    print(f"{'阶段':<30} {'耗时':<10} {'占比':<10}")
    print(f"{'-'*60}")
    
    for stage, duration in timings.items():
        stage_name = {
            'model_init': '模型初始化',
            'structured_output_binding': '结构化输出绑定',
            'prompt_construction': 'Prompt 构建',
            'llm_api_call': 'LLM API 调用',
            'post_processing': '后处理'
        }.get(stage, stage)
        percentage = (duration / total_time) * 100
        bar = '█' * int(percentage / 2)
        print(f"{stage_name:<25} {duration:>6.3f}s  {percentage:>5.1f}% {bar}")
    
    print(f"{'-'*60}")
    print(f"{'总计':<25} {total_time:>6.3f}s  100.0%")
    print(f"{'='*60}\n")
    
    # 输出结果摘要
    print("📋 识别结果:")
    print(f"   置信度: {result.confidence}")
    print(f"   需要澄清: {result.needs_clarification}")
    print(f"   步骤数量: {len(result.steps)}")
    if result.steps:
        for step in result.steps:
            print(f"   - Step {step.step}: [{step.database}] {step.purpose}")
    
    return timings, result


def main():
    """运行测试"""
    # 测试用例
    test_questions = [
        "查询客户张三的设备列表",
        "查询序列号为ABC123的设备最近1小时的流量数据",
        "统计所有在线设备的数量",
    ]
    
    print("\n" + "="*70)
    print("🔬 意图识别耗时测试")
    print("="*70)
    
    all_timings = []
    
    for q in test_questions:
        try:
            timings, _ = measure_intent_recognition(q)
            all_timings.append(timings)
        except Exception as e:
            print(f"\n❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
    
    # 计算平均耗时
    if len(all_timings) > 1:
        print("\n" + "="*70)
        print("📊 平均耗时统计 (基于 {} 个测试)".format(len(all_timings)))
        print("="*70)
        
        avg_timings = {}
        for key in all_timings[0].keys():
            avg_timings[key] = sum(t[key] for t in all_timings) / len(all_timings)
        
        total_avg = sum(avg_timings.values())
        for stage, duration in avg_timings.items():
            stage_name = {
                'model_init': '模型初始化',
                'structured_output_binding': '结构化输出绑定',
                'prompt_construction': 'Prompt 构建',
                'llm_api_call': 'LLM API 调用',
                'post_processing': '后处理'
            }.get(stage, stage)
            print(f"{stage_name:<25} {duration:>6.3f}s")
        print(f"{'-'*40}")
        print(f"{'平均总耗时':<25} {total_avg:>6.3f}s")


if __name__ == "__main__":
    main()
