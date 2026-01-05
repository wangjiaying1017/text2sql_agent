"""
SQL质量评估测试脚本

使用LLM评估Text2SQL系统生成的SQL质量。
"""
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents import Text2SQLOrchestrator
from evaluation.sql_evaluator import (
    SQLEvaluator,
    extract_table_names,
    generate_evaluation_report,
)


# 测试用例 - MySQL查询（符合用户日常提问方式）
TEST_CASES = [
    # ========== 简单查询 ==========
    # 基础查询
    "帮我查一下所有客户的名称和联系电话",
    "有哪些客户的余额超过1000？",
    "列出所有状态正常的客户",
    "查一下最近一周注册的新客户",
    
    # 设备查询
    "当前有多少台已激活设备？",
    "哪些设备处于未激活状态？",
    "列出所有在北京的设备",
    "查一下版本号是2.0开头的设备",
    
    # 统计查询
    "统计各个国家的客户数量",
    "每种客户类型有多少个客户？",
    
    # ========== 联表查询 ==========
    # 客户-设备关联
    "帮我查一下每个客户有多少台设备",
    "哪些客户没有任何设备？",
    "找出设备数量超过10台的客户",
    "统计每个客户的已激活设备和未激活设备数量",
    
    # 客户-账户关联
    "查一下每个公司有多少个用户账号",
    "列出没有账号的客户",
    
    # 多表复杂查询
    "查看海底捞这个客户的所有设备信息",
    "哪些客户有在线设备但余额为0？",
    "列出各个客户的联系人信息及其设备数量",
]


def run_evaluation(
    questions: list[str] = None,
    output_file: str = "tests/evaluation_report.md",
    verbose: bool = False,
):
    """
    运行SQL质量评估。
    
    Args:
        questions: 要评估的问题列表
        output_file: 报告输出路径
        verbose: 是否显示详细输出
    """
    questions = questions or TEST_CASES
    
    print(f"\n{'='*60}")
    print(f"SQL质量评估 - 共 {len(questions)} 个问题")
    print(f"{'='*60}\n")
    
    # 初始化组件
    orchestrator = Text2SQLOrchestrator(use_rag=True, rag_top_k=3)
    evaluator = SQLEvaluator()
    
    # 收集生成结果
    generated_cases = []
    
    print("📝 阶段1: 生成SQL\n")
    for i, question in enumerate(questions, 1):
        print(f"  [{i}/{len(questions)}] {question[:50]}...")
        
        try:
            result = orchestrator.run(question, verbose=False)
            
            if result.get("error"):
                print(f"    ❌ 生成失败: {result['error'][:50]}")
                continue
            
            steps = result.get("steps_results", [])
            if steps:
                sql = steps[0].get("query", "")
                if sql:
                    generated_cases.append({
                        "question": question,
                        "sql": sql,
                    })
                    print(f"    ✅ SQL生成成功")
                else:
                    print(f"    ❌ SQL为空")
            else:
                print(f"    ❌ 无查询步骤")
                
        except Exception as e:
            print(f"    ❌ 异常: {str(e)[:50]}")
    
    if not generated_cases:
        print("\n⚠️ 没有成功生成的SQL，无法进行评估")
        return
    
    # 评估SQL质量
    print(f"\n📊 阶段2: 评估SQL质量 ({len(generated_cases)} 个)\n")
    eval_results = evaluator.evaluate_batch(generated_cases)
    
    # 生成报告
    print(f"\n📄 阶段3: 生成报告\n")
    report = generate_evaluation_report(eval_results)
    
    # 保存报告
    output_path = Path(output_file)
    output_path.write_text(report, encoding="utf-8")
    print(f"✅ 报告已保存到: {output_path}")
    
    # 保存JSON结果
    json_path = output_path.with_suffix(".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(eval_results, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON结果已保存到: {json_path}")
    
    # 打印摘要
    successful = [r for r in eval_results if r["evaluation"]]
    if successful:
        avg_score = sum(r["evaluation"]["overall_score"] for r in successful) / len(successful)
        correct = sum(1 for r in successful if r["evaluation"]["is_correct"])
        
        print(f"\n{'='*60}")
        print(f"评估完成")
        print(f"  综合平均分: {avg_score:.1f}/10")
        print(f"  正确率: {correct}/{len(successful)} ({correct/len(successful)*100:.1f}%)")
        print(f"{'='*60}\n")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="SQL质量评估")
    parser.add_argument(
        "-q", "--questions",
        nargs="+",
        help="要评估的问题"
    )
    parser.add_argument(
        "-o", "--output",
        default="tests/evaluation_report.md",
        help="报告输出路径"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="显示详细输出"
    )
    
    args = parser.parse_args()
    
    run_evaluation(
        questions=args.questions,
        output_file=args.output,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
