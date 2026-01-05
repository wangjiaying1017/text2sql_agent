"""
测试Text2SQL查询系统

自动运行测试用例并生成测试报告。
"""
import json
import time
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents import Text2SQLOrchestrator


# 测试用例定义
TEST_CASES = {
    "简单查询-客户": [
        "帮我查一下所有公司的名称和联系电话",
        "有哪些客户的余额超过1000？",
        "列出所有状态正常的客户",
        "有多少客户来自中国？",
    ],
    "简单查询-设备": [
        "帮我统计一下当前在线的设备数量",
        "哪些设备是未激活状态？",
        "列出所有在北京的设备",
        "统计各个国家的设备数量",
    ],
    "联表查询-客户设备": [
        "列出每个客户的设备数量",
        "哪些客户没有任何设备？",
        "查看设备数量超过10台的客户",
        "统计各个客户的在线设备数和离线设备数",
    ],
    "联表查询-设备配置": [
        "哪些设备配置了AC功能？",
        "列出所有配置了HA高可用的设备及其客户名称",
        "统计每种设备类型的数量及其所属客户",
    ],
    "复杂查询": [
        "列出所有客户及其设备数量、账号数量",
        "统计每个国家的客户数和设备数",
    ],
}


class TestResult:
    """单个测试结果"""
    
    def __init__(self, question: str, category: str):
        self.question = question
        self.category = category
        self.success = False
        self.error: Optional[str] = None
        self.strategy: Optional[str] = None
        self.generated_sql: Optional[str] = None
        self.execution_time: float = 0
        self.result_count: int = 0
    
    def to_dict(self) -> dict:
        return {
            "question": self.question,
            "category": self.category,
            "success": self.success,
            "error": self.error,
            "strategy": self.strategy,
            "generated_sql": self.generated_sql,
            "execution_time": round(self.execution_time, 2),
            "result_count": self.result_count,
        }


def run_test(
    orchestrator: Text2SQLOrchestrator,
    question: str,
    category: str,
    verbose: bool = False,
) -> TestResult:
    """
    运行单个测试用例。
    
    Args:
        orchestrator: 编排器实例
        question: 测试问题
        category: 测试类别
        verbose: 是否输出详细信息
        
    Returns:
        测试结果
    """
    result = TestResult(question, category)
    
    start_time = time.time()
    
    try:
        # 运行查询（不显示详细输出）
        response = orchestrator.run(question, verbose=verbose)
        
        result.execution_time = time.time() - start_time
        
        # 检查是否有错误
        if response.get("error"):
            result.error = response.get("error")
        else:
            result.success = True
            
            # 获取策略
            plan = response.get("plan", {})
            result.strategy = plan.get("strategy", "unknown") if plan else "unknown"
            
            # 获取生成的SQL和结果
            steps_results = response.get("steps_results", [])
            if steps_results:
                first_step = steps_results[0]
                result.generated_sql = first_step.get("query", "")
                step_result = first_step.get("result", [])
                result.result_count = len(step_result) if isinstance(step_result, list) else 0
            
    except Exception as e:
        result.execution_time = time.time() - start_time
        result.error = str(e)
    
    return result


def run_all_tests(
    execute_sql: bool = False,
    verbose: bool = False,
    categories: Optional[list] = None,
) -> list[TestResult]:
    """
    运行所有测试用例。
    
    Args:
        execute_sql: 是否实际执行SQL（False则只测试SQL生成）
        verbose: 是否输出详细信息
        categories: 要运行的类别（None表示全部）
        
    Returns:
        所有测试结果
    """
    # 创建orchestrator
    orchestrator = Text2SQLOrchestrator(
        use_rag=True,
        rag_top_k=5,
    )
    
    results = []
    total = sum(len(cases) for cat, cases in TEST_CASES.items() 
                if categories is None or cat in categories)
    current = 0
    
    print(f"\n{'='*60}")
    print(f"开始测试 - 共 {total} 个用例")
    print(f"{'='*60}\n")
    
    for category, questions in TEST_CASES.items():
        if categories and category not in categories:
            continue
            
        print(f"\n📁 [{category}]")
        print("-" * 40)
        
        for question in questions:
            current += 1
            print(f"\n  [{current}/{total}] {question[:50]}...")
            
            result = run_test(orchestrator, question, category, verbose=verbose)
            results.append(result)
            
            if result.success:
                print(f"  ✅ 成功 ({result.execution_time:.1f}s)")
                if result.generated_sql:
                    # 只显示SQL的前80个字符
                    sql_preview = result.generated_sql.replace('\n', ' ')[:80]
                    print(f"     SQL: {sql_preview}...")
            else:
                print(f"  ❌ 失败: {result.error[:60] if result.error else 'Unknown'}...")
    
    return results


def generate_report(results: list[TestResult], output_file: str = None) -> str:
    """
    生成测试报告。
    
    Args:
        results: 测试结果列表
        output_file: 输出文件路径（可选）
        
    Returns:
        报告内容
    """
    total = len(results)
    passed = sum(1 for r in results if r.success)
    failed = total - passed
    
    # 按类别统计
    category_stats = {}
    for r in results:
        if r.category not in category_stats:
            category_stats[r.category] = {"total": 0, "passed": 0}
        category_stats[r.category]["total"] += 1
        if r.success:
            category_stats[r.category]["passed"] += 1
    
    # 生成报告
    report_lines = [
        "# Text2SQL 测试报告",
        f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 总体统计",
        "",
        f"| 指标 | 数值 |",
        f"|------|------|",
        f"| 总用例数 | {total} |",
        f"| 通过 | {passed} |",
        f"| 失败 | {failed} |",
        f"| 通过率 | {passed/total*100:.1f}% |",
        "",
        "## 分类统计",
        "",
        "| 类别 | 通过/总数 | 通过率 |",
        "|------|----------|--------|",
    ]
    
    for cat, stats in category_stats.items():
        rate = stats["passed"] / stats["total"] * 100
        report_lines.append(f"| {cat} | {stats['passed']}/{stats['total']} | {rate:.0f}% |")
    
    report_lines.extend([
        "",
        "## 详细结果",
        "",
    ])
    
    # 先显示失败的
    if failed > 0:
        report_lines.append("### ❌ 失败用例")
        report_lines.append("")
        for r in results:
            if not r.success:
                report_lines.append(f"- **{r.question}**")
                report_lines.append(f"  - 错误: {r.error}")
                report_lines.append("")
    
    # 显示成功的
    report_lines.append("### ✅ 成功用例")
    report_lines.append("")
    for r in results:
        if r.success:
            report_lines.append(f"- **{r.question}** ({r.execution_time:.1f}s)")
            if r.generated_sql:
                sql_preview = r.generated_sql.replace('\n', ' ')[:100]
                report_lines.append(f"  - SQL: `{sql_preview}...`")
            report_lines.append("")
    
    report = "\n".join(report_lines)
    
    # 保存报告
    if output_file:
        Path(output_file).write_text(report, encoding="utf-8")
        print(f"\n📄 报告已保存到: {output_file}")
    
    return report


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="测试Text2SQL查询系统")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="显示详细输出"
    )
    parser.add_argument(
        "-c", "--category",
        nargs="+",
        help="只运行指定类别的测试"
    )
    parser.add_argument(
        "-o", "--output",
        default="tests/test_report.md",
        help="测试报告输出路径 (默认: tests/test_report.md)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="列出所有测试类别"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="同时输出JSON格式结果"
    )
    
    args = parser.parse_args()
    
    if args.list:
        print("\n可用测试类别:")
        for cat, cases in TEST_CASES.items():
            print(f"  - {cat} ({len(cases)}个用例)")
        return
    
    # 运行测试
    results = run_all_tests(
        verbose=args.verbose,
        categories=args.category,
    )
    
    # 生成报告
    print("\n" + "="*60)
    print("生成测试报告...")
    print("="*60)
    
    report = generate_report(results, args.output)
    
    # 输出JSON
    if args.json:
        json_file = args.output.replace(".md", ".json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in results], f, ensure_ascii=False, indent=2)
        print(f"📄 JSON结果已保存到: {json_file}")
    
    # 显示摘要
    total = len(results)
    passed = sum(1 for r in results if r.success)
    
    print(f"\n{'='*60}")
    print(f"测试完成: {passed}/{total} 通过 ({passed/total*100:.1f}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
