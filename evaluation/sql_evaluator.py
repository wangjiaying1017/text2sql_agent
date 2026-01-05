"""
SQL质量评估模块

提供SQL语句质量评估功能，包括：
- 从SQL中提取表名
- 从schema JSON获取DDL
- 使用LLM评估SQL质量
"""
import re
import json
from pathlib import Path
from typing import Optional, Any
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

from llm.client import create_model
from config import settings
from .prompts import SQL_EVALUATION_SYSTEM_PROMPT, SQL_EVALUATION_USER_PROMPT


# Schema文件路径
SCHEMA_FILE = Path(__file__).parent.parent / "schema" / "all_tables.json"


class EvaluationResult(BaseModel):
    """SQL评估结果模型"""
    syntax_score: int = Field(ge=1, le=10, description="语法正确性评分")
    semantic_score: int = Field(ge=1, le=10, description="语义准确性评分")
    overall_score: int = Field(ge=1, le=10, description="综合评分")
    is_correct: bool = Field(description="SQL是否正确")
    comments: str = Field(description="评价说明")
    suggestions: list[str] = Field(default_factory=list, description="改进建议")


def extract_table_names(sql: str) -> list[str]:
    """
    从SQL语句中提取表名。
    
    使用正则表达式匹配FROM和JOIN后的表名。
    
    Args:
        sql: SQL语句
        
    Returns:
        表名列表
    """
    tables = set()
    
    # 标准化SQL（去除多余空白）
    sql_normalized = re.sub(r'\s+', ' ', sql.upper())
    
    # 匹配 FROM table_name
    from_pattern = r'FROM\s+([`"]?\w+[`"]?)(?:\s+(?:AS\s+)?(\w+))?'
    for match in re.finditer(from_pattern, sql_normalized, re.IGNORECASE):
        table = match.group(1).strip('`"').lower()
        tables.add(table)
    
    # 匹配 JOIN table_name
    join_pattern = r'JOIN\s+([`"]?\w+[`"]?)(?:\s+(?:AS\s+)?(\w+))?'
    for match in re.finditer(join_pattern, sql_normalized, re.IGNORECASE):
        table = match.group(1).strip('`"').lower()
        tables.add(table)
    
    return list(tables)


def load_schema_data() -> dict[str, dict]:
    """
    加载schema数据并建立表名索引。
    
    Returns:
        表名到DDL的映射字典
    """
    if not SCHEMA_FILE.exists():
        return {}
    
    with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
        schemas = json.load(f)
    
    # 建立表名索引
    return {
        schema["table_name"]: schema
        for schema in schemas
    }


def get_ddl_for_tables(table_names: list[str]) -> str:
    """
    获取指定表的DDL。
    
    Args:
        table_names: 表名列表
        
    Returns:
        DDL字符串
    """
    schema_data = load_schema_data()
    
    ddl_parts = []
    for table in table_names:
        if table in schema_data:
            ddl = schema_data[table].get("full_ddl", "")
            if ddl:
                ddl_parts.append(f"-- 表: {table}\n{ddl}")
        else:
            ddl_parts.append(f"-- 表: {table} (未找到DDL)")
    
    return "\n\n".join(ddl_parts) if ddl_parts else "无相关DDL信息"


class SQLEvaluator:
    """SQL质量评估器"""
    
    def __init__(self, model_name: Optional[str] = None):
        """
        初始化评估器。
        
        Args:
            model_name: LLM模型名称，默认使用settings中配置
        """
        self.llm = create_model(
            model_name="gpt-5.2",
            reasoning_effort="medium",
            temperature=0.0,
        )
        
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", SQL_EVALUATION_SYSTEM_PROMPT),
            ("user", SQL_EVALUATION_USER_PROMPT),
        ])
        
        self.parser = JsonOutputParser(pydantic_object=EvaluationResult)
    
    def evaluate(
        self,
        question: str,
        sql: str,
        ddl: Optional[str] = None,
    ) -> EvaluationResult:
        """
        评估SQL语句质量。
        
        Args:
            question: 用户原始问题
            sql: 生成的SQL语句
            ddl: 相关表DDL（可选，如不提供则自动提取）
            
        Returns:
            评估结果
        """
        # 如果未提供DDL，自动提取
        if ddl is None:
            table_names = extract_table_names(sql)
            ddl = get_ddl_for_tables(table_names)
        
        # 构建评估链
        chain = self.prompt | self.llm | self.parser
        
        # 执行评估
        result = chain.invoke({
            "question": question,
            "sql": sql,
            "ddl": ddl,
        })
        
        return EvaluationResult(**result)
    
    def evaluate_batch(
        self,
        test_cases: list[dict[str, str]],
    ) -> list[dict[str, Any]]:
        """
        批量评估SQL语句。
        
        Args:
            test_cases: 测试用例列表，每个包含 question 和 sql
            
        Returns:
            评估结果列表
        """
        results = []
        
        for i, case in enumerate(test_cases, 1):
            print(f"  评估 {i}/{len(test_cases)}: {case['question'][:40]}...")
            
            try:
                eval_result = self.evaluate(
                    question=case["question"],
                    sql=case["sql"],
                )
                results.append({
                    "question": case["question"],
                    "sql": case["sql"],
                    "tables": extract_table_names(case["sql"]),
                    "evaluation": eval_result.model_dump(),
                    "error": None,
                })
            except Exception as e:
                results.append({
                    "question": case["question"],
                    "sql": case["sql"],
                    "tables": extract_table_names(case["sql"]),
                    "evaluation": None,
                    "error": str(e),
                })
        
        return results


def generate_evaluation_report(results: list[dict]) -> str:
    """
    生成评估报告。
    
    Args:
        results: 评估结果列表
        
    Returns:
        Markdown格式报告
    """
    total = len(results)
    successful = [r for r in results if r["evaluation"] is not None]
    failed = [r for r in results if r["evaluation"] is None]
    
    # 计算平均分
    if successful:
        avg_syntax = sum(r["evaluation"]["syntax_score"] for r in successful) / len(successful)
        avg_semantic = sum(r["evaluation"]["semantic_score"] for r in successful) / len(successful)
        avg_overall = sum(r["evaluation"]["overall_score"] for r in successful) / len(successful)
        correct_count = sum(1 for r in successful if r["evaluation"]["is_correct"])
    else:
        avg_syntax = avg_semantic = avg_overall = 0
        correct_count = 0
    
    report = [
        "# SQL质量评估报告",
        "",
        "## 总体统计",
        "",
        f"| 指标 | 数值 |",
        f"|------|------|",
        f"| 总评估数 | {total} |",
        f"| 成功评估 | {len(successful)} |",
        f"| 评估失败 | {len(failed)} |",
        f"| 正确SQL数 | {correct_count} |",
        f"| 正确率 | {correct_count/len(successful)*100:.1f}% |" if successful else "| 正确率 | N/A |",
        "",
        "## 评分统计",
        "",
        f"| 维度 | 平均分 |",
        f"|------|--------|",
        f"| 语法正确性 | {avg_syntax:.1f} |",
        f"| 语义准确性 | {avg_semantic:.1f} |",
        f"| **综合评分** | **{avg_overall:.1f}** |",
        "",
        "## 详细结果",
        "",
    ]
    
    for i, r in enumerate(results, 1):
        eval_data = r.get("evaluation", {})
        if eval_data:
            status = "✅" if eval_data["is_correct"] else "❌"
            report.append(f"### {i}. {status} {r['question'][:50]}")
            report.append(f"- 综合评分: {eval_data['overall_score']}/10")
            report.append(f"- 评价: {eval_data['comments']}")
            if eval_data.get("suggestions"):
                report.append(f"- 建议: {', '.join(eval_data['suggestions'])}")
        else:
            report.append(f"### {i}. ⚠️ {r['question'][:50]}")
            report.append(f"- 错误: {r.get('error', 'Unknown')}")
        report.append("")
    
    return "\n".join(report)
