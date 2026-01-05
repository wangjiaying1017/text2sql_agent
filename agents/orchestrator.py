"""
Text2SQL Agent 编排器

根据意图识别结果编排查询执行流程，使用混合检索(RAG)获取相关DDL。
"""
import json
from typing import Any, Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from intent import IntentRecognizer, QueryPlan
from database import MySQLConnector, InfluxDBConnector
from retrieval import HybridRetriever
from .sql_generator import SQLGenerator


console = Console()


class Text2SQLOrchestrator:
    """
    Text2SQL工作流编排器
    
    流程：
    1. 意图识别 → 生成执行计划
    2. RAG检索 → 获取每步相关的DDL
    3. SQL生成 → 根据DDL生成SQL
    4. 执行查询 → 返回结果
    """
    
    def __init__(self, use_rag: bool = True, rag_top_k: int = 5, use_keyword_search: bool = True):
        """
        初始化编排器。
        
        Args:
            use_rag: 是否使用RAG混合检索获取Schema（默认True）
            rag_top_k: RAG检索返回的表数量（默认5）
            use_keyword_search: 是否使用ES关键词检索（默认True）
                - True: 混合检索（关键词 + 语义 + RRF融合）
                - False: 仅语义检索（跳过ES关键词检索，速度更快）
        """
        self.intent_recognizer = IntentRecognizer()
        self.sql_generator = SQLGenerator()
        self.mysql_connector = MySQLConnector()
        self.influxdb_connector = InfluxDBConnector()
        
        # RAG配置
        self.use_rag = use_rag
        self.rag_top_k = rag_top_k
        self.use_keyword_search = use_keyword_search
        self._mysql_retriever: Optional[HybridRetriever] = None
        self._influxdb_retriever: Optional[HybridRetriever] = None
    
    def _get_retriever(self, database_type: str) -> HybridRetriever:
        """根据数据库类型获取对应的检索器。"""
        if database_type == "mysql":
            if self._mysql_retriever is None:
                self._mysql_retriever = HybridRetriever(
                    database_type="mysql",
                    use_keyword_search=self.use_keyword_search
                )
            return self._mysql_retriever
        else:
            if self._influxdb_retriever is None:
                self._influxdb_retriever = HybridRetriever(
                    database_type="influxdb",
                    use_keyword_search=self.use_keyword_search
                )
            return self._influxdb_retriever
    
    def run(self, question: str, verbose: bool = True, session_memory=None) -> dict[str, Any]:
        """
        运行完整的Text2SQL工作流。
        
        Args:
            question: 用户自然语言问题
            verbose: 是否打印详细输出
            session_memory: 会话记忆（用于澄清场景）
            
        Returns:
            最终结果字典
        """
        import time
        
        results = {
            "question": question,
            "status": "success",  # success, needs_clarification, error
            "plan": None,
            "steps_results": [],
            "final_result": None,
            "error": None,
            "timing": {},
            # A+C组合相关
            "confidence": None,
            "assumptions": [],
            "warning": None,
            "clarification_questions": [],
        }
        
        total_start = time.time()
        
        try:
            # 步骤1: 意图识别
            if verbose:
                console.print(Panel("🔍 意图识别中...", title="步骤 1"))
            
            # 获取对话上下文
            context = session_memory.get_history() if session_memory else ""
            
            intent_start = time.time()
            plan = self.intent_recognizer.recognize(question, context=context, verbose=verbose)
            intent_time = time.time() - intent_start
            results["timing"]["intent_recognition"] = round(intent_time, 2)
            
            results["plan"] = plan.model_dump()
            results["confidence"] = plan.confidence
            results["assumptions"] = plan.assumptions
            
            if verbose:
                console.print(f"[dim]⏱️  意图识别耗时: {intent_time:.2f}s[/dim]")
                console.print(f"[dim]📊 置信度: {plan.confidence:.2f}[/dim]")
            
            # A+C组合决策逻辑
            if plan.confidence < 0.5 or plan.needs_clarification:
                # 低置信度，需要澄清
                results["status"] = "needs_clarification"
                results["clarification_questions"] = plan.clarification_questions
                
                if verbose:
                    console.print("[yellow]⚠️ 问题不够明确，需要用户补充信息[/yellow]")
                    for q in plan.clarification_questions:
                        console.print(f"[yellow]  ❓ {q}[/yellow]")
                
                # 记录总耗时
                results["timing"]["total"] = round(time.time() - total_start, 2)
                return results
            
            elif plan.confidence < 0.8:
                # 中等置信度，执行但警告
                results["warning"] = "置信度较低，结果可能不够准确"
                if verbose:
                    console.print("[yellow]⚠️ 置信度较低，结果可能不够准确[/yellow]")
                    if plan.assumptions:
                        console.print(f"[dim]系统假设: {', '.join(plan.assumptions)}[/dim]")
            
            else:
                # 高置信度，展示假设
                if verbose and plan.assumptions:
                    console.print(f"[dim]系统假设: {', '.join(plan.assumptions)}[/dim]")
            
            if verbose:
                self._print_plan(plan)
            
            # 步骤2: 逐步执行查询
            context = {}  # 存储每步的结果
            
            for step in plan.steps:
                step_timing = {}
                
                if verbose:
                    console.print(Panel(
                        f"📊 执行步骤 {step.step}: {step.purpose}",
                        title=f"步骤 {step.step + 1}"
                    ))
                
                # 获取依赖步骤的上下文
                step_context = ""
                if step.depends_on is not None and step.depends_on in context:
                    step_context = json.dumps(context[step.depends_on], ensure_ascii=False, indent=2)
                
                # 使用RAG混合检索获取相关Schema
                rag_start = time.time()
                if self.use_rag:
                    # 构建检索查询：问题 + 步骤目的
                    retrieval_query = f"{question} {step.purpose}"
                    schema = self._get_schema_by_rag(
                        query=retrieval_query, 
                        database_type=step.database,
                        verbose=verbose
                    )
                else:
                    # 不使用RAG时直接获取Schema
                    schema = self._get_schema_direct(step.database)
                rag_time = time.time() - rag_start
                step_timing["rag_retrieval"] = round(rag_time, 2)
                
                if verbose:
                    console.print(f"[dim]⏱️  RAG检索耗时: {rag_time:.2f}s[/dim]")
                
                # 生成查询
                gen_start = time.time()
                query = self.sql_generator.generate(
                    question=question,
                    purpose=step.purpose,
                    database_type=step.database,
                    schema=schema,
                    context=step_context,
                )
                gen_time = time.time() - gen_start
                step_timing["sql_generation"] = round(gen_time, 2)
                
                if verbose:
                    console.print(f"[dim]⏱️  SQL生成耗时: {gen_time:.2f}s[/dim]")
                    console.print(f"[cyan]生成的查询:[/cyan]\n{query}\n")
                
                # 执行查询（带重试机制）
                exec_start = time.time()
                max_retries = 2
                last_error = None
                
                for attempt in range(max_retries + 1):
                    try:
                        step_result = self._execute_query(step.database, query)
                        last_error = None
                        break
                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_retries:
                            if verbose:
                                console.print(f"[yellow]⚠️ SQL执行失败 (尝试 {attempt + 1}/{max_retries + 1}): {last_error[:60]}...[/yellow]")
                                console.print(f"[yellow]🔄 重新生成SQL...[/yellow]")
                            
                            # 把错误反馈给LLM重新生成
                            error_context = f"{step_context}\n\n上次生成的SQL执行失败:\nSQL: {query}\n错误: {last_error}\n请修正SQL语句。"
                            query = self.sql_generator.generate(
                                question=question,
                                purpose=step.purpose,
                                database_type=step.database,
                                schema=schema,
                                context=error_context,
                            )
                            
                            if verbose:
                                console.print(f"[cyan]重新生成的查询:[/cyan]\n{query}\n")
                        else:
                            # 最后一次尝试也失败，抛出异常
                            raise Exception(f"SQL执行失败 (已重试{max_retries}次): {last_error}")
                
                exec_time = time.time() - exec_start
                step_timing["sql_execution"] = round(exec_time, 2)
                step_timing["retries"] = attempt  # 记录重试次数
                
                if verbose:
                    console.print(f"[dim]⏱️  SQL执行耗时: {exec_time:.2f}s[/dim]")
                
                context[step.step] = step_result
                
                results["steps_results"].append({
                    "step": step.step,
                    "database": step.database,
                    "purpose": step.purpose,
                    "schema_tables": self._extract_table_names(schema) if step.database == "mysql" else [],
                    "query": query,
                    "result": step_result,
                    "timing": step_timing,
                })
                
                if verbose:
                    self._print_results(step_result)
            
            # 最终结果为最后一步的结果
            if results["steps_results"]:
                results["final_result"] = results["steps_results"][-1]["result"]
            
        except Exception as e:
            results["error"] = str(e)
            if verbose:
                console.print(f"[red]错误: {e}[/red]")
        
        # 总耗时
        total_time = time.time() - total_start
        results["timing"]["total"] = round(total_time, 2)
        
        if verbose:
            console.print(f"\n[bold green]✅ 总耗时: {total_time:.2f}s[/bold green]")
            # 打印耗时汇总
            self._print_timing_summary(results)
        
        return results
    
    def _print_timing_summary(self, results: dict[str, Any]):
        """打印耗时汇总。"""
        timing = results.get("timing", {})
        console.print("\n[dim]─── 耗时统计 ───[/dim]")
        console.print(f"[dim]  意图识别: {timing.get('intent_recognition', 0):.2f}s[/dim]")
        
        for step_result in results.get("steps_results", []):
            step_timing = step_result.get("timing", {})
            step_num = step_result.get("step", 0)
            console.print(f"[dim]  步骤{step_num} RAG检索: {step_timing.get('rag_retrieval', 0):.2f}s[/dim]")
            console.print(f"[dim]  步骤{step_num} SQL生成: {step_timing.get('sql_generation', 0):.2f}s[/dim]")
            console.print(f"[dim]  步骤{step_num} SQL执行: {step_timing.get('sql_execution', 0):.2f}s[/dim]")
        
        console.print(f"[dim]  ─────────────[/dim]")
        console.print(f"[dim]  总计: {timing.get('total', 0):.2f}s[/dim]")
    
    def _get_schema_by_rag(
        self, 
        query: str, 
        database_type: str = "mysql",
        verbose: bool = False,
    ) -> str:
        """
        使用RAG混合检索获取相关DDL。
        
        Args:
            query: 检索查询
            database_type: 数据库类型 ("mysql" 或 "influxdb")
            verbose: 是否打印详细信息
            
        Returns:
            相关表/measurement的DDL字符串
        """
        db_label = "MySQL表" if database_type == "mysql" else "InfluxDB measurement"
        if verbose:
            console.print(f"[dim]🔎 RAG检索({db_label}): {query[:60]}...[/dim]")
        
        # 获取对应数据库类型的检索器
        retriever = self._get_retriever(database_type)
        
        # 执行混合检索（获取详细结果）
        search_result = retriever.search_with_details(query, top_k=self.rag_top_k)
        
        keyword_results = search_result["keyword_results"]
        semantic_results = search_result["semantic_results"]
        results = search_result["fused_results"]
        
        if verbose:
            # 显示ES关键词检索结果
            if keyword_results:
                es_tables = [r["table_name"] for r in keyword_results[:5]]
                console.print(f"[blue]📝 ES关键词检索 ({len(keyword_results)}个): {', '.join(es_tables)}[/blue]")
            else:
                console.print("[yellow]⚠️ ES关键词检索无结果或不可用[/yellow]")
            
            # 显示Qdrant语义检索结果
            if semantic_results:
                qdrant_tables = [r["table_name"] for r in semantic_results[:5]]
                console.print(f"[magenta]📐 Qdrant语义检索 ({len(semantic_results)}个): {', '.join(qdrant_tables)}[/magenta]")
            else:
                console.print("[yellow]⚠️ Qdrant语义检索无结果或不可用[/yellow]")
        
        if not results:
            if verbose:
                console.print("[yellow]⚠️ RAG未检索到相关表，使用全量Schema[/yellow]")
            return self._get_schema_direct("mysql")
        
        if verbose:
            table_names = [r["table_name"] for r in results]
            console.print(f"[green]🔀 RRF融合结果 ({len(results)}个): {', '.join(table_names)}[/green]")
        
        # 构建DDL字符串
        ddl_parts = []
        for r in results:
            ddl = r.get("full_ddl", "")
            if ddl:
                ddl_parts.append(f"-- 表: {r['table_name']}\n{ddl}")
        
        schema_text = "\n\n".join(ddl_parts) if ddl_parts else self._get_schema_direct("mysql")
        
        # 打印DDL内容
        if verbose and ddl_parts:
            console.print("\n[dim]📄 检索到的DDL:[/dim]")
            console.print(Panel(schema_text, title="DDL Schema", border_style="dim"))
        
        return schema_text
    
    def _get_schema_direct(self, database: str) -> str:
        """直接从数据库获取Schema信息（原方法）。"""
        try:
            if database == "mysql":
                with MySQLConnector() as conn:
                    schema = conn.get_schema()
                    return json.dumps(schema, ensure_ascii=False, indent=2)
            else:
                with InfluxDBConnector() as conn:
                    measurements = conn.get_measurements()
                    return f"Measurements: {', '.join(measurements)}"
        except Exception:
            return "Schema不可用"
    
    def _extract_table_names(self, schema: str) -> list[str]:
        """从Schema字符串中提取表名。"""
        tables = []
        for line in schema.split("\n"):
            if line.startswith("-- 表:"):
                table_name = line.replace("-- 表:", "").strip()
                tables.append(table_name)
        return tables
    
    def _execute_query(self, database: str, query: str) -> list[dict[str, Any]]:
        """在指定数据库上执行查询。"""
        if database == "mysql":
            with MySQLConnector() as conn:
                return conn.execute(query)
        else:
            with InfluxDBConnector() as conn:
                return conn.execute(query)
    
    def _print_plan(self, plan: QueryPlan) -> None:
        """打印查询计划。"""
        console.print(f"\n[green]分析:[/green] {plan.analysis}")
        console.print(f"[green]策略:[/green] {plan.strategy}")
        console.print(f"[green]置信度:[/green] {plan.confidence:.2%}")
        
        table = Table(title="执行计划")
        table.add_column("步骤", style="cyan")
        table.add_column("数据库", style="magenta")
        table.add_column("目的", style="green")
        table.add_column("依赖", style="yellow")
        
        for step in plan.steps:
            table.add_row(
                str(step.step),
                step.database,
                step.purpose,
                str(step.depends_on) if step.depends_on else "-",
            )
        
        console.print(table)
        console.print()
    
    def _print_results(self, results: list[dict[str, Any]]) -> None:
        """打印查询结果。"""
        if not results:
            console.print("[yellow]无结果[/yellow]\n")
            return
        
        # 限制显示前10条
        display_results = results[:10]
        
        if display_results:
            table = Table(title=f"查询结果 (共 {len(results)} 条)")
            
            # 添加列
            for key in display_results[0].keys():
                table.add_column(str(key))
            
            # 添加行
            for row in display_results:
                table.add_row(*[str(v) for v in row.values()])
            
            console.print(table)
            
            if len(results) > 10:
                console.print(f"[dim]... 还有 {len(results) - 10} 条结果未显示[/dim]")
        
        console.print()
