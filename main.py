"""
Text2SQL Agent - 主程序入口

基于 LangGraph 的 Text2SQL 智能查询代理
"""
import sys
import uuid
import logging
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from graph import build_text2sql_graph
from graph.nodes import warmup_all


# 配置日志（生产环境使用 INFO 级别，调试时可改为 DEBUG）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
# 降低第三方库日志级别
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

console = Console()


def print_banner():
    """打印欢迎横幅。"""
    banner = """
╔════════════════════════════════════════════╗
║         Text2SQL Agent v2.0                ║
║   智能多数据库查询代理 (MySQL + InfluxDB)    ║
║          Powered by LangGraph              ║
╚════════════════════════════════════════════╝
    """
    console.print(Panel(banner, style="bold blue"))
    console.print("[dim]输入自然语言问题，系统将自动识别意图并查询相应数据库[/dim]")
    console.print("[dim]输入 'quit' 或 'exit' 退出程序[/dim]")
    console.print("[dim]输入 'clear' 清除对话历史[/dim]\n")


def print_results(results: list[dict], title: str = "查询结果"):
    """打印查询结果表格（自动转换时区）。"""
    from utils import convert_timezone
    
    if not results:
        console.print("[yellow]无查询结果[/yellow]")
        return
    
    # 自动转换时区（UTC → 北京时间）
    results = convert_timezone(results)
    
    # 获取所有列名
    columns = list(results[0].keys()) if results else []
    
    table = Table(title=title, show_header=True, header_style="bold magenta")
    for col in columns:
        table.add_column(col)
    
    for row in results[:20]:  # 最多显示20行
        table.add_row(*[str(row.get(col, "")) for col in columns])
    
    if len(results) > 20:
        console.print(f"[dim]... 共 {len(results)} 行，只显示前 20 行[/dim]")
    
    console.print(table)


def print_timing(timing: dict[str, float]):
    """打印耗时信息。"""
    console.print("\n[bold]⏱️ 耗时统计:[/bold]")
    for name, duration in timing.items():
        console.print(f"  {name}: {duration:.2f}s")


def main():
    """主函数。"""
    print_banner()
    
    # 构建 LangGraph 工作流（带 InMemorySaver）
    console.print("🔧 初始化 LangGraph 工作流...")
    graph = build_text2sql_graph()
    console.print("✅ 工作流初始化完成\n")
    
    # 预热连接
    warmup_all(database_types=["mysql"])
    
    # 🆕 生成会话 thread_id（用于 InMemorySaver 区分会话）
    thread_id = str(uuid.uuid4())
    console.print(f"[dim]会话ID: {thread_id[:8]}...[/dim]\n")
    
    while True:
        try:
            # 获取用户输入
            question = Prompt.ask("\n[bold green]请输入您的问题[/bold green]")
            
            # 检查特殊命令
            if question.lower() in ["quit", "exit", "q"]:
                console.print("\n[yellow]再见！👋[/yellow]")
                break
            
            if question.lower() == "clear":
                # 🆕 生成新的 thread_id 来重置会话
                thread_id = str(uuid.uuid4())
                console.print(f"[green]对话历史已清除，新会话ID: {thread_id[:8]}...[/green]")
                continue
            
            if not question.strip():
                continue
            
            # 准备输入状态
            # 只传入本轮需要更新的字段，其他字段（如 messages）由 MemorySaver 从历史恢复
            input_state = {
                "question": question,
                "verbose": True,
                # 以下字段每轮重置
                "status": "running",  # 初始状态
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
                # 🆕 messages 不传入，由 MemorySaver 自动恢复历史
            }
            
            # 🆕 运行工作流（使用 thread_id 区分会话）
            console.print()
            config = {"configurable": {"thread_id": thread_id}}
            result = graph.invoke(input_state, config)
            
            # 根据 status 显示结果
            status = result.get("status", "error")
            
            if status == "error":
                console.print(f"[red]❌ 执行出错: {result.get('error', '未知错误')}[/red]")
            elif status == "no_result":
                error_msg = result.get("error", "")
                if error_msg:
                    console.print(f"[yellow]⚠️ 查询无结果: {error_msg}[/yellow]")
                else:
                    console.print("[yellow]⚠️ 查询无结果，请检查查询条件是否正确[/yellow]")
            else:
                # status == "success"
                final_results = result.get("final_results", [])
                print_results(final_results)
                
                # 尝试绘制折线图（如果有时间序列数据）
                from utils import plot_line_chart
                if final_results:
                    plot_line_chart(final_results, title=input_state["question"])
            
            # 打印耗时
            if result.get("timing"):
                print_timing(result["timing"])
            
        except KeyboardInterrupt:
            console.print("\n[yellow]程序已中断[/yellow]")
            break
        except Exception as e:
            console.print(f"[red]发生错误: {e}[/red]")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()

