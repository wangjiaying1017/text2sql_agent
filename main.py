"""
Text2SQL Agent - 主程序入口

基于 LangGraph 的 Text2SQL 智能查询代理（含澄清机制）
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
║         Text2SQL Agent v2.1                ║
║   智能多数据库查询代理 (MySQL + InfluxDB)    ║
║     Powered by LangGraph + 澄清机制         ║
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


def get_next_tasks(graph, config) -> list[str]:
    """获取下一个将要执行的节点名称列表。"""
    try:
        state = graph.get_state(config)
        return list(state.next) if state.next else []
    except Exception:
        return []


def handle_clarification(graph, config, clarification_question: str, original_question: str) -> dict:
    """
    处理澄清流程。
    
    Args:
        graph: LangGraph 工作流
        config: 配置字典
        clarification_question: 澄清问题
        original_question: 用户原始问题
        
    Returns:
        最终状态
    """
    console.print(f"\n[bold yellow]❓ 需要澄清:[/bold yellow] {clarification_question}")
    console.print("[dim]输入 '继续' 或 '跳过' 可强制执行原问题[/dim]")
    
    # 获取用户澄清回答
    answer = Prompt.ask("[bold cyan]请补充信息[/bold cyan]")
    
    # 检查是否强制跳过
    if answer.strip().lower() in ["继续", "跳过", "skip", "continue"]:
        console.print("[yellow]已跳过澄清，将使用原问题继续执行[/yellow]")
        # 更新状态：跳过澄清
        update_state = {
            "skip_clarification": True,
            "clarification_question": None,
        }
    else:
        # 将用户回答追加到原问题
        enhanced_question = f"{original_question}（补充：{answer}）"
        console.print(f"[dim]增强问题: {enhanced_question}[/dim]")
        update_state = {
            "question": enhanced_question,
            "clarification_question": None,
        }
    
    # 继续工作流
    result = graph.invoke(update_state, config)
    return result


def run_query(graph, config, input_state: dict) -> dict:
    """
    运行查询并处理澄清循环。
    
    Args:
        graph: LangGraph 工作流
        config: 配置字典
        input_state: 输入状态
        
    Returns:
        最终状态
    """
    original_question = input_state["question"]
    
    # 首次调用
    result = graph.invoke(input_state, config)
    
    # 检查是否需要澄清（最多循环2次）
    max_clarification_loops = 3  # 安全保护
    loop_count = 0
    
    while loop_count < max_clarification_loops:
        loop_count += 1
        
        # 检查下一个节点
        next_tasks = get_next_tasks(graph, config)
        
        if "wait_clarification" in next_tasks:
            # 需要澄清
            clarification_question = result.get("clarification_question", "请提供更多信息")
            result = handle_clarification(graph, config, clarification_question, original_question)
        elif "human_input" in next_tasks:
            # 正常查询结束，等待下一轮输入
            break
        else:
            # 工作流已结束或其他情况
            break
    
    return result


def main():
    """主函数。"""
    print_banner()
    
    # 构建 LangGraph 工作流
    console.print("🔧 初始化 LangGraph 工作流...")
    graph = build_text2sql_graph()
    console.print("✅ 工作流初始化完成\n")
    
    # 预热连接
    warmup_all(database_types=["mysql"])
    
    # 生成会话 thread_id
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
                thread_id = str(uuid.uuid4())
                console.print(f"[green]对话历史已清除，新会话ID: {thread_id[:8]}...[/green]")
                continue
            
            if not question.strip():
                continue
            
            # 解析预解析参数（模拟主 Agent 传入）
            # 格式: [serial=xxx,client_id=yyy] 问题内容
            # 例如: [serial=abc123,client_id=456] 这个设备的流量
            serial = None
            client_id = None
            actual_question = question
            
            if question.startswith("[") and "]" in question:
                param_end = question.index("]")
                param_str = question[1:param_end]
                actual_question = question[param_end + 1:].strip()
                
                # 解析参数
                for param in param_str.split(","):
                    param = param.strip()
                    if "=" in param:
                        key, value = param.split("=", 1)
                        key = key.strip().lower()
                        value = value.strip()
                        if key == "serial":
                            serial = value
                        elif key == "client_id":
                            client_id = value
                
                console.print(f"[dim]预解析参数: serial={serial}, client_id={client_id}[/dim]")
                console.print(f"[dim]实际问题: {actual_question}[/dim]")
            
            # 准备输入状态
            input_state = {
                "question": actual_question,
                "serial": serial,  # 主 Agent 预解析参数
                "client_id": client_id,  # 主 Agent 预解析参数
                "verbose": True,
                # 每轮重置的字段
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
                # 澄清相关字段重置
                "parsed_query": None,
                "clarification_count": 0,
                "skip_clarification": False,
                "clarification_question": None,
            }
            
            # 运行工作流（含澄清处理）
            console.print()
            config = {"configurable": {"thread_id": thread_id}}
            result = run_query(graph, config, input_state)
            
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
                # status == "success" 或其他
                final_results = result.get("final_results", [])
                if final_results:
                    print_results(final_results)
                    
                    # 尝试绘制折线图（如果有时间序列数据）
                    from utils import plot_line_chart
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
