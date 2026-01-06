"""
Text2SQL Agent - 主程序入口

基于LangChain的Text2SQL智能查询代理
"""
import sys
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from agents import Text2SQLOrchestrator


console = Console()


def print_banner():
    """打印欢迎横幅。"""
    banner = """
╔════════════════════════════════════════════╗
║         Text2SQL Agent v1.0                ║
║   智能多数据库查询代理 (MySQL + InfluxDB)    ║
╚════════════════════════════════════════════╝
    """
    console.print(Panel(banner, style="bold blue"))
    console.print("[dim]输入自然语言问题，系统将自动识别意图并查询相应数据库[/dim]")
    console.print("[dim]输入 'quit' 或 'exit' 退出程序[/dim]\n")


def main():
    """主函数。"""
    import argparse
    from context.memory import SessionMemory
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Text2SQL Agent - 智能多数据库查询代理")
    parser.add_argument(
        "--no-keyword-search", 
        action="store_true",
        help="关闭ES关键词检索，仅使用语义检索（速度更快）"
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="RAG检索返回的表数量（默认5）"
    )
    args = parser.parse_args()
    
    print_banner()
    
    # 打印配置信息
    if args.no_keyword_search:
        console.print("[yellow]⚙️  已关闭ES关键词检索，仅使用语义检索[/yellow]\n")
    
    orchestrator = Text2SQLOrchestrator(
        use_keyword_search=not args.no_keyword_search,
        rag_top_k=args.top_k
    )
    
    while True:
        try:
            # 获取用户输入
            question = Prompt.ask("\n[bold green]请输入您的问题[/bold green]")
            
            # 检查是否退出
            if question.lower() in ["quit", "exit", "q"]:
                console.print("\n[yellow]再见！👋[/yellow]")
                break
            
            if not question.strip():
                continue
            
            # 运行编排器
            console.print()
            result = orchestrator.run(question, verbose=True)
            
            # 检查错误
            if result.get("error"):
                console.print(f"[red]执行出错: {result['error']}[/red]")
            
        except KeyboardInterrupt:
            console.print("\n[yellow]程序已中断[/yellow]")
            break
        except Exception as e:
            console.print(f"[red]发生错误: {e}[/red]")


if __name__ == "__main__":
    main()
