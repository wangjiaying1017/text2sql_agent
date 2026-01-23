"""
Text2SQL Agent Web API

使用 FastAPI 提供 Web API 和前端界面。
"""
import uuid
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from graph import build_text2sql_graph

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger("text2sql.api")

# 全局 Graph 实例
graph = None
sessions = {}  # 存储会话信息


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动/关闭时的处理"""
    global graph
    logger.info("初始化 LangGraph 工作流...")
    graph = build_text2sql_graph()
    logger.info("工作流初始化完成")
    yield
    logger.info("应用关闭")


app = FastAPI(
    title="Text2SQL Agent API",
    description="智能多数据库查询代理",
    version="2.1",
    lifespan=lifespan
)


# ============== 数据模型 ==============

class QueryRequest(BaseModel):
    question: str
    serial: Optional[str] = None
    client_id: Optional[str] = None
    session_id: Optional[str] = None


class QueryResponse(BaseModel):
    status: str
    results: list = []
    result_count: int = 0
    sql_queries: list = []
    clarification_question: Optional[str] = None
    error: Optional[str] = None
    session_id: str
    timing: dict = {}


# ============== API 路由 ==============

@app.get("/", response_class=HTMLResponse)
async def index():
    """返回前端页面"""
    return HTML_PAGE


@app.post("/api/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """执行查询"""
    global graph
    
    if not graph:
        raise HTTPException(status_code=500, detail="工作流未初始化")
    
    # 获取或创建会话
    session_id = request.session_id or str(uuid.uuid4())
    config = {"configurable": {"thread_id": session_id}}
    
    # 构建输入状态
    input_state = {
        "question": request.question,
        "serial": request.serial if request.serial else None,
        "client_id": request.client_id if request.client_id else None,
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
    
    logger.info(f"收到查询: {request.question}")
    if request.serial:
        logger.info(f"  serial: {request.serial}")
    if request.client_id:
        logger.info(f"  client_id: {request.client_id}")
    
    try:
        # 执行查询
        result = graph.invoke(input_state, config)
        
        # 检查是否需要澄清
        next_tasks = []
        try:
            state = graph.get_state(config)
            next_tasks = list(state.next) if state.next else []
        except Exception:
            pass
        
        # 提取 SQL 查询
        sql_queries = []
        for step_result in result.get("step_results", []):
            if step_result.get("query"):
                sql_queries.append({
                    "database": step_result.get("database"),
                    "query": step_result.get("query")
                })
        
        return QueryResponse(
            status=result.get("status", "unknown"),
            results=result.get("final_results", [])[:50],  # 限制返回数量
            result_count=len(result.get("final_results", [])),
            sql_queries=sql_queries,
            clarification_question=result.get("clarification_question") if "wait_clarification" in next_tasks else None,
            error=result.get("error"),
            session_id=session_id,
            timing=result.get("timing", {})
        )
        
    except Exception as e:
        logger.error(f"查询失败: {e}")
        return QueryResponse(
            status="error",
            error=str(e),
            session_id=session_id
        )


@app.post("/api/clarify")
async def clarify(request: QueryRequest):
    """处理澄清回答"""
    global graph
    
    if not request.session_id:
        raise HTTPException(status_code=400, detail="需要 session_id")
    
    config = {"configurable": {"thread_id": request.session_id}}
    
    # 检查是否跳过澄清
    if request.question.strip().lower() in ["继续", "跳过", "skip", "continue"]:
        update_state = {
            "skip_clarification": True,
            "clarification_question": None,
        }
    else:
        update_state = {
            "question": request.question,
            "clarification_question": None,
        }
    
    result = graph.invoke(update_state, config)
    
    return QueryResponse(
        status=result.get("status", "unknown"),
        results=result.get("final_results", [])[:50],
        result_count=len(result.get("final_results", [])),
        sql_queries=[],
        clarification_question=result.get("clarification_question"),
        error=result.get("error"),
        session_id=request.session_id,
        timing=result.get("timing", {})
    )


# ============== 前端页面 ==============

HTML_PAGE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Text2SQL Agent</title>
    <style>
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --bg: #0f172a;
            --bg-card: #1e293b;
            --text: #f1f5f9;
            --text-dim: #94a3b8;
            --border: #334155;
            --success: #22c55e;
            --warning: #f59e0b;
            --error: #ef4444;
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
            padding: 2rem;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        h1 {
            text-align: center;
            margin-bottom: 2rem;
            background: linear-gradient(135deg, var(--primary), #a855f7);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .card {
            background: var(--bg-card);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            border: 1px solid var(--border);
        }
        
        .form-group {
            margin-bottom: 1rem;
        }
        
        label {
            display: block;
            margin-bottom: 0.5rem;
            color: var(--text-dim);
            font-size: 0.875rem;
        }
        
        input, textarea {
            width: 100%;
            padding: 0.75rem 1rem;
            border: 1px solid var(--border);
            border-radius: 8px;
            background: var(--bg);
            color: var(--text);
            font-size: 1rem;
            transition: border-color 0.2s;
        }
        
        input:focus, textarea:focus {
            outline: none;
            border-color: var(--primary);
        }
        
        .params-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
        }
        
        button {
            background: var(--primary);
            color: white;
            border: none;
            padding: 0.875rem 2rem;
            border-radius: 8px;
            font-size: 1rem;
            cursor: pointer;
            transition: background 0.2s;
            width: 100%;
        }
        
        button:hover {
            background: var(--primary-dark);
        }
        
        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        
        .status {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 500;
        }
        
        .status-success { background: var(--success); }
        .status-error { background: var(--error); }
        .status-warning { background: var(--warning); color: #000; }
        .status-running { background: var(--primary); }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
            font-size: 0.875rem;
        }
        
        .results-table th,
        .results-table td {
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }
        
        .results-table th {
            background: var(--bg);
            color: var(--primary);
            font-weight: 600;
        }
        
        .results-table tr:hover {
            background: rgba(99, 102, 241, 0.1);
        }
        
        .sql-box {
            background: var(--bg);
            padding: 1rem;
            border-radius: 8px;
            font-family: 'Consolas', monospace;
            font-size: 0.875rem;
            overflow-x: auto;
            margin-top: 0.5rem;
        }
        
        .clarification {
            background: rgba(245, 158, 11, 0.1);
            border: 1px solid var(--warning);
            border-radius: 8px;
            padding: 1rem;
            margin-top: 1rem;
        }
        
        .hidden { display: none; }
        
        .loading {
            text-align: center;
            padding: 2rem;
        }
        
        .loading::after {
            content: '';
            display: inline-block;
            width: 24px;
            height: 24px;
            border: 3px solid var(--border);
            border-top-color: var(--primary);
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        .timing {
            display: flex;
            gap: 1rem;
            flex-wrap: wrap;
            margin-top: 1rem;
            font-size: 0.875rem;
            color: var(--text-dim);
        }
        
        .timing span {
            background: var(--bg);
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 Text2SQL Agent</h1>
        
        <div class="card">
            <h3 style="margin-bottom: 1rem;">📝 查询输入</h3>
            
            <div class="params-grid">
                <div class="form-group">
                    <label for="serial">设备序列号 (Serial)</label>
                    <input type="text" id="serial" placeholder="可选，如: ee38312e085b1a...">
                </div>
                <div class="form-group">
                    <label for="client_id">客户 ID (Client ID)</label>
                    <input type="text" id="client_id" placeholder="可选，如: 074864910636...">
                </div>
            </div>
            
            <div class="form-group">
                <label for="question">问题</label>
                <textarea id="question" rows="3" placeholder="输入您的自然语言问题，如：这个设备最近3小时的上行流量"></textarea>
            </div>
            
            <button id="submitBtn" onclick="submitQuery()">🔍 查询</button>
        </div>
        
        <div id="loadingCard" class="card hidden">
            <div class="loading">正在查询...</div>
        </div>
        
        <div id="clarificationCard" class="card hidden">
            <div class="clarification">
                <h4>❓ 需要澄清</h4>
                <p id="clarificationQuestion" style="margin: 0.5rem 0;"></p>
                <div class="form-group">
                    <input type="text" id="clarifyAnswer" placeholder="输入补充信息，或输入 '继续' 跳过">
                </div>
                <button onclick="submitClarification()">提交</button>
            </div>
        </div>
        
        <div id="resultCard" class="card hidden">
            <h3>
                📊 查询结果 
                <span id="statusBadge" class="status"></span>
                <span id="resultCount" style="color: var(--text-dim); font-size: 0.875rem; margin-left: 0.5rem;"></span>
            </h3>
            
            <div id="errorBox" class="hidden" style="color: var(--error); margin-top: 1rem;"></div>
            
            <div id="sqlSection" class="hidden">
                <h4 style="margin-top: 1rem; color: var(--text-dim);">生成的 SQL</h4>
                <div id="sqlQueries"></div>
            </div>
            
            <div id="tableSection" class="hidden">
                <table class="results-table">
                    <thead id="tableHead"></thead>
                    <tbody id="tableBody"></tbody>
                </table>
            </div>
            
            <div id="timingSection" class="timing hidden"></div>
        </div>
    </div>
    
    <script>
        let currentSessionId = null;
        
        async function submitQuery() {
            const question = document.getElementById('question').value.trim();
            if (!question) return;
            
            const serial = document.getElementById('serial').value.trim() || null;
            const client_id = document.getElementById('client_id').value.trim() || null;
            
            showLoading(true);
            hideResults();
            
            try {
                const response = await fetch('/api/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question, serial, client_id })
                });
                
                const data = await response.json();
                currentSessionId = data.session_id;
                
                if (data.clarification_question) {
                    showClarification(data.clarification_question);
                } else {
                    showResults(data);
                }
            } catch (error) {
                showError(error.message);
            } finally {
                showLoading(false);
            }
        }
        
        async function submitClarification() {
            const answer = document.getElementById('clarifyAnswer').value.trim();
            if (!answer) return;
            
            showLoading(true);
            document.getElementById('clarificationCard').classList.add('hidden');
            
            try {
                const response = await fetch('/api/clarify', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ 
                        question: answer, 
                        session_id: currentSessionId 
                    })
                });
                
                const data = await response.json();
                
                if (data.clarification_question) {
                    showClarification(data.clarification_question);
                } else {
                    showResults(data);
                }
            } catch (error) {
                showError(error.message);
            } finally {
                showLoading(false);
            }
        }
        
        function showLoading(show) {
            document.getElementById('loadingCard').classList.toggle('hidden', !show);
            document.getElementById('submitBtn').disabled = show;
        }
        
        function hideResults() {
            document.getElementById('resultCard').classList.add('hidden');
            document.getElementById('clarificationCard').classList.add('hidden');
        }
        
        function showClarification(question) {
            document.getElementById('clarificationQuestion').textContent = question;
            document.getElementById('clarifyAnswer').value = '';
            document.getElementById('clarificationCard').classList.remove('hidden');
        }
        
        function showResults(data) {
            const card = document.getElementById('resultCard');
            card.classList.remove('hidden');
            
            // 状态徽章
            const badge = document.getElementById('statusBadge');
            badge.textContent = data.status;
            badge.className = 'status status-' + data.status;
            
            // 结果数量
            document.getElementById('resultCount').textContent = 
                data.result_count > 0 ? `(${data.result_count} 条记录)` : '';
            
            // 错误信息
            const errorBox = document.getElementById('errorBox');
            if (data.error) {
                errorBox.textContent = data.error;
                errorBox.classList.remove('hidden');
            } else {
                errorBox.classList.add('hidden');
            }
            
            // SQL 查询
            const sqlSection = document.getElementById('sqlSection');
            const sqlQueries = document.getElementById('sqlQueries');
            if (data.sql_queries && data.sql_queries.length > 0) {
                sqlQueries.innerHTML = data.sql_queries.map(sq => 
                    `<div class="sql-box"><strong>[${sq.database}]</strong><br>${sq.query}</div>`
                ).join('');
                sqlSection.classList.remove('hidden');
            } else {
                sqlSection.classList.add('hidden');
            }
            
            // 结果表格
            const tableSection = document.getElementById('tableSection');
            if (data.results && data.results.length > 0) {
                const columns = Object.keys(data.results[0]);
                
                document.getElementById('tableHead').innerHTML = 
                    '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
                
                document.getElementById('tableBody').innerHTML = 
                    data.results.slice(0, 20).map(row => 
                        '<tr>' + columns.map(c => `<td>${row[c] ?? ''}</td>`).join('') + '</tr>'
                    ).join('');
                
                tableSection.classList.remove('hidden');
            } else {
                tableSection.classList.add('hidden');
            }
            
            // 耗时统计
            const timingSection = document.getElementById('timingSection');
            if (data.timing && Object.keys(data.timing).length > 0) {
                timingSection.innerHTML = Object.entries(data.timing)
                    .map(([k, v]) => `<span>${k}: ${v.toFixed(2)}s</span>`)
                    .join('');
                timingSection.classList.remove('hidden');
            } else {
                timingSection.classList.add('hidden');
            }
        }
        
        function showError(message) {
            const card = document.getElementById('resultCard');
            card.classList.remove('hidden');
            document.getElementById('statusBadge').textContent = 'error';
            document.getElementById('statusBadge').className = 'status status-error';
            document.getElementById('errorBox').textContent = message;
            document.getElementById('errorBox').classList.remove('hidden');
        }
        
        // 回车提交
        document.getElementById('question').addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                submitQuery();
            }
        });
        
        document.getElementById('clarifyAnswer').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                submitClarification();
            }
        });
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    print("\n🚀 启动 Text2SQL Agent Web 服务...")
    print("📌 访问地址: http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
