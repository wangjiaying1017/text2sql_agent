"""
意图识别器

使用LLM分析用户问题，自动生成查询计划。
使用 Qwen 模型的结构化输出功能（with_structured_output）。
"""
from typing import Optional, Literal
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate

from llm.client import get_qwen_model
from .prompts import INTENT_RECOGNITION_SYSTEM_PROMPT, INTENT_RECOGNITION_USER_PROMPT


class QueryStep(BaseModel):
    """查询计划中的单个步骤。"""
    step: int = Field(description="步骤编号")
    database: Literal["mysql", "influxdb"] = Field(description="目标数据库")
    purpose: str = Field(description="该步骤的目的")
    depends_on: Optional[int] = Field(default=None, description="依赖的步骤编号")


class QueryPlan(BaseModel):
    """LLM生成的查询计划。"""
    analysis: str = Field(description="对用户问题的分析")
    strategy: str = Field(description="查询策略描述")
    
    # 置信度评估
    confidence: float = Field(ge=0, le=1, description="对问题理解的置信度(0-1)")
    assumptions: list[str] = Field(default_factory=list, description="LLM做的假设")
    
    # 澄清机制
    needs_clarification: bool = Field(default=False, description="是否需要用户澄清")
    clarification_questions: list[str] = Field(default_factory=list, description="需要用户回答的澄清问题")
    
    # 执行步骤
    steps: list[QueryStep] = Field(description="执行步骤")


class IntentRecognizer:
    """
    意图识别器
    
    根据用户问题和数据库描述，让LLM自主决策查询策略并生成执行计划。
    使用 Qwen 模型的结构化输出功能（with_structured_output）。
    支持动态 RAG 检索相关 MySQL 表信息。
    """
    
    def __init__(self, model_name: str = None, rag_top_k: int = 5):
        """
        初始化意图识别器。
        
        Args:
            model_name: Qwen 模型名称，默认使用 settings.qwen_model
            rag_top_k: 语义检索返回的表数量（默认5）
        """
        # 获取 Qwen 模型并绑定结构化输出
        base_llm = get_qwen_model(model_name=model_name, temperature=0)
        self.llm = base_llm.with_structured_output(QueryPlan)
        
        # RAG 配置
        self.rag_top_k = rag_top_k
        self._qdrant_store = None
        
        # 构建提示词模板
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", INTENT_RECOGNITION_SYSTEM_PROMPT),
            ("human", INTENT_RECOGNITION_USER_PROMPT),
        ])
    
    def _get_qdrant_store(self, db_type: str = "mysql"):
        """
        懒加载 Qdrant 存储实例。
        
        Args:
            db_type: 数据库类型 ("mysql" 或 "influxdb")
        """
        if db_type == "mysql":
            if self._qdrant_store is None:
                from scripts.import_to_qdrant import QdrantStore
                self._qdrant_store = QdrantStore(collection_name="mysql_table_schema")
            return self._qdrant_store
        else:
            # InfluxDB 使用单独的 collection
            if not hasattr(self, '_influxdb_qdrant_store') or self._influxdb_qdrant_store is None:
                from scripts.import_to_qdrant import QdrantStore
                self._influxdb_qdrant_store = QdrantStore(collection_name="influxdb_measurement_schema")
            return self._influxdb_qdrant_store
    
    def _retrieve_relevant_tables(self, question: str, db_type: str = "mysql") -> list[dict]:
        """
        使用语义检索获取与问题相关的表/measurement 信息。
        
        Args:
            question: 用户问题
            db_type: 数据库类型 ("mysql" 或 "influxdb")
            
        Returns:
            相关表/measurement 信息列表
        """
        try:
            qdrant_store = self._get_qdrant_store(db_type)
            results = qdrant_store.search(question, limit=self.rag_top_k)
            return results
        except Exception as e:
            print(f"⚠️ 意图识别 RAG 检索失败 ({db_type}): {e}")
            return []
    
    def _format_table_info(self, tables: list[dict]) -> str:
        """
        格式化表信息为文本描述。
        
        优先使用 Qdrant 中的 structured_description（包含 Relationships 和 Join Hints）。
        
        Args:
            tables: 表信息列表
            
        Returns:
            格式化的表描述文本
        """
        if not tables:
            return "（暂无相关表信息）"
        
        lines = []
        for t in tables:
            table_name = t.get("table_name", "")
            
            # 优先使用 structured_description（与 SQL 生成保持一致）
            structured = t.get("structured_description", "")
            if structured:
                lines.append(f"-- 表: {table_name}\n{structured}")
            else:
                # 兜底：使用简短格式
                table_comment = t.get("table_comment", "")
                columns = t.get("columns", [])
                key_fields = []
                for col in columns[:8]:
                    col_name = col.get("name", "")
                    col_comment = col.get("comment", "")
                    if col_comment:
                        key_fields.append(f"{col_name}({col_comment})")
                    else:
                        key_fields.append(col_name)
                
                line = f"- **{table_name}**: {table_comment}"
                if key_fields:
                    line += f"\n  关键字段: {', '.join(key_fields)}"
                lines.append(line)
        
        return "\n\n".join(lines)
    
    def _format_influxdb_info(self, measurements: list[dict]) -> str:
        """
        格式化 InfluxDB measurement 信息为简洁的文本描述。
        
        Args:
            measurements: measurement 信息列表
            
        Returns:
            格式化的 measurement 描述文本
        """
        if not measurements:
            return "（暂无相关 InfluxDB 表信息）"
        
        lines = []
        for m in measurements:
            name = m.get("measurement_name", "")
            description = m.get("measurement_description", m.get("table_comment", ""))
            
            # 提取 tags 和 fields 信息
            tags = m.get("tags", {})
            fields = m.get("fields", {})
            
            line = f"- **{name}**: {description}"
            
            if tags:
                tag_names = list(tags.keys())[:5]  # 最多显示 5 个 tag
                line += f"\n  Tags: {', '.join(tag_names)}"
            
            if fields:
                field_names = list(fields.keys())[:5]  # 最多显示 5 个 field
                line += f"\n  Fields: {', '.join(field_names)}"
            
            lines.append(line)
        
        return "\n".join(lines)
    
    def recognize(self, question: str, context: str = "", verbose: bool = False) -> QueryPlan:
        """
        识别意图并生成查询计划。
        
        Args:
            question: 用户自然语言问题
            context: 对话上下文（用于澄清场景）
            verbose: 是否打印完整 prompt
            
        Returns:
            QueryPlan: 包含策略和步骤的查询计划
        """
        from concurrent.futures import ThreadPoolExecutor
        
        # 1. 并行执行 MySQL 和 InfluxDB 的 RAG 检索
        with ThreadPoolExecutor(max_workers=2) as executor:
            mysql_future = executor.submit(self._retrieve_relevant_tables, question, "mysql")
            influxdb_future = executor.submit(self._retrieve_relevant_tables, question, "influxdb")
            
            relevant_tables = mysql_future.result()
            relevant_measurements = influxdb_future.result()
        
        mysql_tables_info = self._format_table_info(relevant_tables)
        influxdb_info = self._format_influxdb_info(relevant_measurements)
        
        # 3. 创建处理链（使用结构化输出，无需额外解析器）
        chain = self.prompt | self.llm
        
        prompt_inputs = {
            "question": question,
            "context": context if context else "无历史上下文",
            "mysql_relevant_tables": mysql_tables_info,
            "influxdb_relevant_measurements": influxdb_info,
        }
        
        # 打印完整 prompt（用于调试）
        if verbose:
            # 手动格式化 prompt 用于显示
            formatted_prompt = self.prompt.format(**prompt_inputs)
            print("\n" + "="*60)
            print("📝 Intent Recognition Prompt:")
            print("="*60)
            print(formatted_prompt)
            print("="*60 + "\n")
        
        # 4. 执行处理链，直接返回 QueryPlan 对象
        result = chain.invoke(prompt_inputs)
        
        # 规范化 database 字段（容错处理）
        for step in result.steps:
            step.database = step.database.lower()
        
        return result
    
    async def arecognize(self, question: str, context: str = "") -> QueryPlan:
        """
        异步版本的意图识别。
        
        Args:
            question: 用户自然语言问题
            context: 对话上下文（用于澄清场景）
            
        Returns:
            QueryPlan: 包含策略和步骤的查询计划
        """
        # 1. 使用语义检索获取相关 MySQL 表信息
        relevant_tables = self._retrieve_relevant_tables(question)
        mysql_tables_info = self._format_table_info(relevant_tables)
        
        # 2. 创建处理链
        chain = self.prompt | self.llm
        
        # 3. 异步执行处理链
        result = await chain.ainvoke({
            "question": question,
            "context": context if context else "无历史上下文",
            "mysql_relevant_tables": mysql_tables_info,
        })
        
        # 规范化 database 字段
        for step in result.steps:
            step.database = step.database.lower()
        
        return result
