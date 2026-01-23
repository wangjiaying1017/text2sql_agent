"""
Query Parser - 问题结构化解析器

将用户自然语言问题解析为结构化语义信息，用于判断是否需要澄清。
使用 qwen-flash 模型进行快速解析。
"""
import logging
from typing import Optional, Literal
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate

from llm.client import get_qwen_model

logger = logging.getLogger("text2sql.query_parser")


# ============== 结构化问题模型 ==============

class ParsedQuery(BaseModel):
    """结构化用户问题"""
    
    # === 核心语义 ===
    query_type: Literal["list", "aggregate", "detail", "status", "unknown"] = Field(
        description="查询类型：list=列表查询, aggregate=聚合统计, detail=详情查询, status=状态查询, unknown=无法识别"
    )
    
    object_type: Optional[Literal["device", "customer", "link", "node", "config", None]] = Field(
        default=None,
        description="查询对象类型"
    )
    
    object_identifier: Optional[str] = Field(
        default=None,
        description="对象标识（设备名/客户名/序列号等，原文提取）"
    )
    
    # === 指标相关（仅 aggregate/status 类型需要）===
    metric: Optional[str] = Field(
        default=None,
        description="指标名称（自然语言描述，如'上行流量'、'延迟'）"
    )
    
    aggregation: Optional[Literal["sum", "avg", "max", "min", "count", "latest", None]] = Field(
        default=None,
        description="聚合方式"
    )
    
    # === 时间相关 ===
    time_range: Optional[str] = Field(
        default=None,
        description="时间范围描述（原文提取，如'最近3小时'、'昨天'）"
    )
    
    # === 过滤条件 ===
    filters: list[str] = Field(
        default_factory=list,
        description="其他过滤条件（原文提取）"
    )
    
    # === 澄清相关 ===
    confidence: Literal["high", "medium", "low"] = Field(
        default="high",
        description="解析置信度"
    )
    
    ambiguous_fields: list[str] = Field(
        default_factory=list,
        description="模糊/缺失的字段名"
    )
    
    clarification_question: Optional[str] = Field(
        default=None,
        description="如果需要澄清，建议的澄清问题（每次只问1个最重要的问题）"
    )


# ============== Prompt 模板 ==============

QUERY_PARSER_SYSTEM_PROMPT = """你是一个语义解析专家。将用户的自然语言问题解析为结构化语义信息。

## 核心原则
1. **只做语义理解**：描述"用户想问什么"，不考虑数据库实现
2. **保留原文表述**：object_identifier、time_range 等字段直接提取用户原话
3. **标注不确定性**：无法确定的字段设为 null，并记录到 ambiguous_fields

## 字段说明

### query_type（必填）
- "list": 列表查询（如"有哪些设备"、"客户列表"）
- "aggregate": 聚合统计（如"总流量"、"平均延迟"）
- "detail": 详情查询（如"设备A的配置"、"客户信息"）
- "status": 状态查询（如"在线状态"、"连接状态"）
- "unknown": 无法识别

### object_type
- "device": 设备/边缘节点
- "customer": 客户/组网
- "link": 线路/链路
- "node": NC节点
- "config": 配置信息
- null: 无法判断

### confidence 置信度判断规则
- "high": 问题清晰完整，所有必要信息都有
- "medium": 大部分信息清楚，但有少量可选信息缺失
- "low": 关键信息缺失或模糊，需要澄清

### 澄清规则（⚠️ 重要）
当以下情况出现时，设置 confidence = "low" 并生成 clarification_question：
1. query_type = "aggregate" 但缺少 metric（指标）
2. query_type = "aggregate" 但缺少 time_range（时间范围）
3. object_identifier 不明确（如"那个设备"没有上下文）
4. 问题太短或太模糊无法理解

**渐进式澄清**：clarification_question 每次只问 1 个最重要的问题！

## 示例

用户问题："海底捞组网最近3小时的上行流量"
输出：
{{
  "query_type": "aggregate",
  "object_type": "customer",
  "object_identifier": "海底捞组网",
  "metric": "上行流量",
  "aggregation": null,
  "time_range": "最近3小时",
  "filters": [],
  "confidence": "medium",
  "ambiguous_fields": ["aggregation"],
  "clarification_question": null
}}

用户问题："查流量"
输出：
{{
  "query_type": "aggregate",
  "object_type": null,
  "object_identifier": null,
  "metric": "流量",
  "aggregation": null,
  "time_range": null,
  "filters": [],
  "confidence": "low",
  "ambiguous_fields": ["object_type", "object_identifier", "time_range"],
  "clarification_question": "请问您想查询哪个设备或客户的流量？"
}}

用户问题："设备有哪些"
输出：
{{
  "query_type": "list",
  "object_type": "device",
  "object_identifier": null,
  "metric": null,
  "aggregation": null,
  "time_range": null,
  "filters": [],
  "confidence": "medium",
  "ambiguous_fields": ["object_identifier"],
  "clarification_question": null
}}
"""

QUERY_PARSER_USER_PROMPT = """## 历史对话上下文
{context}

## 用户问题
{question}

请直接输出 JSON，不要附加解释。"""


# ============== QueryParser 类 ==============

class QueryParser:
    """
    问题结构化解析器
    
    使用 qwen-flash 模型将用户问题解析为结构化语义信息。
    """
    
    def __init__(self, model_name: str = "qwen-flash"):
        """
        初始化解析器。
        
        Args:
            model_name: 模型名称，默认使用 qwen-flash（快速且便宜）
        """
        self.model_name = model_name
        self._llm = None
        self._chain = None
    
    def _get_chain(self):
        """懒加载 LLM chain"""
        if self._chain is None:
            self._llm = get_qwen_model(model_name=self.model_name)
            
            # 使用结构化输出
            structured_llm = self._llm.with_structured_output(ParsedQuery)
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", QUERY_PARSER_SYSTEM_PROMPT),
                ("human", QUERY_PARSER_USER_PROMPT),
            ])
            
            self._chain = prompt | structured_llm
        
        return self._chain
    
    def parse(
        self, 
        question: str, 
        context: str = "",
        verbose: bool = False
    ) -> ParsedQuery:
        """
        解析用户问题为结构化语义信息。
        
        Args:
            question: 用户问题
            context: 历史对话上下文
            verbose: 是否打印详细信息
            
        Returns:
            ParsedQuery: 结构化问题
        """
        chain = self._get_chain()
        
        context_str = context if context else "无历史上下文"
        
        if verbose:
            logger.info(f"[QueryParser] 解析问题: {question}")
        
        try:
            result: ParsedQuery = chain.invoke({
                "question": question,
                "context": context_str
            })
            
            if verbose:
                logger.info(f"[QueryParser] 解析结果: {result.model_dump_json(indent=2)}")
            
            return result
            
        except Exception as e:
            logger.error(f"[QueryParser] 解析失败: {e}")
            # 返回一个默认的结果，标记为需要澄清
            return ParsedQuery(
                query_type="unknown",
                confidence="low",
                ambiguous_fields=["query_type"],
                clarification_question="抱歉，我没有理解您的问题，请您重新描述一下？"
            )
    
    def needs_clarification(
        self, 
        parsed: ParsedQuery, 
        clarification_count: int = 0,
        max_clarifications: int = 2
    ) -> bool:
        """
        判断是否需要澄清。
        
        Args:
            parsed: 解析后的问题
            clarification_count: 已澄清次数
            max_clarifications: 最大澄清次数
            
        Returns:
            bool: 是否需要澄清
        """
        # 达到最大澄清次数，不再澄清
        if clarification_count >= max_clarifications:
            logger.info(f"[QueryParser] 已达到最大澄清次数 ({max_clarifications})，跳过澄清")
            return False
        
        # 置信度为低且有澄清问题
        if parsed.confidence == "low" and parsed.clarification_question:
            return True
        
        return False
