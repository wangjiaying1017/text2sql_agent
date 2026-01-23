"""
Long-Term Memory Storage

将对话历史存储到 Qdrant 向量库，实现长期记忆。
基于问题相似度检索相关的历史对话。
"""
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, Any
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
from openai import OpenAI

from config import settings

logger = logging.getLogger("text2sql.memory")

# 长期记忆集合名称
MEMORY_COLLECTION_NAME = "conversation_memory"
EMBEDDING_MODEL = "text-embedding-v4"
EMBEDDING_DIM = 1536


class LongTermMemory:
    """
    长期记忆存储类
    
    将对话历史存储到 Qdrant，支持按问题相似度检索。
    """
    
    _instance: Optional["LongTermMemory"] = None
    _client: Optional[QdrantClient] = None
    _openai: Optional[OpenAI] = None
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # 初始化 Qdrant 客户端
        if LongTermMemory._client is None:
            LongTermMemory._client = QdrantClient(
                host=settings.qdrant_host,
                port=settings.qdrant_port
            )
        self._client = LongTermMemory._client
        
        # 初始化 OpenAI 客户端（用于 embedding）
        if LongTermMemory._openai is None:
            LongTermMemory._openai = OpenAI(
                api_key=settings.qwen_api_key,
                base_url=settings.qwen_base_url
            )
        self._openai = LongTermMemory._openai
        
        # 确保集合存在
        self._ensure_collection()
        
        self._initialized = True
    
    def _ensure_collection(self) -> None:
        """确保长期记忆集合存在"""
        try:
            collections = self._client.get_collections().collections
            exists = any(c.name == MEMORY_COLLECTION_NAME for c in collections)
            
            if not exists:
                logger.info(f"[Memory] Creating collection: {MEMORY_COLLECTION_NAME}")
                self._client.create_collection(
                    collection_name=MEMORY_COLLECTION_NAME,
                    vectors_config=VectorParams(
                        size=EMBEDDING_DIM,
                        distance=Distance.COSINE
                    )
                )
        except Exception as e:
            logger.warning(f"[Memory] Failed to ensure collection: {e}")
    
    def _get_embedding(self, text: str) -> list[float]:
        """生成文本的 embedding 向量"""
        if not text.strip():
            return [0.0] * EMBEDDING_DIM
        
        response = self._openai.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text,
            dimensions=EMBEDDING_DIM
        )
        return response.data[0].embedding
    
    def archive(self, messages: list[BaseMessage], thread_id: str = "default") -> int:
        """
        将消息存档到长期记忆。
        
        将相邻的 Human-AI 消息对提取出来，存储为独立记录。
        
        Args:
            messages: 要存档的消息列表
            thread_id: 会话 ID
            
        Returns:
            存档的记录数
        """
        if not messages:
            return 0
        
        # 提取 Human-AI 消息对
        pairs = self._extract_message_pairs(messages)
        
        if not pairs:
            return 0
        
        points = []
        for question, ai_content in pairs:
            # 解析 AI 回复内容
            try:
                ai_data = json.loads(ai_content)
            except (json.JSONDecodeError, TypeError):
                ai_data = {"raw": ai_content}
            
            # 构建存储记录
            record = {
                "id": str(uuid.uuid4()),
                "thread_id": thread_id,
                "question": question,
                "sql_queries": ai_data.get("sql_queries", []),
                "result_summary": ai_data.get("result_summary", ""),
                "timestamp": datetime.now().isoformat()
            }
            
            # 对问题生成 embedding
            embedding = self._get_embedding(question)
            
            points.append(PointStruct(
                id=record["id"],
                vector=embedding,
                payload=record
            ))
        
        # 批量存储
        if points:
            try:
                self._client.upsert(
                    collection_name=MEMORY_COLLECTION_NAME,
                    points=points
                )
                logger.info(f"[Memory] Archived {len(points)} conversation pairs")
            except Exception as e:
                logger.error(f"[Memory] Failed to archive: {e}")
                return 0
        
        return len(points)
    
    def _extract_message_pairs(self, messages: list[BaseMessage]) -> list[tuple[str, str]]:
        """
        提取 Human-AI 消息对。
        
        Returns:
            [(question, ai_response), ...]
        """
        pairs = []
        i = 0
        
        while i < len(messages):
            if messages[i].type == "human":
                question = messages[i].content
                # 查找对应的 AI 回复
                if i + 1 < len(messages) and messages[i + 1].type == "ai":
                    ai_response = messages[i + 1].content
                    pairs.append((question, ai_response))
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        
        return pairs
    
    def retrieve(
        self,
        query: str,
        thread_id: Optional[str] = None,
        limit: int = 3,
        score_threshold: float = 0.5
    ) -> list[dict[str, Any]]:
        """
        检索相关的历史对话。
        
        Args:
            query: 查询问题
            thread_id: 可选，限制在指定会话内检索
            limit: 返回的最大记录数
            score_threshold: 相似度阈值
            
        Returns:
            相关的历史对话记录列表
        """
        # 生成查询向量
        query_embedding = self._get_embedding(query)
        
        # 构建过滤条件
        search_filter = None
        if thread_id:
            search_filter = Filter(
                must=[
                    FieldCondition(
                        key="thread_id",
                        match=MatchValue(value=thread_id)
                    )
                ]
            )
        
        # 搜索
        try:
            results = self._client.query_points(
                collection_name=MEMORY_COLLECTION_NAME,
                query=query_embedding,
                query_filter=search_filter,
                limit=limit
            ).points
        except AttributeError:
            # 兼容旧版 API
            results = self._client.search(
                collection_name=MEMORY_COLLECTION_NAME,
                query_vector=query_embedding,
                query_filter=search_filter,
                limit=limit
            )
        
        # 过滤低相似度结果
        filtered = []
        for hit in results:
            if hit.score >= score_threshold:
                record = hit.payload
                record["_score"] = hit.score
                filtered.append(record)
        
        logger.debug(f"[Memory] Retrieved {len(filtered)} relevant memories for: {query[:50]}...")
        
        return filtered
    
    def get_stats(self) -> dict[str, Any]:
        """获取长期记忆统计信息"""
        try:
            info = self._client.get_collection(MEMORY_COLLECTION_NAME)
            return {
                "collection": MEMORY_COLLECTION_NAME,
                "points_count": info.points_count,
                "status": str(info.status)
            }
        except Exception as e:
            return {"error": str(e)}
