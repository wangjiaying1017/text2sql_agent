"""
Qdrant Vector Store for MySQL Schema

将MySQL表结构信息存储到Qdrant向量数据库，用于语义检索。
使用OpenAI text-embedding-3-small模型进行embedding。
"""
import json
import re
import sys
from typing import Any, Optional
from pathlib import Path

# 确保能够导入项目模块
sys.path.insert(0, str(Path(__file__).parent.parent))

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from openai import OpenAI

from config import settings


# Qdrant集合名称
QDRANT_COLLECTION_NAME = "mysql_table_schema"

# OpenAI Embedding模型
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536  # text-embedding-3-small 的向量维度

# 外键注释模式: (关联t_xxx.yyy字段)
FK_COMMENT_PATTERN = re.compile(r'\(关联\s*(t_\w+)\.(\w+)\s*字段?\)')

# 列名模式: xxx_id (用于推断外键)
FK_COLUMN_PATTERN = re.compile(r'^(\w+)_id$')


def extract_foreign_keys(columns: list[dict], table_name: str = "") -> list[dict]:
    """
    提取外键关系。
    
    策略：
    1. 优先从 COMMENT 中提取：匹配 (关联t_xxx.yyy字段) 模式
    2. 备选：根据列名模式推断：xxx_id → t_xxx.id
    
    Args:
        columns: 列信息列表
        table_name: 当前表名（用于排除自引用）
        
    Returns:
        外键关系列表
    """
    relationships = []
    seen_columns = set()  # 避免重复
    
    for col in columns:
        col_name = col.get('name', '')
        comment = col.get('comment', '')
        
        # 跳过主键
        if col_name == 'id':
            continue
        
        # 策略1: 从 COMMENT 中提取
        match = FK_COMMENT_PATTERN.search(comment)
        if match:
            target_table = match.group(1)
            target_column = match.group(2)
            clean_comment = FK_COMMENT_PATTERN.sub('', comment).strip()
            
            relationships.append({
                'column': col_name,
                'target_table': target_table,
                'target_column': target_column,
                'comment': clean_comment
            })
            seen_columns.add(col_name)
            continue
        
        # 策略2: 根据列名模式推断
        match = FK_COLUMN_PATTERN.match(col_name)
        if match and col_name not in seen_columns:
            prefix = match.group(1)  # e.g., 'client' from 'client_id'
            target_table = f"t_{prefix}"
            
            # 排除自引用
            if target_table == table_name:
                continue
            
            relationships.append({
                'column': col_name,
                'target_table': target_table,
                'target_column': 'id',
                'comment': comment
            })
    
    return relationships


def build_structured_description(schema: dict) -> str:
    """
    构建结构化的表描述文本，用于 embedding。
    
    格式示例:
    Table: t_edge
    Business Meaning: 边缘节点信息表
    
    Primary Key:
    - id: 数据库主键id
    
    Important Columns:
    - name: 设备名称
    - serial: 设备序列号
    ...
    
    Relationships:
    - t_edge.client_id → t_client.id
    
    Join Hints:
    - JOIN t_client ON t_edge.client_id = t_client.id
    
    Args:
        schema: 表结构信息
        
    Returns:
        结构化描述文本
    """
    table_name = schema.get('table_name', '')
    table_comment = schema.get('table_comment', '')
    columns = schema.get('columns', [])
    
    # 提取主键（通常是第一个列或名为 id 的列）
    primary_key = next((c for c in columns if c['name'] == 'id'), columns[0] if columns else None)
    
    # 提取重要列（前 10 个非外键列）
    important_cols = columns[:10]
    
    # 优先使用 schema 中已有的 relationships，否则从 columns 中提取
    relationships = schema.get('relationships', [])
    if not relationships:
        relationships = extract_foreign_keys(columns, table_name)
    
    # 构建描述文本
    lines = [
        f"Table: {table_name}",
        f"Business Meaning: {table_comment}",
        "",
        "Primary Key:",
    ]
    
    if primary_key:
        pk_comment = FK_COMMENT_PATTERN.sub('', primary_key.get('comment', '')).strip()
        lines.append(f"- {primary_key['name']}: {pk_comment}")
    else:
        lines.append("- (unknown)")
    
    lines.append("")
    lines.append("Important Columns:")
    
    for col in important_cols:
        # 清理注释中的外键信息
        clean_comment = FK_COMMENT_PATTERN.sub('', col.get('comment', '')).strip()
        lines.append(f"- {col['name']}: {clean_comment}")
    
    if relationships:
        lines.append("")
        lines.append("Relationships:")
        for rel in relationships:
            lines.append(f"- {table_name}.{rel['column']} → {rel['target_table']}.{rel['target_column']} ({rel['comment']})")
        
        lines.append("")
        lines.append("Join Hints:")
        for rel in relationships:
            lines.append(f"- JOIN {rel['target_table']} ON {table_name}.{rel['column']} = {rel['target_table']}.{rel['target_column']}")
    
    return "\n".join(lines)


class QdrantStore:
    """
    Qdrant向量存储类，用于MySQL表结构的语义检索。
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        collection_name: str = QDRANT_COLLECTION_NAME,
        embedding_model: str = EMBEDDING_MODEL,
    ):
        """
        初始化Qdrant连接和OpenAI客户端。
        
        Args:
            host: Qdrant主机地址
            port: Qdrant端口
            collection_name: 集合名称
            embedding_model: Embedding模型名称
        """
        self.host = host or settings.qdrant_host
        self.port = port or settings.qdrant_port
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        self._embedding_dim = EMBEDDING_DIM
        
        # 连接Qdrant
        print(f"🔗 连接Qdrant: {self.host}:{self.port}")
        self._client = QdrantClient(host=self.host, port=self.port)
        
        # 初始化OpenAI客户端
        print(f"📥 使用OpenAI Embedding模型: {embedding_model}")
        self._openai = OpenAI(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
        )
        print(f"   向量维度: {self._embedding_dim}")
    
    def create_collection(self, delete_existing: bool = False) -> None:
        """
        创建Qdrant集合。
        
        Args:
            delete_existing: 是否删除已存在的集合
        """
        # 检查集合是否存在
        collections = self._client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)
        
        if exists:
            if delete_existing:
                print(f"🗑️  删除已存在的集合: {self.collection_name}")
                self._client.delete_collection(self.collection_name)
            else:
                print(f"ℹ️  集合已存在: {self.collection_name}")
                return
        
        # 创建集合
        print(f"📦 创建集合: {self.collection_name}")
        self._client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self._embedding_dim,
                distance=Distance.COSINE,
            ),
        )
    
    def _build_text_for_embedding(self, schema: dict[str, Any]) -> str:
        """
        构建用于Embedding的文本。
        
        使用结构化描述代替原始DDL，包含：
        - 表名和业务含义
        - 主键
        - 重要列
        - 外键关系和Join提示
        
        Args:
            schema: 表结构信息
            
        Returns:
            用于Embedding的结构化描述文本
        """
        return build_structured_description(schema)
    
    def _get_embedding(self, text: str) -> list[float]:
        """
        调用OpenAI API生成单个文本的embedding。
        
        Args:
            text: 输入文本
            
        Returns:
            embedding向量
        """
        if not text.strip():
            # 空文本返回零向量
            return [0.0] * self._embedding_dim
        
        response = self._openai.embeddings.create(
            model=self.embedding_model,
            input=text,
        )
        return response.data[0].embedding
    
    def _get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """
        批量生成embedding。
        
        Args:
            texts: 文本列表
            
        Returns:
            embedding向量列表
        """
        # 过滤空文本并记录索引
        non_empty_indices = []
        non_empty_texts = []
        for i, text in enumerate(texts):
            if text.strip():
                non_empty_indices.append(i)
                non_empty_texts.append(text)
        
        # 批量调用API
        embeddings = [[0.0] * self._embedding_dim] * len(texts)
        
        if non_empty_texts:
            # OpenAI API 每次最多2048个输入
            batch_size = 100
            for start in range(0, len(non_empty_texts), batch_size):
                end = min(start + batch_size, len(non_empty_texts))
                batch_texts = non_empty_texts[start:end]
                batch_indices = non_empty_indices[start:end]
                
                print(f"   处理 {start+1}-{end}/{len(non_empty_texts)} ...")
                response = self._openai.embeddings.create(
                    model=self.embedding_model,
                    input=batch_texts,
                )
                
                for j, data in enumerate(response.data):
                    original_idx = batch_indices[j]
                    embeddings[original_idx] = data.embedding
        
        return embeddings
    
    def upsert_schema(self, schema: dict[str, Any], point_id: int) -> None:
        """
        插入或更新单个表的schema。
        
        Args:
            schema: 表结构信息
            point_id: 点ID
        """
        # 构建Embedding文本
        text = self._build_text_for_embedding(schema)
        
        # 生成Embedding
        embedding = self._get_embedding(text)
        
        # 插入到Qdrant
        self._client.upsert(
            collection_name=self.collection_name,
            points=[
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=schema,
                )
            ],
        )
    
    def batch_upsert(self, schemas: list[dict[str, Any]]) -> int:
        """
        批量插入表结构。
        
        Args:
            schemas: 表结构列表
            
        Returns:
            成功插入的数量
        """
        print(f"📊 生成Embedding向量...")
        
        # 为每个 schema 添加结构化描述
        texts = []
        for s in schemas:
            structured_desc = build_structured_description(s)
            s['structured_description'] = structured_desc
            texts.append(structured_desc)
        
        # 批量生成Embedding
        embeddings = self._get_embeddings_batch(texts)
        
        # 构建Points
        points = [
            PointStruct(
                id=i,
                vector=embeddings[i],
                payload=schemas[i],
            )
            for i in range(len(schemas))
        ]
        
        # 批量插入
        print(f"📥 写入Qdrant...")
        self._client.upsert(
            collection_name=self.collection_name,
            points=points,
        )
        
        return len(points)
    
    def search(
        self,
        query: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """
        语义检索表结构。
        
        Args:
            query: 查询文本
            limit: 返回结果数量
            
        Returns:
            匹配的表结构列表
        """
        # 生成查询向量
        query_embedding = self._get_embedding(query)
        
        # 搜索（兼容新旧版本API）
        try:
            # 新版API: query_points
            results = self._client.query_points(
                collection_name=self.collection_name,
                query=query_embedding,
                limit=limit,
            ).points
        except AttributeError:
            # 旧版API: search
            results = self._client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=limit,
            )
        
        # 提取结果
        return [
            {
                **hit.payload,
                "_score": hit.score,
            }
            for hit in results
        ]
    
    def get_collection_info(self) -> dict[str, Any]:
        """获取集合信息。"""
        info = self._client.get_collection(self.collection_name)
        return {
            "name": self.collection_name,
            "points_count": info.points_count,
            "status": str(info.status),
        }


def import_from_json(json_file: str, delete_existing: bool = False) -> None:
    """
    从JSON文件导入表结构到Qdrant。
    
    Args:
        json_file: JSON文件路径
        delete_existing: 是否删除已存在的集合
    """
    with open(json_file, "r", encoding="utf-8") as f:
        schemas = json.load(f)
    
    if not isinstance(schemas, list):
        schemas = [schemas]
    
    store = QdrantStore()
    store.create_collection(delete_existing=delete_existing)
    
    print(f"📥 导入 {len(schemas)} 个表结构到Qdrant...")
    count = store.batch_upsert(schemas)
    print(f"✅ 成功导入 {count} 个表结构")
    
    # 显示集合信息
    info = store.get_collection_info()
    print(f"📊 集合信息: {info}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="将MySQL表结构导入Qdrant向量数据库")
    parser.add_argument(
        "-f", "--file",
        default="schema/all_tables.json",
        help="JSON文件路径 (默认: schema/all_tables.json)"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="删除已存在的集合后重新创建"
    )
    parser.add_argument(
        "-s", "--search",
        help="搜索关键词（测试用）"
    )
    
    args = parser.parse_args()
    
    if args.search:
        # 搜索模式
        store = QdrantStore()
        results = store.search(args.search)
        print(f"\n🔍 语义搜索: {args.search}")
        print(f"📊 找到 {len(results)} 个结果:\n")
        for r in results:
            print(f"  [{r['_score']:.4f}] {r['table_name']}: {r.get('table_comment', '')}")
    else:
        # 导入模式
        import_from_json(args.file, args.delete)


if __name__ == "__main__":
    main()
