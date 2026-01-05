"""
Elasticsearch Schema Store

将MySQL表结构信息存储到Elasticsearch，用于关键词检索。
"""
import json
from typing import Any, Optional
from pathlib import Path
from elasticsearch import Elasticsearch

from config import settings


# ES索引名称
ES_INDEX_NAME = "mysql_table_schema"

# ES Mapping配置
ES_MAPPING = {
    "properties": {
        "table_name": {"type": "text", "analyzer": "ik_max_word"},
        "table_comment": {"type": "text", "analyzer": "ik_max_word"},
        "column_names_str": {"type": "text", "analyzer": "ik_max_word"},
        "column_comments_str": {"type": "text", "analyzer": "ik_max_word"},
        "full_ddl": {"type": "keyword"},  # 原始DDL，不分词
        "columns": {"type": "object", "enabled": False},  # 不索引columns对象
    }
}


class ElasticsearchStore:
    """
    Elasticsearch存储类，用于管理MySQL表结构的ES索引。
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        index_name: str = ES_INDEX_NAME,
    ):
        """
        初始化Elasticsearch连接。
        
        Args:
            host: ES主机地址
            port: ES端口
            username: ES用户名
            password: ES密码
            index_name: 索引名称
        """
        self.host = host or settings.es_host
        self.port = port or settings.es_port
        self.username = (username or settings.es_user).strip()
        self.password = (password or settings.es_password).strip()
        self.index_name = index_name
        
        # 构建连接URL
        es_url = f"http://{self.host}:{self.port}"
        
        # 创建ES客户端
        if self.username and self.password:
            self._client = Elasticsearch(
                hosts=[es_url],
                basic_auth=(self.username, self.password),
            )
        else:
            self._client = Elasticsearch(hosts=[es_url])
    
    def create_index(self, delete_existing: bool = False) -> None:
        """
        创建ES索引。
        
        Args:
            delete_existing: 是否删除已存在的索引
        """
        # 删除已存在的索引
        if delete_existing:
            print(f"🗑️  删除已存在的索引: {self.index_name}")
            try:
                self._client.indices.delete(index=self.index_name)
            except Exception:
                pass  # 索引不存在时忽略错误
        
        # 检查索引是否存在
        try:
            exists = self._client.indices.exists(index=self.index_name).body
        except Exception:
            # 兼容不同版本的ES客户端
            try:
                self._client.indices.get(index=self.index_name)
                exists = True
            except Exception:
                exists = False
        
        if exists:
            print(f"ℹ️  索引已存在: {self.index_name}")
            return
        
        # 创建索引
        print(f"📦 创建索引: {self.index_name}")
        self._client.indices.create(
            index=self.index_name,
            mappings=ES_MAPPING
        )
    
    def index_schema(self, schema: dict[str, Any]) -> None:
        """
        索引单个表的schema。
        
        Args:
            schema: 表结构信息字典
        """
        doc_id = schema.get("table_name", "")
        self._client.index(
            index=self.index_name,
            id=doc_id,
            document=schema,
        )
    
    def bulk_index(self, schemas: list[dict[str, Any]]) -> int:
        """
        批量索引表结构。
        
        Args:
            schemas: 表结构列表
            
        Returns:
            成功索引的数量
        """
        from elasticsearch.helpers import bulk
        
        actions = [
            {
                "_index": self.index_name,
                "_id": schema.get("table_name", ""),
                "_source": schema,
            }
            for schema in schemas
        ]
        
        success, _ = bulk(self._client, actions)
        return success
    
    def search(
        self,
        query: str,
        size: int = 10,
        fields: Optional[list[str]] = None,
    ) -> list[dict[str, Any]]:
        """
        搜索表结构。
        
        Args:
            query: 搜索关键词
            size: 返回结果数量
            fields: 搜索字段列表（支持权重设置，如 "field^2"）
            
        Returns:
            匹配的表结构列表
        """
        if fields is None:
            # 默认字段权重配置
            fields = [
                "table_name^1",           # 表名权重 1
                "column_names_str^1.5",   # 列名权重 1.5
                "table_comment^5",        # 表注释权重 5
                "column_comments_str^5"   # 列注释权重 5
            ]
        
        search_query = {
            "multi_match": {
                "query": query,
                "fields": fields,
                "type": "best_fields",
            }
        }
        
        response = self._client.search(index=self.index_name, query=search_query, size=size)
        
        results = []
        for hit in response["hits"]["hits"]:
            result = hit["_source"]
            result["_score"] = hit["_score"]
            results.append(result)
        
        return results
    
    def get_table(self, table_name: str) -> Optional[dict[str, Any]]:
        """
        获取指定表的schema。
        
        Args:
            table_name: 表名
            
        Returns:
            表结构信息，不存在则返回None
        """
        try:
            response = self._client.get(index=self.index_name, id=table_name)
            return response["_source"]
        except Exception:
            return None


def import_from_json(json_file: str, delete_existing: bool = False) -> None:
    """
    从JSON文件导入表结构到ES。
    
    Args:
        json_file: JSON文件路径
        delete_existing: 是否删除已存在的索引
    """
    with open(json_file, "r", encoding="utf-8") as f:
        schemas = json.load(f)
    
    if not isinstance(schemas, list):
        schemas = [schemas]
    
    store = ElasticsearchStore()
    store.create_index(delete_existing=delete_existing)
    
    print(f"📥 导入 {len(schemas)} 个表结构到ES...")
    count = store.bulk_index(schemas)
    print(f"✅ 成功导入 {count} 个表结构")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="将MySQL表结构导入Elasticsearch")
    parser.add_argument(
        "-f", "--file",
        default="schema/all_tables.json",
        help="JSON文件路径 (默认: schema/all_tables.json)"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="删除已存在的索引后重新创建"
    )
    parser.add_argument(
        "-s", "--search",
        help="搜索关键词（测试用）"
    )
    
    args = parser.parse_args()
    
    if args.search:
        # 搜索模式
        store = ElasticsearchStore()
        results = store.search(args.search)
        print(f"\n🔍 搜索: {args.search}")
        print(f"📊 找到 {len(results)} 个结果:\n")
        for r in results:
            print(f"  [{r['_score']:.2f}] {r['table_name']}: {r.get('table_comment', '')}")
    else:
        # 导入模式
        import_from_json(args.file, args.delete)


if __name__ == "__main__":
    main()
