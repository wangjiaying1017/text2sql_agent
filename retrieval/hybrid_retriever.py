"""
混合检索器

结合Elasticsearch关键词检索和Qdrant语义检索，使用RRF(Reciprocal Rank Fusion)算法融合结果。
支持MySQL和InfluxDB两种数据源的schema检索。
"""
import json
from typing import Any, Optional, Literal
from collections import defaultdict

from scripts.import_to_es import ElasticsearchStore
from scripts.import_to_qdrant import QdrantStore


# MySQL索引/集合
MYSQL_ES_INDEX = "mysql_table_schema"
MYSQL_QDRANT_COLLECTION = "mysql_table_schema"

# InfluxDB索引/集合
INFLUXDB_ES_INDEX = "influxdb_measurement_schema"
INFLUXDB_QDRANT_COLLECTION = "influxdb_measurement_schema"


class HybridRetriever:
    """
    混合检索器
    
    使用RRF算法融合ES关键词检索和Qdrant语义检索的结果，
    返回与查询最相关的DDL结构。
    
    支持MySQL和InfluxDB两种数据源。
    """
    
    def __init__(
        self,
        database_type: Literal["mysql", "influxdb", "all"] = "mysql",
        use_keyword_search: bool = True,
    ):
        """
        初始化混合检索器。
        
        Args:
            database_type: 目标数据库类型
                - "mysql": 只检索MySQL表结构
                - "influxdb": 只检索InfluxDB measurement
                - "all": 检索两者
            use_keyword_search: 是否启用ES关键词检索
                - True: 混合检索（关键词 + 语义 + RRF融合）
                - False: 仅语义检索（跳过ES关键词检索）
        """
        self.database_type = database_type
        self.use_keyword_search = use_keyword_search
        
        # 延迟初始化的存储实例
        self._mysql_es: Optional[ElasticsearchStore] = None
        self._mysql_qdrant: Optional[QdrantStore] = None
        self._influxdb_es: Optional[ElasticsearchStore] = None
        self._influxdb_qdrant: Optional[QdrantStore] = None
    
    def _get_es_store(self, db_type: str) -> ElasticsearchStore:
        """获取ES存储实例。"""
        if db_type == "mysql":
            if self._mysql_es is None:
                self._mysql_es = ElasticsearchStore(index_name=MYSQL_ES_INDEX)
            return self._mysql_es
        else:
            if self._influxdb_es is None:
                self._influxdb_es = ElasticsearchStore(index_name=INFLUXDB_ES_INDEX)
            return self._influxdb_es
    
    def _get_qdrant_store(self, db_type: str) -> QdrantStore:
        """获取Qdrant存储实例。"""
        if db_type == "mysql":
            if self._mysql_qdrant is None:
                print("🔗 初始化MySQL Qdrant连接...")
                self._mysql_qdrant = QdrantStore(collection_name=MYSQL_QDRANT_COLLECTION)
            return self._mysql_qdrant
        else:
            if self._influxdb_qdrant is None:
                print("🔗 初始化InfluxDB Qdrant连接...")
                self._influxdb_qdrant = QdrantStore(collection_name=INFLUXDB_QDRANT_COLLECTION)
            return self._influxdb_qdrant
    
    def _get_target_db_types(self) -> list[str]:
        """获取要检索的数据库类型列表。"""
        if self.database_type == "all":
            return ["mysql", "influxdb"]
        return [self.database_type]
    
    def search_keyword(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        ES关键词检索。
        
        Args:
            query: 查询文本
            limit: 返回结果数量
            
        Returns:
            检索结果列表，包含_score和表结构信息
        """
        all_results = []
        
        for db_type in self._get_target_db_types():
            try:
                es_store = self._get_es_store(db_type)
                results = es_store.search(query, size=limit)
                # 添加数据库类型标记
                for r in results:
                    r["database_type"] = db_type
                all_results.extend(results)
            except Exception as e:
                print(f"⚠️ ES关键词检索({db_type})失败: {e}")
        
        # 按分数排序并截取
        all_results.sort(key=lambda x: x.get("_score", 0), reverse=True)
        return all_results[:limit]
    
    def search_semantic(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        Qdrant语义检索。
        
        Args:
            query: 查询文本
            limit: 返回结果数量
            
        Returns:
            检索结果列表，包含_score和表结构信息
        """
        all_results = []
        
        for db_type in self._get_target_db_types():
            try:
                qdrant_store = self._get_qdrant_store(db_type)
                results = qdrant_store.search(query, limit=limit)
                # 添加数据库类型标记
                for r in results:
                    r["database_type"] = db_type
                all_results.extend(results)
            except Exception as e:
                print(f"⚠️ Qdrant语义检索({db_type})失败: {e}")
        
        # 按分数排序并截取
        all_results.sort(key=lambda x: x.get("_score", 0), reverse=True)
        return all_results[:limit]
    
    def _get_table_by_name(self, table_name: str) -> Optional[dict[str, Any]]:
        """
        按表名从 Qdrant 精确查询获取表信息。
        
        Args:
            table_name: 表名
            
        Returns:
            表信息字典，如果未找到返回 None
        """
        # 使用 MySQL Qdrant store
        qdrant_store = self._get_qdrant_store("mysql")
        if not qdrant_store:
            return None
        
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            
            # 按 table_name 字段精确匹配
            results = qdrant_store._client.scroll(
                collection_name=qdrant_store.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="table_name",
                            match=MatchValue(value=table_name)
                        )
                    ]
                ),
                limit=1,
                with_payload=True,
            )
            
            if results and results[0]:
                point = results[0][0]
                return point.payload
        except Exception:
            pass
        
        return None
    
    def _get_tables_by_names(self, table_names: list[str]) -> dict[str, dict[str, Any]]:
        """
        批量按表名从 Qdrant 获取表信息。
        
        Args:
            table_names: 表名列表
            
        Returns:
            表名到表信息的映射字典
        """
        if not table_names:
            return {}
        
        qdrant_store = self._get_qdrant_store("mysql")
        if not qdrant_store:
            return {}
        
        result = {}
        try:
            from qdrant_client.models import Filter, FieldCondition, MatchAny
            
            scroll_result = qdrant_store._client.scroll(
                collection_name=qdrant_store.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="table_name",
                            match=MatchAny(any=table_names)
                        )
                    ]
                ),
                limit=len(table_names),
                with_payload=True,
            )
            
            if scroll_result and scroll_result[0]:
                for point in scroll_result[0]:
                    table_name = point.payload.get("table_name", "")
                    if table_name:
                        result[table_name] = point.payload
        except Exception:
            pass
        
        return result
    
    def rrf_fusion(
        self,
        keyword_results: list[dict[str, Any]],
        semantic_results: list[dict[str, Any]],
        k: int = 60,
        top_k: int = 10,
    ) -> list[dict[str, Any]]:
        """
        RRF(Reciprocal Rank Fusion)融合算法。
        
        将关键词检索和语义检索的结果进行融合排序。
        
        Args:
            keyword_results: 关键词检索结果
            semantic_results: 语义检索结果
            k: RRF参数，用于平滑排名差异（默认60）
            top_k: 返回前k个结果
            
        Returns:
            融合后的结果列表，按RRF分数降序排列
        """
        # 存储每个文档的RRF分数和原始数据
        scores = defaultdict(float)
        doc_data = {}
        
        # 先处理语义检索结果（Qdrant 有 structured_description）
        for rank, doc in enumerate(semantic_results):
            doc_id = doc.get("table_name", "")
            if doc_id:
                scores[doc_id] += 1 / (k + rank + 1)
                doc_data[doc_id] = doc  # Qdrant 数据优先
        
        # 再处理关键词检索结果（ES 数据补充，现在 ES 也包含 structured_description）
        for rank, doc in enumerate(keyword_results):
            doc_id = doc.get("table_name", "")
            if doc_id:
                scores[doc_id] += 1 / (k + rank + 1)
                if doc_id not in doc_data:
                    # 只有 Qdrant 没有的表才用 ES 数据
                    doc_data[doc_id] = doc
        
        # 按RRF分数排序
        sorted_ids = sorted(scores.items(), key=lambda x: -x[1])[:top_k]
        
        # 构建最终结果
        results = []
        for doc_id, rrf_score in sorted_ids:
            doc = doc_data.get(doc_id, {})
            results.append({
                "table_name": doc_id,
                "table_comment": doc.get("table_comment", ""),
                "columns": doc.get("columns", []),
                "full_ddl": doc.get("full_ddl", ""),
                "structured_description": doc.get("structured_description", ""),  # 添加结构化描述
                "relationships": doc.get("relationships", []),  # 添加关系信息
                "rrf_score": rrf_score,
            })
        
        return results
    
    def search(
        self,
        query: str,
        top_k: int = 10,
        keyword_limit: int = 20,
        semantic_limit: int = 20,
        k: int = 60,
    ) -> list[dict[str, Any]]:
        """
        混合检索（关键词 + 语义 + RRF融合）。
        
        Args:
            query: 查询文本
            top_k: 返回前k个结果
            keyword_limit: 关键词检索的候选数量
            semantic_limit: 语义检索的候选数量
            k: RRF参数
            
        Returns:
            融合后的结果列表
        """
        # 使用search_with_details实现并行检索
        result = self.search_with_details(query, top_k, keyword_limit, semantic_limit, k)
        return result["fused_results"]
    
    def search_with_details(
        self,
        query: str,
        top_k: int = 3,
        keyword_limit: int = 5,
        semantic_limit: int = 5,
        k: int = 60,
    ) -> dict[str, Any]:
        """
        混合检索（返回详细信息，包括ES和Qdrant的单独结果）。
        使用并行执行优化检索速度。
        
        Args:
            query: 查询文本
            top_k: 返回前k个结果
            keyword_limit: 关键词检索的候选数量
            semantic_limit: 语义检索的候选数量
            k: RRF参数
            
        Returns:
            包含详细信息的字典:
            - keyword_results: ES关键词检索结果
            - semantic_results: Qdrant语义检索结果
            - fused_results: RRF融合结果
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        keyword_results = []
        semantic_results = []
        
        # 根据配置决定是否执行关键词检索
        if self.use_keyword_search:
            # 并行执行ES和Qdrant检索
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(self.search_keyword, query, keyword_limit): "keyword",
                    executor.submit(self.search_semantic, query, semantic_limit): "semantic",
                }
                
                for future in as_completed(futures):
                    search_type = futures[future]
                    try:
                        result = future.result()
                        if search_type == "keyword":
                            keyword_results = result
                        else:
                            semantic_results = result
                    except Exception as e:
                        print(f"⚠️ 并行检索({search_type})失败: {e}")
        else:
            # 仅执行语义检索
            semantic_results = self.search_semantic(query, semantic_limit)
        fused_results = self.rrf_fusion(
            keyword_results=keyword_results,
            semantic_results=semantic_results,
            k=k,
            top_k=top_k,
        )
        
        return {
            "keyword_results": keyword_results,
            "semantic_results": semantic_results,
            "fused_results": fused_results,
        }
    
    def get_ddl_for_query(
        self,
        query: str,
        top_k: int = 5,
    ) -> str:
        """
        获取与查询最相关的DDL结构（格式化输出）。
        
        Args:
            query: 用户查询
            top_k: 返回前k个最相关的表
            
        Returns:
            格式化的DDL结构字符串
        """
        results = self.search(query, top_k=top_k)
        
        if not results:
            return "未找到相关表结构"
        
        # 格式化输出
        output_parts = []
        for i, r in enumerate(results, 1):
            part = f"## 表 {i}: {r['table_name']}"
            if r.get("table_comment"):
                part += f" ({r['table_comment']})"
            part += f"\nRRF分数: {r['rrf_score']:.4f}\n"
            
            if r.get("full_ddl"):
                part += f"\n```sql\n{r['full_ddl']}\n```"
            
            output_parts.append(part)
        
        return "\n\n".join(output_parts)


def main():
    """测试混合检索。"""
    import argparse
    
    parser = argparse.ArgumentParser(description="混合检索测试")
    parser.add_argument(
        "-q", "--query",
        required=True,
        help="查询文本"
    )
    parser.add_argument(
        "-n", "--top-k",
        type=int,
        default=5,
        help="返回结果数量（默认: 5）"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="显示详细信息"
    )
    
    args = parser.parse_args()
    
    print(f"\n🔍 混合检索: {args.query}\n")
    
    retriever = HybridRetriever()
    
    if args.verbose:
        # 分别显示两种检索结果
        print("=" * 50)
        print("📝 ES关键词检索结果:")
        keyword_results = retriever.search_keyword(args.query, limit=10)
        for i, r in enumerate(keyword_results[:5], 1):
            print(f"  {i}. [{r.get('_score', 0):.2f}] {r['table_name']}: {r.get('table_comment', '')}")
        
        print("\n📐 Qdrant语义检索结果:")
        semantic_results = retriever.search_semantic(args.query, limit=10)
        for i, r in enumerate(semantic_results[:5], 1):
            print(f"  {i}. [{r.get('_score', 0):.4f}] {r['table_name']}: {r.get('table_comment', '')}")
        
        print("\n" + "=" * 50)
    
    # 混合检索
    print("🔀 RRF融合结果:")
    results = retriever.search(args.query, top_k=args.top_k)
    
    for i, r in enumerate(results, 1):
        print(f"\n{i}. [{r['rrf_score']:.4f}] {r['table_name']}")
        if r.get("table_comment"):
            print(f"   📋 {r['table_comment']}")


if __name__ == "__main__":
    main()
