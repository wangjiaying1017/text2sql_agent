"""
导入InfluxDB DDL到ES和Qdrant

将influx_ddl_explanations.json中的InfluxDB measurement信息导入到ES和Qdrant，
与MySQL表结构使用相同的存储格式。
"""
import sys
from pathlib import Path

# 添加项目根目录到 sys.path，使脚本可以从任意目录运行
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from typing import Any

from scripts.import_to_es import ElasticsearchStore
from scripts.import_to_qdrant import QdrantStore


# 集合/索引名称（与MySQL分开存储）
INFLUXDB_ES_INDEX = "influxdb_measurement_schema"
INFLUXDB_QDRANT_COLLECTION = "influxdb_measurement_schema"


def load_influxdb_ddl(json_file: str = "influx_ddl_explanations.json") -> list[dict[str, Any]]:
    """
    加载InfluxDB DDL文件。
    
    Args:
        json_file: JSON文件路径
        
    Returns:
        measurement列表
    """
    file_path = Path(json_file)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {json_file}")
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return data.get("explanations", [])


def convert_to_schema_format(measurement: dict[str, Any]) -> dict[str, Any]:
    """
    将InfluxDB measurement信息转换为与MySQL相同的schema格式。
    
    Args:
        measurement: 原始measurement数据
        
    Returns:
        统一格式的schema字典
    """
    import re
    
    measurement_name = measurement.get("measurement_name", "")
    measurement_desc = measurement.get("measurement_description", "")
    
    # 构建列信息（tags + fields）
    columns = []
    column_names = []
    column_comments = []
    tag_names = []
    field_names = []
    
    # 处理tags
    tags = measurement.get("tags", {})
    for tag_name, tag_desc in tags.items():
        columns.append({
            "name": tag_name,
            "type": "TAG",
            "comment": tag_desc,
        })
        column_names.append(tag_name)
        tag_names.append(tag_name)
        column_comments.append(tag_desc.split("。")[0] if tag_desc else "")  # 取第一句作为简短注释
    
    # 处理fields
    fields = measurement.get("fields", {})
    for field_name, field_desc in fields.items():
        # 从描述中提取类型
        field_type = "FIELD"
        if "integer" in field_desc.lower():
            field_type = "INTEGER"
        elif "float" in field_desc.lower():
            field_type = "FLOAT"
        elif "string" in field_desc.lower():
            field_type = "STRING"
        
        columns.append({
            "name": field_name,
            "type": field_type,
            "comment": field_desc,
        })
        column_names.append(field_name)
        field_names.append(field_name)
        column_comments.append(field_desc.split("。")[0] if field_desc else "")  # 取第一句
    
    # 构建DDL字符串（InfluxDB风格的Schema描述）
    ddl_parts = [f"-- Measurement: {measurement_name}"]
    ddl_parts.append(f"-- 描述: {measurement_desc}")
    ddl_parts.append("")
    ddl_parts.append("-- Tags (维度字段，用于索引和过滤):")
    for tag_name, tag_desc in tags.items():
        ddl_parts.append(f"--   {tag_name}: {tag_desc[:100]}...")
    
    ddl_parts.append("")
    ddl_parts.append("-- Fields (数值字段):")
    for field_name, field_desc in fields.items():
        ddl_parts.append(f"--   {field_name}: {field_desc[:100]}...")
    
    full_ddl = "\n".join(ddl_parts)
    
    # 提取关键词（从 measurement 名称和描述中提取）
    keywords = []
    
    # 从 measurement_name 中提取英文词（按下划线分割）
    name_parts = measurement_name.split("_")
    keywords.extend(name_parts)
    
    # 从描述中提取中文关键词（简单分词：按常见分隔符分割）
    desc_keywords = re.split(r'[，,。/、（）()]+', measurement_desc)
    keywords.extend([k.strip() for k in desc_keywords if k.strip()])
    
    # 添加常见业务关键词映射
    keyword_mapping = {
        "wan": ["wan口", "广域网"],
        "traffic": ["流量"],
        "monitor": ["监控"],
        "bandwidth": ["带宽"],
        "edge": ["边缘", "边缘设备"],
        "cpe": ["CPE", "客户端设备"],
        "node": ["节点"],
        "connectivity": ["连通性", "联通"],
        "delay": ["时延", "延迟"],
        "jitter": ["抖动"],
        "drop": ["丢包"],
        "session": ["会话"],
        "rssi": ["信号强度"],
        "business": ["业务"],
    }
    
    for eng_word, cn_words in keyword_mapping.items():
        if eng_word in measurement_name.lower():
            keywords.extend(cn_words)
    
    # 去重并拼接
    keywords = list(dict.fromkeys(keywords))  # 保持顺序去重
    measurement_keywords = " ".join(keywords)
    
    return {
        "table_name": measurement_name,  # 使用table_name保持一致
        "table_comment": measurement_desc,
        "database_type": "influxdb",  # 标记为InfluxDB
        "columns": columns,
        "column_names_str": " ".join(column_names),
        "column_comments_str": " ".join(column_comments),
        "tags_str": " ".join(tag_names),        # 新增：纯 tags 名称
        "fields_str": " ".join(field_names),    # 新增：纯 fields 名称
        "measurement_keywords": measurement_keywords,  # 新增：提取的关键词
        "full_ddl": full_ddl,
        # 保留原始结构
        "tags": tags,
        "fields": fields,
    }


def import_to_es(
    measurements: list[dict[str, Any]],
    delete_existing: bool = False,
) -> int:
    """
    导入到Elasticsearch。
    
    Args:
        measurements: measurement列表
        delete_existing: 是否删除已存在的索引
        
    Returns:
        成功导入的数量
    """
    # 转换格式
    schemas = [convert_to_schema_format(m) for m in measurements]
    
    # 创建ES存储（使用独立索引）
    store = ElasticsearchStore(index_name=INFLUXDB_ES_INDEX)
    store.create_index(delete_existing=delete_existing)
    
    # 批量导入
    print(f"[ES] Importing {len(schemas)} measurements...")
    count = store.bulk_index(schemas)
    
    print(f"[ES] Successfully imported {count} measurements")
    return count


def import_to_qdrant(
    measurements: list[dict[str, Any]],
    delete_existing: bool = False,
) -> int:
    """
    导入到Qdrant。
    
    Args:
        measurements: measurement列表
        delete_existing: 是否删除已存在的集合
        
    Returns:
        成功导入的数量
    """
    # 转换格式
    schemas = [convert_to_schema_format(m) for m in measurements]
    
    # 创建Qdrant存储（使用独立集合）
    store = QdrantStore(collection_name=INFLUXDB_QDRANT_COLLECTION)
    store.create_collection(delete_existing=delete_existing)
    
    # 批量导入
    print(f"[Qdrant] Importing {len(schemas)} measurements...")
    count = store.batch_upsert(schemas)
    
    print(f"[Qdrant] Successfully imported {count} measurements")
    return count


def main():
    """主函数。"""
    import argparse
    
    parser = argparse.ArgumentParser(description="导入InfluxDB DDL到ES和Qdrant")
    parser.add_argument(
        "-f", "--file",
        default="influx_ddl_explanations.json",
        help="InfluxDB DDL JSON文件路径 (默认: influx_ddl_explanations.json)"
    )
    parser.add_argument(
        "-d", "--delete",
        action="store_true",
        help="删除已存在的索引/集合后重新创建"
    )
    parser.add_argument(
        "--es-only",
        action="store_true",
        help="只导入到ES"
    )
    parser.add_argument(
        "--qdrant-only",
        action="store_true",
        help="只导入到Qdrant"
    )
    
    args = parser.parse_args()
    
    # 加载数据
    print(f"[Loading] File: {args.file}")
    measurements = load_influxdb_ddl(args.file)
    print(f"[Loading] Total: {len(measurements)} measurements")
    
    # 导入
    if args.es_only:
        import_to_es(measurements, delete_existing=args.delete)
    elif args.qdrant_only:
        import_to_qdrant(measurements, delete_existing=args.delete)
    else:
        # 默认导入到两个存储
        import_to_es(measurements, delete_existing=args.delete)
        print()
        import_to_qdrant(measurements, delete_existing=args.delete)
    
    print("\n[Done] Import completed!")


if __name__ == "__main__":
    main()
