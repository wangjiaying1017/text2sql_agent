"""
简化InfluxDB DDL描述

将influx_ddl_explanations.json中的冗长描述简化为简短版本。
"""
import json
from pathlib import Path


def shorten_description(desc: str, max_length: int = 80) -> str:
    """
    简化描述，保留核心信息。
    
    Args:
        desc: 原始描述
        max_length: 最大长度
        
    Returns:
        简化后的描述
    """
    if not desc:
        return ""
    
    # 取第一句（以句号、分号或逗号分隔）
    for sep in ["。", "；", "，"]:
        if sep in desc:
            first_part = desc.split(sep)[0]
            if len(first_part) <= max_length:
                return first_part + sep if sep == "。" else first_part
            break
    
    # 如果还是太长，直接截断
    if len(desc) > max_length:
        return desc[:max_length-3] + "..."
    
    return desc


def simplify_influxdb_ddl(
    input_file: str = "influx_ddl_explanations.json",
    output_file: str = "influx_ddl_explanations_simple.json",
    max_desc_length: int = 100,
    max_field_length: int = 60,
):
    """
    简化InfluxDB DDL描述文件。
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
        max_desc_length: measurement描述最大长度
        max_field_length: tag/field描述最大长度
    """
    input_path = Path(input_file)
    if not input_path.exists():
        print(f"❌ 文件不存在: {input_file}")
        return
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    explanations = data.get("explanations", [])
    print(f"📊 共 {len(explanations)} 个 measurement")
    
    simplified_count = 0
    
    for m in explanations:
        # 简化 measurement 描述
        original_desc = m.get("measurement_description", "")
        if len(original_desc) > max_desc_length:
            m["measurement_description"] = shorten_description(original_desc, max_desc_length)
            simplified_count += 1
        
        # 简化 tags 描述
        tags = m.get("tags", {})
        for tag_name, tag_desc in tags.items():
            if len(tag_desc) > max_field_length:
                tags[tag_name] = shorten_description(tag_desc, max_field_length)
        
        # 简化 fields 描述
        fields = m.get("fields", {})
        for field_name, field_desc in fields.items():
            if len(field_desc) > max_field_length:
                fields[field_name] = shorten_description(field_desc, max_field_length)
    
    # 保存
    output_path = Path(output_file)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 简化完成，共简化 {simplified_count} 个 measurement")
    print(f"📁 输出文件: {output_path.absolute()}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="简化InfluxDB DDL描述")
    parser.add_argument(
        "-i", "--input",
        default="influx_ddl_explanations.json",
        help="输入文件路径"
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="输出文件路径（默认覆盖原文件）"
    )
    parser.add_argument(
        "--max-desc",
        type=int,
        default=100,
        help="measurement描述最大长度（默认100）"
    )
    parser.add_argument(
        "--max-field",
        type=int,
        default=60,
        help="tag/field描述最大长度（默认60）"
    )
    
    args = parser.parse_args()
    
    output_file = args.output or args.input  # 默认覆盖原文件
    
    simplify_influxdb_ddl(
        input_file=args.input,
        output_file=output_file,
        max_desc_length=args.max_desc,
        max_field_length=args.max_field,
    )


if __name__ == "__main__":
    main()
