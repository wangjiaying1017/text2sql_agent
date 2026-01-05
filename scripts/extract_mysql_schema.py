"""
MySQL Schema Extractor

从MySQL数据库中提取表结构信息，生成JSON格式的schema文件，
用于后续的向量检索和Text2SQL。
"""
import json
from typing import Any
from pathlib import Path

from database import MySQLConnector
from config import settings


def get_table_ddl(connector: MySQLConnector, table_name: str) -> str:
    """
    获取表的DDL语句。
    
    Args:
        connector: MySQL连接器
        table_name: 表名
        
    Returns:
        DDL语句字符串
    """
    try:
        result = connector.execute(f"SHOW CREATE TABLE `{table_name}`")
        if result and len(result) > 0:
            row = result[0]
            # SHOW CREATE TABLE 返回的列名可能是 "Create Table" 或第二个值
            # 尝试多种可能的列名
            for key in ["Create Table", "create table", "CREATE TABLE"]:
                if key in row:
                    return row[key]
            # 如果没有找到，尝试获取第二个值（通常是DDL）
            values = list(row.values())
            if len(values) >= 2:
                return values[1]
    except Exception as e:
        print(f"  ⚠️ 获取DDL失败 {table_name}: {e}")
    return ""


def extract_table_schema(connector: MySQLConnector, table_name: str, include_ddl: bool = True) -> dict[str, Any]:
    """
    提取单个表的schema信息。
    
    Args:
        connector: MySQL连接器
        table_name: 表名
        include_ddl: 是否包含DDL语句
        
    Returns:
        包含表结构信息的字典
    """
    # 获取表注释
    table_comment_sql = """
    SELECT TABLE_COMMENT 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """
    table_info = connector.execute(table_comment_sql, (settings.mysql_database, table_name))
    table_comment = table_info[0].get("TABLE_COMMENT", "") if table_info else ""
    
    # 获取列信息
    columns_sql = """
    SELECT COLUMN_NAME, COLUMN_COMMENT, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION
    """
    columns = connector.execute(columns_sql, (settings.mysql_database, table_name))
    
    # 构建列信息列表
    column_list = []
    column_names = []
    column_comments = []
    
    for col in columns:
        col_name = col.get("COLUMN_NAME", "")
        col_comment = col.get("COLUMN_COMMENT", "")
        
        column_list.append({
            "name": col_name,
            "comment": col_comment,
        })
        
        column_names.append(col_name)
        if col_comment:
            column_comments.append(col_comment)
    
    schema = {
        "table_name": table_name,
        "table_comment": table_comment,
        "columns": column_list,
        # 扁平化字段用于ES检索
        "column_names_str": " ".join(column_names),
        "column_comments_str": " ".join(column_comments),
    }
    
    # 添加DDL
    if include_ddl:
        schema["full_ddl"] = get_table_ddl(connector, table_name)
    
    return schema


def extract_all_tables(
    output_dir: str = "schema",
    table_prefixes: list[str] = None,
) -> list[dict[str, Any]]:
    """
    提取数据库中所有表的schema信息。
    
    Args:
        output_dir: 输出目录路径
        table_prefixes: 表名前缀过滤列表（如["t_edge_", "t_device_", "t_client_"]）
        
    Returns:
        所有表的schema列表
    """
    # 默认只提取这些前缀的表（不带尾部下划线，可以匹配 t_edge 和 t_edge_xxx）
    if table_prefixes is None:
        table_prefixes = ["t_edge", "t_device", "t_client"]
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    all_schemas = []
    
    with MySQLConnector() as conn:
        # 获取所有表名
        tables_sql = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        tables = conn.execute(tables_sql, (settings.mysql_database,))
        
        # 过滤表名
        if table_prefixes:
            filtered_tables = []
            for table in tables:
                table_name = table.get("TABLE_NAME", "")
                if any(table_name.startswith(prefix) for prefix in table_prefixes):
                    filtered_tables.append(table)
            tables = filtered_tables
            print(f"📊 过滤条件: {table_prefixes}")
        
        print(f"📊 共 {len(tables)} 个表需要处理")
        
        for table in tables:
            table_name = table.get("TABLE_NAME", "")
            if not table_name:
                continue
            
            print(f"  处理表: {table_name}")
            schema = extract_table_schema(conn, table_name)
            all_schemas.append(schema)
            
            # 保存单个表的JSON文件
            table_file = output_path / f"{table_name}.json"
            with open(table_file, "w", encoding="utf-8") as f:
                json.dump(schema, f, ensure_ascii=False, indent=2)
    
    # 保存汇总文件
    summary_file = output_path / "all_tables.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(all_schemas, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Schema文件已保存到: {output_path.absolute()}")
    print(f"   - 单表文件: {len(all_schemas)} 个")
    print(f"   - 汇总文件: all_tables.json")
    
    return all_schemas


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="从MySQL提取表结构生成JSON")
    parser.add_argument(
        "-o", "--output",
        default="schema",
        help="输出目录路径 (默认: schema)"
    )
    parser.add_argument(
        "-t", "--table",
        help="指定单个表名（不指定则提取所有表）"
    )
    parser.add_argument(
        "-p", "--prefixes",
        nargs="+",
        default=["t_edge", "t_device", "t_client"],
        help="表名前缀过滤 (默认: t_edge t_device t_client)"
    )
    parser.add_argument(
        "-a", "--all",
        action="store_true",
        help="提取所有表，不进行前缀过滤"
    )
    
    args = parser.parse_args()
    
    if args.table:
        # 提取单个表
        output_path = Path(args.output)
        output_path.mkdir(parents=True, exist_ok=True)
        
        with MySQLConnector() as conn:
            schema = extract_table_schema(conn, args.table)
            
            output_file = output_path / f"{args.table}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(schema, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 表 {args.table} 的schema已保存到: {output_file}")
            print(json.dumps(schema, ensure_ascii=False, indent=2))
    else:
        # 提取多个表
        prefixes = None if args.all else args.prefixes
        extract_all_tables(args.output, table_prefixes=prefixes)


if __name__ == "__main__":
    main()
