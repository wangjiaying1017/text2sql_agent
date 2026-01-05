"""
Schema Regeneration Script

从 MySQL 数据库直接获取原始 DDL（包含完整的 COMMENT），
解析外键关系并生成结构化的 schema JSON。
"""
import json
import re
from typing import Any
from pathlib import Path

from database.mysql_connector import MySQLConnector
from config import settings


# 外键注释模式: (关联t_xxx.yyy字段)
FK_COMMENT_PATTERN = re.compile(r'\(关联\s*(t_\w+)\.(\w+)\s*字段?\)')


# 需要处理的表名前缀
TABLE_PREFIXES = ('t_client', 't_edge', 't_device')


def get_all_tables(connector: MySQLConnector) -> list[str]:
    """获取所有表名（仅返回指定前缀的表）。"""
    result = connector.execute("SHOW TABLES")
    # 结果是 [{"Tables_in_xxx": "table_name"}, ...]
    all_tables = [list(row.values())[0] for row in result]
    # 过滤只保留指定前缀的表
    filtered = [t for t in all_tables if any(t.startswith(p) for p in TABLE_PREFIXES)]
    return filtered


def get_table_ddl(connector: MySQLConnector, table_name: str) -> str:
    """获取表的原始 DDL。"""
    result = connector.execute(f"SHOW CREATE TABLE `{table_name}`")
    if result:
        # 结果是 {"Table": "xxx", "Create Table": "CREATE TABLE ..."}
        return result[0].get("Create Table", "")
    return ""


def parse_ddl_columns(ddl: str) -> list[dict]:
    """
    从 DDL 中解析列信息。
    
    Args:
        ddl: CREATE TABLE DDL 语句
        
    Returns:
        列信息列表，包含 name 和 comment
    """
    columns = []
    
    # 列名提取模式: 以反引号开头
    col_name_pattern = re.compile(r"^`(\w+)`")
    
    # COMMENT 提取模式
    comment_pattern = re.compile(r"COMMENT\s+'((?:[^'\\]|\\.)*)'", re.IGNORECASE)
    
    # 排除这些关键字开头的行（索引、约束等）
    exclude_keywords = ('PRIMARY', 'UNIQUE', 'KEY', 'CONSTRAINT', 'INDEX', 'FOREIGN')
    
    for line in ddl.split('\n'):
        stripped = line.strip()
        
        # 跳过以排除关键字开头的行
        if any(stripped.upper().startswith(kw) for kw in exclude_keywords):
            continue
        
        # 跳过不是列定义的行
        if not stripped.startswith('`'):
            continue
        
        # 提取列名
        name_match = col_name_pattern.match(stripped)
        if not name_match:
            continue
        
        col_name = name_match.group(1)
        
        # 提取 COMMENT
        comment_match = comment_pattern.search(stripped)
        comment = ""
        if comment_match:
            comment = comment_match.group(1)
            # 处理转义的单引号
            comment = comment.replace("\\'", "'")
        
        columns.append({
            "name": col_name,
            "comment": comment
        })
    
    return columns


def extract_table_comment(ddl: str) -> str:
    """从 DDL 中提取表注释。"""
    match = re.search(r"COMMENT\s*=\s*'((?:[^'\\]|\\.)*)'", ddl, re.IGNORECASE)
    if match:
        return match.group(1).replace("\\'", "'")
    return ""


def extract_foreign_keys_from_columns(columns: list[dict], table_name: str) -> list[dict]:
    """
    从列的 COMMENT 中提取外键关系。
    
    策略：
    1. 优先从 COMMENT 中提取：匹配 (关联t_xxx.yyy字段) 模式
    2. 备选：根据列名模式推断：xxx_id → t_xxx.id
    """
    relationships = []
    seen_columns = set()
    
    for col in columns:
        col_name = col.get('name', '')
        comment = col.get('comment', '')
        
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
        if col_name.endswith('_id') and col_name not in seen_columns:
            prefix = col_name[:-3]  # 去掉 _id
            target_table = f"t_{prefix}"
            
            # 排除自引用
            if target_table == table_name:
                continue
            
            relationships.append({
                'column': col_name,
                'target_table': target_table,
                'target_column': 'id',
                'comment': comment,
                'inferred': True  # 标记为推断的
            })
    
    return relationships


def build_schema_entry(table_name: str, ddl: str) -> dict:
    """
    构建单个表的 schema 条目。
    
    Args:
        table_name: 表名
        ddl: 原始 DDL
        
    Returns:
        包含表结构信息的字典
    """
    columns = parse_ddl_columns(ddl)
    table_comment = extract_table_comment(ddl)
    relationships = extract_foreign_keys_from_columns(columns, table_name)
    
    # 构建列名和注释的拼接字符串（用于关键词检索）
    column_names_str = " ".join([c["name"] for c in columns])
    column_comments_str = " ".join([c["comment"] for c in columns if c["comment"]])
    
    return {
        "table_name": table_name,
        "table_comment": table_comment,
        "columns": columns,
        "column_names_str": column_names_str,
        "column_comments_str": column_comments_str,
        "full_ddl": ddl,
        "relationships": relationships,  # 新增：外键关系
    }


def regenerate_schema(output_file: str = "schema/all_tables_v2.json") -> None:
    """
    从数据库重新生成 schema JSON。
    
    Args:
        output_file: 输出文件路径
    """
    print(f"[INFO] Connecting to MySQL: {settings.mysql_host}:{settings.mysql_port}")
    
    with MySQLConnector() as conn:
        tables = get_all_tables(conn)
        print(f"[INFO] Found {len(tables)} tables")
        
        schemas = []
        fk_count = 0
        
        for i, table_name in enumerate(tables):
            if (i + 1) % 20 == 0:
                print(f"   Processing... {i + 1}/{len(tables)}")
            
            ddl = get_table_ddl(conn, table_name)
            if ddl:
                entry = build_schema_entry(table_name, ddl)
                schemas.append(entry)
                fk_count += len(entry.get("relationships", []))
        
        print(f"[OK] Successfully parsed {len(schemas)} tables")
        print(f"[INFO] Found {fk_count} foreign key relationships")
        
        # 保存到文件
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(schemas, f, ensure_ascii=False, indent=2)
        
        print(f"[INFO] Saved to: {output_file}")
        
        # 显示一些示例
        print("\n=== Example: t_edge table foreign keys ===")
        t_edge = next((s for s in schemas if s["table_name"] == "t_edge"), None)
        if t_edge:
            for rel in t_edge.get("relationships", []):
                inferred = " (inferred)" if rel.get("inferred") else ""
                print(f"  - {rel['column']} -> {rel['target_table']}.{rel['target_column']}{inferred}")


if __name__ == "__main__":
    regenerate_schema()
