# 检查 full_ddl 中是否有关联信息
import json
import re

FK_PATTERN = re.compile(r'\(关联\s*(t_\w+)\.(\w+)\s*字段?\)')

with open('schema/all_tables.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 检查几个表的 full_ddl
tables_with_fk = []
for t in data[:50]:
    ddl = t.get('full_ddl', '')
    matches = FK_PATTERN.findall(ddl)
    if matches:
        tables_with_fk.append({
            'table': t['table_name'],
            'fk_count': len(matches),
            'fks': matches[:5]
        })

print(f"在前50个表中，找到 {len(tables_with_fk)} 个表包含外键注释:")
for info in tables_with_fk:
    print(f"  {info['table']}: {info['fk_count']} 个")
    for fk in info['fks']:
        print(f"    - 关联 {fk[0]}.{fk[1]}")

# 如果没有找到，打印一个表的 client_id 列的 COMMENT 部分
if not tables_with_fk:
    print("\n未找到标准格式的外键注释。")
    print("\n检查 t_edge 表的 full_ddl 中 client_id 的定义:")
    t_edge = [t for t in data if t['table_name'] == 't_edge'][0]
    ddl = t_edge['full_ddl']
    # 查找 client_id 行
    for line in ddl.split('\n'):
        if 'client_id' in line.lower():
            print(f"  {line}")
