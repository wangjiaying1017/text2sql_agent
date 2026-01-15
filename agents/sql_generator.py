"""
SQL查询生成器

使用LLM根据用户问题和数据库Schema生成SQL/InfluxQL查询语句。
"""
from typing import Literal
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from config.settings import settings
from llm.client import create_model

# ==================== MySQL 专用 Prompt ====================
MYSQL_GENERATION_PROMPT = """你是一个MySQL专家。根据用户问题和数据库schema，生成准确的SQL查询语句。

## 核心基础表（设备和客户主表，使用这两个表与其他表进行关联）

### t_edge（设备信息表）
Table: t_edge
Business Meaning: 边缘节点信息表，版本号等信息由EC上报经数据整理后更新

Primary Key:
- id: 数据库主键id

Important Columns:
- id: 数据库主键id
- name: 设备名称，由客户添加设备时输入
- serial: 设备序列号
- status: 0:未激活，1:已激活，2:手动停止，3:欠费停止，4:到期停止
- mac: 设备WAN口MAC地址
- client_id: 公司id，代表该设备属于哪个公司

Relationships:
- t_edge.client_id -> t_client.id (公司id，代表该设备属于哪个公司)
- t_edge.apply_id -> t_edge_apply.id (设备信息id)

Join Hints:
- JOIN t_client ON t_edge.client_id = t_client.id
- JOIN t_edge_apply ON t_edge.apply_id = t_edge_apply.id

### t_client（客户信息表）
Table: t_client
Business Meaning: 公司(客户)信息表

Primary Key:
- id: 早期客户id，通过uuid生成
- auto_id: 数据库自增id

Important Columns:
- id: 客户id (UUID格式)
- auto_id: 数据库自增id
- name: 公司名称
- status: 状态
- balance: 余额

Relationships:
- 其他表通过 client_id 关联到 t_client.id

## 数据库Schema
{schema}

## 用户问题
{question}

## 查询目的
{purpose}

## 上下文信息（来自之前步骤的结果）
{context}

## 要求
1. **只输出一条 SQL 查询语句**，不要输出多条，不要解释
2. 确保语法正确
3. 只能使用schema中明确给出的表和字段
4. 观察上下文中schema信息，理解表与表之间的关联关系
5. 保证生成的sql语句能够高效运行
6. 当用户使用设备名称或客户名称查询时，必须通过 t_edge 或 t_client 表进行 JOIN 关联
7. ***如果某条件未被明确指定***，则一定不要出现在 WHERE 子句中
8. ***不一定需要使用到上下文中所有的schema信息***，根据用户问题使用必要的schema即可

请生成SQL查询语句："""


# ==================== InfluxDB 专用 Prompt ====================
INFLUXQL_GENERATION_PROMPT = """你是一个InfluxQL专家。根据用户问题和measurement schema，生成准确的InfluxQL查询语句。

## ⚠️ InfluxQL 核心限制（必须严格遵守！）
1. **不支持 JOIN**：每个查询只能查询一个 measurement
2. **不支持子查询**：不能使用 `IN (SELECT ...)` 或嵌套 SELECT
3. **不支持 OR 连接 tag 条件**：tag 过滤只能用 AND 连接，但字段(field)条件可以用 OR
4. **只输出一条查询语句**：不要输出多条语句或额外文本

## InfluxQL 语法规则
1. **时间过滤**：
   - 相对时间：`WHERE time >= now() - 3h`（支持 s/m/h/d/w）
   - 绝对时间：`WHERE time >= '2024-01-14T00:00:00Z'`
2. **引用规则**：
   - 字符串值用单引号：`'abc123'`
   - measurement名、tag名、field名用双引号：`"cpu_usage"`、`"serial"`
3. **聚合函数**：支持 `MEAN()`、`MAX()`、`MIN()`、`SUM()`、`COUNT()` 等
4. **GROUP BY**：支持按时间间隔分组，如 `GROUP BY time(5m)`

## 重要注意事项
1. **区分 tag 和 field**：
   - tag：用于过滤和分组，支持 `=`、`!=` 操作
   - field：存储数值，支持 `>`、`<`、`>=`、`<=`、`=`、`!=` 操作
2. **默认时间顺序**：InfluxDB 默认按时间升序返回（从旧到新）
3. **空值处理**：使用 `fill(none)` 来排除没有值的间隔

## Measurement Schema
{schema}

## 当前时间（UTC）
{current_time_utc}

## 用户问题
{question}

## 查询目的
{purpose}

## 上下文信息（来自之前步骤的结果）
{context}

## 输出要求
1. **只输出一条完整的InfluxQL查询语句**，不要有任何解释、注释或其他文本
2. 严格使用上述语法规则，确保语法正确
3. **时间范围使用绝对时间**：
   - 使用上面提供的"当前时间（UTC）"作为基准
   - 例如：查询近3小时，使用 `time >= '计算后的UTC时间'`，而不是 `now() - 3h`
   - 如果用户未指定时间范围，默认查询近 3 小时的数据
4. 设备过滤优先级：
   - 如果用户问题中指定了设备，使用用户的指定
   - 如果上下文提供了有效的设备序列号（serial），在 WHERE 中使用它
   - 如果上下文显示"上一步查询无结果"或没有序列号，则不要添加 serial 过滤
5. 只能使用 schema 中明确给出的 measurement、tag 和 field
6. **不要添加 LIMIT 子句**，除非用户明确要求限制返回数量
7. **不要添加 ORDER BY time DESC**，除非用户明确要求按时间降序排列
8. 如果需要聚合数据，考虑使用 GROUP BY time() 子句
9. 确保 WHERE 子句中 tag 条件使用 AND 连接，不能使用 OR

请生成InfluxQL查询语句："""


# 保留旧的通用 prompt 作为备用
SQL_GENERATION_PROMPT = MYSQL_GENERATION_PROMPT



class SQLGenerator:
    """
    SQL/InfluxQL查询生成器
    
    使用LLM根据问题、目的、Schema和上下文生成数据库查询语句。
    """
    
    def __init__(self):
        self.llm = create_model(model_name=settings.llm_model,reasoning_effort="medium", temperature=0.0)
        self.prompt = ChatPromptTemplate.from_template(SQL_GENERATION_PROMPT)
        self.parser = StrOutputParser()
    
    def generate(
        self,
        question: str,
        purpose: str,
        database_type: Literal["mysql", "influxdb"],
        schema: str,
        context: str = "",
        verbose: bool = True,  # 默认开启 verbose
    ) -> str:
        """
        根据用户问题生成SQL或InfluxQL查询。
        
        Args:
            question: 用户原始问题
            purpose: 本次查询的目的
            database_type: 目标数据库类型
            schema: 数据库Schema信息
            context: 前置查询结果上下文
            verbose: 是否打印完整 prompt（默认 True）
            
        Returns:
            生成的SQL或InfluxQL查询语句
        """
        # 根据数据库类型选择对应的 prompt
        if database_type == "mysql":
            prompt_template = ChatPromptTemplate.from_template(MYSQL_GENERATION_PROMPT)
        else:
            prompt_template = ChatPromptTemplate.from_template(INFLUXQL_GENERATION_PROMPT)
        
        chain = prompt_template | self.llm | self.parser
        
        prompt_inputs = {
            "schema": schema,
            "question": question,
            "purpose": purpose,
            "context": context or "无",
        }
        
        # InfluxQL 需要注入当前 UTC 时间
        if database_type != "mysql":
            from datetime import datetime, timezone
            current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            prompt_inputs["current_time_utc"] = current_utc
        
        # 打印完整 prompt（用于调试）
        if verbose:
            formatted_prompt = prompt_template.format(**prompt_inputs)
            print("\n" + "─"*60)
            print(f"📝 [SQL Generator] Prompt ({database_type.upper()})")
            print("─"*60)
            # 限制显示长度避免刷屏
            if len(formatted_prompt) > 2000:
                print(formatted_prompt[:1000])
                print(f"\n... (省略 {len(formatted_prompt) - 2000} 字符) ...\n")
                print(formatted_prompt[-1000:])
            else:
                print(formatted_prompt)
            print("─"*60 + "\n")
        
        result = chain.invoke(prompt_inputs)
        
        # 清理结果（移除markdown代码块）
        result = result.strip()
        
        # 移除所有markdown代码块标记
        import re
        # 匹配 ```sql, ```influxql, ``` 等
        result = re.sub(r'```(?:sql|influxql|influx)?\s*\n?', '', result)
        result = re.sub(r'\n?```\s*', '', result)
        
        # 如果结果中包含多个SQL，只取第一个有效的 SELECT 语句
        # 策略1: 按分号分隔（处理 "SELECT ...; SELECT ..." 的情况）
        if ';' in result:
            parts = result.split(';')
            for part in parts:
                part = part.strip()
                if part and part.upper().startswith('SELECT'):
                    result = part
                    break
        
        # 策略2: 按双空行分隔
        if '\n\n' in result:
            parts = result.split('\n\n')
            # 取第一个非空的部分
            for part in parts:
                if part.strip():
                    result = part.strip()
                    break
        
        return result.strip()
