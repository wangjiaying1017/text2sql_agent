"""
SQL查询生成器

使用LLM根据用户问题和数据库Schema生成SQL/InfluxQL查询语句。
"""
from typing import Literal
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from config.settings import settings
from llm.client import create_model
SQL_GENERATION_PROMPT = """你是一个SQL专家。根据用户问题和数据库schema，生成合适的查询语句。

## 目标数据库
{database_type}

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

## 数据库Schema（RAG检索结果）
{schema}

## 用户问题
{question}

## 查询目的
{purpose}

## 上下文信息（来自之前步骤的结果）
{context}

## 要求
1. 只输出SQL/Flux查询语句，不要解释
2. 确保语法正确
3. 如果有上下文信息，请在查询中使用
4. 只能使用schema中明确给出的表和字段
5. 观察上下文中schema信息，理解表与表之间的关联关系
6. 保证生成的sql语句能够高效运行
7. 当用户使用设备名称或客户名称查询时，必须通过 t_edge 或 t_client 表进行 JOIN 关联

请生成查询语句："""


class SQLGenerator:
    """
    SQL/InfluxQL查询生成器
    
    使用LLM根据问题、目的、Schema和上下文生成数据库查询语句。
    """
    
    def __init__(self):
        self.llm = create_model(model_name=settings.llm_model,reasoning_effort="medium",temperature=0.0)
        self.prompt = ChatPromptTemplate.from_template(SQL_GENERATION_PROMPT)
        self.parser = StrOutputParser()
    
    def generate(
        self,
        question: str,
        purpose: str,
        database_type: Literal["mysql", "influxdb"],
        schema: str,
        context: str = "",
        verbose: bool = False,
    ) -> str:
        """
        根据用户问题生成SQL或InfluxQL查询。
        
        Args:
            question: 用户原始问题
            purpose: 本次查询的目的
            database_type: 目标数据库类型
            schema: 数据库Schema信息
            context: 前置查询结果上下文
            verbose: 是否打印完整 prompt
            
        Returns:
            生成的SQL或InfluxQL查询语句
        """
        chain = self.prompt | self.llm | self.parser
        
        db_info = "MySQL (使用标准SQL语法)" if database_type == "mysql" else "InfluxDB (使用InfluxQL查询语法)"
        
        prompt_inputs = {
            "database_type": db_info,
            "schema": schema,
            "question": question,
            "purpose": purpose,
            "context": context or "无",
        }
        
        # 打印完整 prompt（用于调试）
        if verbose:
            formatted_prompt = self.prompt.format(**prompt_inputs)
            print("\n" + "="*60)
            print("📝 SQL Generation Prompt:")
            print("="*60)
            print(formatted_prompt)
            print("="*60 + "\n")
        
        result = chain.invoke(prompt_inputs)
        
        # 清理结果（移除markdown代码块）
        result = result.strip()
        
        # 移除所有markdown代码块标记
        import re
        # 匹配 ```sql, ```influxql, ``` 等
        result = re.sub(r'```(?:sql|influxql|influx)?\s*\n?', '', result)
        result = re.sub(r'\n?```\s*', '', result)
        
        # 如果结果中包含多个SQL（用空行分隔），只取第一个
        if '\n\n' in result:
            parts = result.split('\n\n')
            # 取第一个非空的部分
            for part in parts:
                if part.strip():
                    result = part.strip()
                    break
        
        return result.strip()
