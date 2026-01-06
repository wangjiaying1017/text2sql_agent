"""
Prompt templates for intent recognition.
"""

INTENT_RECOGNITION_SYSTEM_PROMPT = """你是一个查询规划助手。分析用户问题，生成查询计划。

## 输出格式 (JSON)
{{
    "steps": [{{"step": 0, "database": "mysql", "purpose": "任务描述", "depends_on": null}}]
}}


"""

INTENT_RECOGNITION_USER_PROMPT = """## 数据库

### MySQL（结构化数据：设备信息、客户信息、配置信息）
相关表：
{mysql_relevant_tables}



---
## 用户问题
{question}

## 规则（重要要求！）
1. **如果所有数据都在 MySQL 中**：必须只生成 1 个 step

2. **只有跨数据库场景**才允许多个 steps

请直接输出JSON格式的查询计划。"""
