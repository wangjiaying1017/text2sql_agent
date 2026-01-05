# Text2SQL Agent

基于LangChain框架的Text2SQL智能查询代理，支持MySQL和InfluxDB多数据库查询。

## 功能特性

- 🤖 **智能意图识别**: 使用大模型分析用户问题，自动生成查询计划
- 🔀 **多数据库支持**: 支持MySQL和InfluxDB
- 📋 **查询计划生成**: 支持单库查询和跨库联合查询
- 🛠️ **工具调用**: 基于LangChain Agent的工具调用机制

## 支持的查询策略

| 策略 | 说明 |
|-----|------|
| `mysql_only` | 仅查询MySQL |
| `influxdb_only` | 仅查询InfluxDB |
| `mysql_then_influxdb` | 先MySQL后InfluxDB |
| `influxdb_then_mysql` | 先InfluxDB后MySQL |

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 文件，填入实际配置
```

### 3. 运行

```bash
python main.py
```

## 项目结构

```
text2sql_agent/
├── config/          # 配置管理
├── llm/             # LLM客户端
├── intent/          # 意图识别
├── database/        # 数据库连接器
├── agents/          # Agent和工具
└── main.py          # 主入口
```

## License

MIT
