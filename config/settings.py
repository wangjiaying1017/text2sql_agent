"""
Configuration settings using Pydantic Settings.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # 配置从.env文件加载
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    
    # LLM Configuration
    llm_api_key: str = Field(default="", alias="LLM_API_KEY")
    llm_base_url: str = Field(default="https://api.openai.com/v1", alias="LLM_BASE_URL")
    llm_model: str = Field(default="gpt-5.2", alias="LLM_MODEL")
    
    # ONE_Hub Configuration (优先使用)
    one_hub_api_key: str = Field(default="", alias="ONE_Hub_svip_key")
    one_hub_base_url: str = Field(default="", alias="ONE_Hub_base_url")
    
    # Qwen Configuration (阿里云 API)
    qwen_api_key: str = Field(default="", alias="QWEN_API_KEY")
    qwen_base_url: str = Field(default="https://dashscope.aliyuncs.com/compatible-mode/v1", alias="QWEN_BASE_URL")
    qwen_model: str = Field(default="qwen-flash", alias="QWEN_MODEL")
    
    # MySQL Configuration
    mysql_host: str = Field(default="localhost", alias="MYSQL_HOST")
    mysql_port: int = Field(default=3306, alias="MYSQL_PORT")
    mysql_user: str = Field(default="root", alias="MYSQL_USER")
    mysql_password: str = Field(default="", alias="MYSQL_PASSWORD")
    mysql_database: str = Field(default="", alias="MYSQL_DATABASE")
    
    # InfluxDB 1.x Configuration
    influxdb_host: str = Field(default="localhost", alias="INFLUXDB_HOST")
    influxdb_port: int = Field(default=8086, alias="INFLUXDB_PORT")
    influxdb_user: str = Field(default="", alias="INFLUXDB_USER")
    influxdb_password: str = Field(default="", alias="INFLUXDB_PASSWORD")
    influxdb_database: str = Field(default="", alias="INFLUXDB_DATABASE")
    
    # Elasticsearch Configuration
    es_host: str = Field(default="localhost", alias="ES_HOST")
    es_port: int = Field(default=9200, alias="ES_PORT")
    es_user: str = Field(default="", alias="ES_USER")
    es_password: str = Field(default="", alias="ES_PASSWORD")
    
    # Qdrant Configuration
    qdrant_host: str = Field(default="localhost", alias="QDRANT_HOST")
    qdrant_port: int = Field(default=6333, alias="QDRANT_PORT")
    



# Global settings instance
settings = Settings()

