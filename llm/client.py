"""
LLM Client wrapper using LangChain.
"""
from langchain_openai import ChatOpenAI
from config import settings
"""
模型工厂模块 - 统一管理所有AI模型的创建

提供统一的工厂函数来创建ChatOpenAI和OpenAIEmbeddings实例，
默认使用GPT_load配置，避免重复的API密钥和URL配置。
"""

import os
import json
from typing import Optional, Type, Any
from pydantic import BaseModel
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_deepseek import ChatDeepSeek
from langchain_core.runnables import RunnableLambda
from langchain_core.messages import AIMessage
from langchain_core.output_parsers import PydanticOutputParser
from dotenv import load_dotenv

# 确保环境变量被正确加载
load_dotenv()


def create_model(
    model_name: str,
    temperature: float = 0.7,
    reasoning_effort: Optional[str] = None,
    extra_body: Optional[dict] = None,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    disable_structured_output: bool = False,
    timeout: float = 60.0,  # 超时时间（秒）
    max_retries: int = 2,    # 最大重试次数
) -> ChatOpenAI:
    """
    通用的GPT模型创建工厂函数，默认使用GPT_load配置。
    
    Args:
        model_name (str): 模型名称，如 'gpt-5', 'gpt-5-mini', 'qwen-vl-max' 等
        temperature (float): 模型温度，默认0.7
        reasoning_effort (Optional[str]): 推理努力程度，如 "minimal", "medium", "high"
        extra_body (Optional[dict]): 额外的请求体参数
        api_key (Optional[str]): API密钥，默认使用GPT_load_app_key
        base_url (Optional[str]): 基础URL，默认使用GPT_load_base_url
        disable_structured_output (bool): 是否禁用structured output（用于网关兼容性）
        timeout (float): 请求超时时间（秒），默认120秒
        max_retries (int): 最大重试次数，默认2次
    
    Returns:
        ChatOpenAI: 配置好的ChatOpenAI模型实例
    """
    # 使用LLM_API_KEY配置（ONE_Hub暂时禁用）
    # 如需启用ONE_Hub，取消下面的注释并注释掉当前行
    # default_api_key = api_key or settings.one_hub_api_key or settings.llm_api_key
    # default_base_url = base_url or settings.one_hub_base_url or settings.llm_base_url
    default_api_key = api_key or settings.llm_api_key
    default_base_url = base_url or settings.llm_base_url
    
    # 构建模型参数
    model_params = {
        "model": model_name,
        "api_key": default_api_key,
        "base_url": default_base_url,
        "temperature": temperature,
        "timeout": timeout,
        "max_retries": max_retries,
    }
    
    # 添加可选参数（如果网关不支持某些功能则跳过）
    if reasoning_effort and not disable_structured_output:
        model_params["reasoning_effort"] = reasoning_effort
    
    if extra_body and not disable_structured_output:
        model_params["extra_body"] = extra_body
    
    # 如果需要禁用structured output相关功能，可以在这里移除不兼容的参数
    if disable_structured_output:
        # 移除可能导致兼容性问题的参数
        model_params.pop("reasoning_effort", None)
        if extra_body and "verbosity" in extra_body:
            # 创建一个副本并移除不兼容的字段
            compatible_extra_body = {k: v for k, v in extra_body.items() if k != "verbosity"}
            if compatible_extra_body:
                model_params["extra_body"] = compatible_extra_body
    
    return ChatOpenAI(**model_params)


def create_embedding_model(
    model_name: str = "text-embedding-v4",
    api_key: Optional[str] = None,
    base_url: Optional[str] = None
) -> OpenAIEmbeddings:
    """
    通用的Embedding模型创建工厂函数，默认使用DashScope配置。
    
    Args:
        model_name (str): 嵌入模型名称，默认使用text-embedding-v4 (DashScope)
        api_key (Optional[str]): API密钥，默认使用QWEN_API_KEY
        base_url (Optional[str]): 基础URL，默认使用QWEN_BASE_URL
    
    Returns:
        OpenAIEmbeddings: 配置好的OpenAIEmbeddings实例
    """
    # 使用Qwen API配置（DashScope）
    default_api_key = api_key or settings.qwen_api_key
    default_base_url = base_url or settings.qwen_base_url
    
    return OpenAIEmbeddings(
        model=model_name,
        api_key=default_api_key,
        base_url=default_base_url
    )


def create_deepseek_model(
    model_name: str = "deepseek-chat",
    api_key: Optional[str] = None,
    temperature: float = 0.7
) -> ChatDeepSeek:
    """
    创建ChatDeepSeek模型实例的工厂函数。
    
    Args:
        model_name (str): DeepSeek模型名称，默认使用deepseek-chat
        api_key (Optional[str]): API密钥，默认使用DEEPSEEK_API_KEY
        temperature (float): 模型温度，默认0.7
    
    Returns:
        ChatDeepSeek: 配置好的ChatDeepSeek实例
    """
    default_api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
    
    return ChatDeepSeek(
        model=model_name,
        api_key=default_api_key,
        temperature=temperature
    )


# 预定义的常用模型实例（可选，供快速使用）
def get_default_gpt5():
    """获取默认配置的GPT-5模型"""
    return create_model(
        'customer_service_model',
        temperature=1,
        reasoning_effort="minimal",
        extra_body={"verbosity": "low"}
    )


def get_default_gpt5_mini():
    """获取默认配置的GPT-5-mini模型"""
    return create_model(
        'gpt-5-mini',
        temperature=1,
        extra_body={"verbosity": "low"}
    )


def get_qwen_model(
    model_name: str = None,
    temperature: float = 0.0,
    max_tokens: int = 2048,
    timeout: float = 60.0,  # 超时时间（秒）
    max_retries: int = 2,    # 最大重试次数
):
    """
    获取 Qwen 模型，通过阿里云 DashScope API 访问。
    
    Args:
        model_name: Qwen 模型名称，默认使用 settings.qwen_model
        temperature: 温度参数，默认 0.0（结构化输出建议使用低温度）
        max_tokens: 最大输出 token 数，默认 2048（避免输出被截断）
        timeout: 请求超时时间（秒），默认120秒
        max_retries: 最大重试次数，默认2次
    
    Returns:
        ChatOpenAI: 配置好的 Qwen 模型实例
    """
    return ChatOpenAI(
        model=model_name or settings.qwen_model,
        api_key=settings.qwen_api_key,
        base_url=settings.qwen_base_url,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
        max_retries=max_retries,
    )



def get_llm_client(
    temperature: float = 0.0,
    timeout: float = 60.0,  # 超时时间（秒）
    max_retries: int = 2,    # 最大重试次数
) -> ChatOpenAI:
    """
    Get a configured LLM client.
    
    Args:
        temperature: Sampling temperature (0.0 for deterministic output)
        timeout: Request timeout in seconds, default 120s
        max_retries: Maximum retry attempts, default 2
        
    Returns:
        ChatOpenAI: Configured LangChain chat model
    """
    return ChatOpenAI(
        api_key=settings.llm_api_key,
        base_url=settings.llm_base_url,
        model=settings.llm_model,
        temperature=temperature,
        timeout=timeout,
        max_retries=max_retries,
    )
