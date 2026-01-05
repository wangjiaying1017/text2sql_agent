"""
会话记忆管理模块

使用LangChain的ConversationBufferWindowMemory管理短期对话上下文。
"""
from typing import Optional
from langchain_classic.memory import ConversationBufferWindowMemory


class SessionMemory:
    """
    会话记忆管理器
    
    管理多轮对话的上下文，用于支持澄清机制。
    """
    
    def __init__(self, window_size: int = 3):
        """
        初始化会话记忆。
        
        Args:
            window_size: 保留的对话轮数
        """
        self.memory = ConversationBufferWindowMemory(
            k=window_size,
            return_messages=True,
        )
    
    def add_exchange(self, user_message: str, assistant_message: str) -> None:
        """
        添加一轮对话交换。
        
        Args:
            user_message: 用户消息
            assistant_message: 助手回复
        """
        self.memory.save_context(
            {"input": user_message},
            {"output": assistant_message}
        )
    
    def get_history(self) -> str:
        """
        获取对话历史字符串。
        
        Returns:
            格式化的对话历史
        """
        messages = self.memory.load_memory_variables({})
        history = messages.get("history", [])
        
        if not history:
            return ""
        
        # 格式化历史记录
        lines = []
        for msg in history:
            role = "用户" if msg.type == "human" else "系统"
            lines.append(f"{role}: {msg.content}")
        
        return "\n".join(lines)
    
    def clear(self) -> None:
        """清除对话历史。"""
        self.memory.clear()
    
    def is_empty(self) -> bool:
        """检查是否有历史记录。"""
        return len(self.memory.load_memory_variables({}).get("history", [])) == 0
