"""
MySQL数据库连接器

提供MySQL数据库的连接、查询执行和Schema获取功能。
"""
from typing import Any, Optional
import mysql.connector
from mysql.connector import Error

from config import settings


class MySQLConnector:
    """
    MySQL数据库连接器
    
    用于执行SQL查询并返回结果。
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        """
        初始化MySQL连接器。
        
        Args:
            host: MySQL主机地址
            port: MySQL端口
            user: MySQL用户名
            password: MySQL密码
            database: MySQL数据库名
        """
        self.host = host or settings.mysql_host
        self.port = port or settings.mysql_port
        self.user = user or settings.mysql_user
        self.password = password or settings.mysql_password
        self.database = database or settings.mysql_database
        self._connection = None
    
    def connect(self) -> None:
        """建立MySQL数据库连接。"""
        try:
            self._connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except Error as e:
            raise ConnectionError(f"MySQL连接失败: {e}")
    
    def disconnect(self) -> None:
        """关闭数据库连接。"""
        if self._connection and self._connection.is_connected():
            self._connection.close()
            self._connection = None
    
    def execute(self, sql: str, params: Optional[tuple] = None) -> list[dict[str, Any]]:
        """
        执行SQL查询并返回结果。
        
        Args:
            sql: SQL查询语句
            params: 可选的查询参数
            
        Returns:
            包含查询结果的字典列表
        """
        if not self._connection or not self._connection.is_connected():
            self.connect()
        
        # 使用buffered cursor，确保结果被完全读取
        cursor = self._connection.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(sql, params)
            
            # 获取所有结果（无论是SELECT还是SHOW命令）
            sql_upper = sql.strip().upper()
            if sql_upper.startswith(("SELECT", "SHOW", "DESCRIBE", "DESC")):
                results = cursor.fetchall()
                # 转换不可JSON序列化的类型（如Decimal）
                return self._convert_results(list(results) if results else [])
            else:
                # 对于INSERT/UPDATE/DELETE，提交事务并返回影响行数
                self._connection.commit()
                return [{"affected_rows": cursor.rowcount}]
        except Error as e:
            raise RuntimeError(f"SQL执行失败: {e}")
        finally:
            cursor.close()
    
    def _convert_results(self, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        转换结果中不可JSON序列化的类型。
        
        将Decimal转为float，bytes转为str等。
        
        Args:
            results: 原始查询结果
            
        Returns:
            转换后的结果
        """
        from decimal import Decimal
        from datetime import datetime, date
        
        converted = []
        for row in results:
            new_row = {}
            for key, value in row.items():
                if isinstance(value, Decimal):
                    new_row[key] = float(value)
                elif isinstance(value, bytes):
                    new_row[key] = value.decode('utf-8', errors='ignore')
                elif isinstance(value, (datetime, date)):
                    new_row[key] = value.isoformat()
                else:
                    new_row[key] = value
            converted.append(new_row)
        return converted
    
    def get_schema(self) -> list[dict[str, Any]]:
        """
        获取数据库Schema信息。
        
        Returns:
            包含表和列信息的列表
        """
        sql = """
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_KEY,
            COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        return self.execute(sql, (self.database,))
    
    def __enter__(self):
        """上下文管理器入口。"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出。"""
        self.disconnect()
