"""
InfluxDB 1.x 数据库连接器

支持InfluxDB 1.x版本，使用InfluxQL查询语法。
"""
from typing import Any, Optional
from influxdb import InfluxDBClient

from config import settings


class InfluxDBConnector:
    """
    InfluxDB 1.x 数据库连接器
    
    用于执行InfluxQL查询并返回结果。
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
    ):
        """
        初始化InfluxDB 1.x连接器。
        
        Args:
            host: InfluxDB主机地址
            port: InfluxDB端口
            username: InfluxDB用户名
            password: InfluxDB密码
            database: InfluxDB数据库名
        """
        self.host = host or settings.influxdb_host
        self.port = port or settings.influxdb_port
        self.username = username or settings.influxdb_user
        self.password = password or settings.influxdb_password
        self.database = database or settings.influxdb_database
        self._client: Optional[InfluxDBClient] = None
    
    def connect(self) -> None:
        """建立InfluxDB连接。"""
        try:
            self._client = InfluxDBClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
        except Exception as e:
            raise ConnectionError(f"InfluxDB连接失败: {e}")
    
    def disconnect(self) -> None:
        """关闭数据库连接。"""
        if self._client:
            self._client.close()
            self._client = None
    
    def execute(self, query: str) -> list[dict[str, Any]]:
        """
        执行InfluxQL查询并返回结果。
        
        Args:
            query: InfluxQL查询语句
            
        Returns:
            包含查询结果的字典列表
        """
        if not self._client:
            self.connect()
        
        try:
            result = self._client.query(query, database=self.database)
            
            # 将ResultSet转换为字典列表
            results = []
            
            # 检查返回类型
            if result is None:
                return []
            
            # 如果是 ResultSet 对象
            if hasattr(result, 'get_points'):
                for series in result.get_points():
                    results.append(dict(series))
            # 如果是列表（多个查询结果）
            elif isinstance(result, list):
                for item in result:
                    if hasattr(item, 'get_points'):
                        for series in item.get_points():
                            results.append(dict(series))
                    elif isinstance(item, dict):
                        results.append(item)
            # 如果是字典
            elif isinstance(result, dict):
                results.append(result)
            else:
                raise RuntimeError(f"未知的返回类型: {type(result)}, 值: {str(result)[:200]}")
            
            return results
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"InfluxQL查询执行失败: {e}")
    
    def get_measurements(self) -> list[str]:
        """
        获取数据库中的measurement列表。
        
        Returns:
            measurement名称列表
        """
        query = "SHOW MEASUREMENTS"
        results = self.execute(query)
        return [r.get("name", "") for r in results]
    
    def get_fields(self, measurement: str) -> list[dict[str, str]]:
        """
        获取指定measurement的字段列表。
        
        Args:
            measurement: measurement名称
            
        Returns:
            包含字段信息的字典列表，含'fieldKey'和'fieldType'
        """
        query = f'SHOW FIELD KEYS FROM "{measurement}"'
        return self.execute(query)
    
    def get_tags(self, measurement: str) -> list[str]:
        """
        获取指定measurement的标签列表。
        
        Args:
            measurement: measurement名称
            
        Returns:
            标签名称列表
        """
        query = f'SHOW TAG KEYS FROM "{measurement}"'
        results = self.execute(query)
        return [r.get("tagKey", "") for r in results]
    
    def __enter__(self):
        """上下文管理器入口。"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出。"""
        self.disconnect()
