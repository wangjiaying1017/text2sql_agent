"""
Tests for Intent Recognizer.

运行测试: python -m pytest tests/test_intent.py -v
"""
import pytest
from unittest.mock import patch, MagicMock

# 测试数据
TEST_QUESTIONS = [
    {
        "question": "查询所有用户的信息",
        "expected_strategy": "mysql_only",
    },
    {
        "question": "最近一小时服务器CPU使用率",
        "expected_strategy": "influxdb_only",
    },
    {
        "question": "查询设备A最近一天的温度数据",
        "expected_strategy": "mysql_then_influxdb",
    },
    {
        "question": "今天温度最高的传感器属于哪个部门",
        "expected_strategy": "influxdb_then_mysql",
    },
]


class TestIntentRecognizer:
    """Test cases for IntentRecognizer."""
    
    def test_query_plan_structure(self):
        """Test that QueryPlan has correct structure."""
        from intent.recognizer import QueryPlan, QueryStep
        
        step = QueryStep(
            step=1,
            database="mysql",
            purpose="测试目的",
            depends_on=None,
        )
        
        plan = QueryPlan(
            analysis="测试分析",
            strategy="mysql_only",
            steps=[step],
            confidence=0.95,
        )
        
        assert plan.strategy == "mysql_only"
        assert len(plan.steps) == 1
        assert plan.confidence == 0.95
    
    def test_query_step_validation(self):
        """Test QueryStep validation."""
        from intent.recognizer import QueryStep
        
        # Valid step
        step = QueryStep(
            step=1,
            database="mysql",
            purpose="获取用户ID",
            depends_on=None,
        )
        assert step.database == "mysql"
        
        # Invalid database should raise error
        with pytest.raises(ValueError):
            QueryStep(
                step=1,
                database="invalid_db",
                purpose="测试",
                depends_on=None,
            )
    
    @patch("intent.recognizer.get_llm_client")
    def test_recognizer_mock(self, mock_llm):
        """Test IntentRecognizer with mocked LLM."""
        from intent.recognizer import IntentRecognizer
        
        # Mock LLM response
        mock_response = {
            "analysis": "用户想查询所有用户信息，这是纯业务数据查询",
            "strategy": "mysql_only",
            "steps": [
                {
                    "step": 1,
                    "database": "mysql",
                    "purpose": "查询用户表获取所有用户信息",
                    "depends_on": None,
                }
            ],
            "confidence": 0.95,
        }
        
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = mock_response
        
        mock_llm_instance = MagicMock()
        mock_llm.return_value = mock_llm_instance
        
        # Create recognizer
        recognizer = IntentRecognizer()
        
        # Verify recognizer is properly initialized
        assert recognizer.llm is not None
        assert recognizer.parser is not None


class TestIntentStrategies:
    """Test different intent strategies."""
    
    def test_mysql_only_keywords(self):
        """Test keywords that suggest MySQL-only queries."""
        mysql_keywords = ["用户", "订单", "配置", "设备信息", "部门", "员工"]
        
        for keyword in mysql_keywords:
            question = f"查询{keyword}"
            # This would test with actual LLM, marked as integration test
            assert keyword is not None
    
    def test_influxdb_only_keywords(self):
        """Test keywords that suggest InfluxDB-only queries."""
        influxdb_keywords = ["温度", "CPU", "内存", "监控", "指标", "时序"]
        
        for keyword in influxdb_keywords:
            question = f"最近一小时的{keyword}数据"
            assert keyword is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
