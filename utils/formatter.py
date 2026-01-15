"""
数据格式转换工具

提供查询结果的格式转换功能。
"""
from typing import Any, Optional

# 默认时区（北京时间）
DEFAULT_TIMEZONE = "Asia/Shanghai"


def convert_timezone(
    results: list[dict[str, Any]],
    timezone: str = DEFAULT_TIMEZONE,
    time_columns: list[str] = None,
) -> list[dict[str, Any]]:
    """
    将结果中的时间列转换为指定时区。
    
    支持的时间格式：
    - ISO 字符串：'2026-01-13T10:00:00Z'
    - 毫秒级时间戳（整数）：1736762400000
    - 秒级时间戳（整数）：1736762400
    
    Args:
        results: 查询结果列表
        timezone: 目标时区（默认 Asia/Shanghai）
        time_columns: 时间列名列表，如果为 None 则自动检测（包含 "time" 的列）
        
    Returns:
        时区转换后的结果列表
    """
    import pandas as pd
    from datetime import datetime
    
    if not results:
        return results
    
    # 自动检测时间列
    if time_columns is None:
        time_columns = [k for k in results[0].keys() if "time" in k.lower()]
    
    if not time_columns:
        return results
    
    # 转换时区
    converted = []
    for row in results:
        new_row = row.copy()
        for col in time_columns:
            if col in new_row and new_row[col] is not None:
                try:
                    val = new_row[col]
                    
                    # 处理数字类型的时间戳
                    if isinstance(val, (int, float)):
                        # 判断是毫秒还是秒（毫秒级时间戳 > 10^12）
                        if val > 1e12:
                            # 毫秒级时间戳
                            ts = pd.Timestamp(val, unit='ms', tz='UTC')
                        else:
                            # 秒级时间戳
                            ts = pd.Timestamp(val, unit='s', tz='UTC')
                    else:
                        # 字符串格式
                        ts = pd.to_datetime(val)
                        if ts.tzinfo is None:
                            ts = ts.tz_localize("UTC")
                    
                    # 转换到目标时区
                    ts = ts.tz_convert(timezone)
                    # 格式化输出
                    new_row[col] = ts.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass  # 解析失败则保留原值
        converted.append(new_row)
    
    return converted


def results_to_dataframe(
    results: list[dict[str, Any]],
    convert_tz: bool = True,
    timezone: str = DEFAULT_TIMEZONE,
) -> "pd.DataFrame":
    """
    将查询结果列表转换为 pandas DataFrame。
    
    Args:
        results: 查询结果列表，每个元素是一个字典
        convert_tz: 是否自动转换时区（默认 True）
        timezone: 目标时区（默认 Asia/Shanghai）
        
    Returns:
        pandas DataFrame 对象
        
    Example:
        >>> results = [{"time": "2026-01-13T10:00:00Z", "value": 100}]
        >>> df = results_to_dataframe(results)
        >>> print(df)
                          time  value
        0  2026-01-13 18:00:00    100
    """
    import pandas as pd
    
    if not results:
        return pd.DataFrame()
    
    # 时区转换
    if convert_tz:
        results = convert_timezone(results, timezone)
    
    return pd.DataFrame(results)


def format_results(
    results: list[dict[str, Any]],
    format: str = "table",
    max_rows: int = 20,
    convert_tz: bool = True,
    timezone: str = DEFAULT_TIMEZONE,
) -> str:
    """
    将查询结果格式化为字符串输出。
    
    Args:
        results: 查询结果列表
        format: 输出格式，支持 "table"（默认）, "markdown", "csv", "json"
        max_rows: 最大显示行数
        convert_tz: 是否自动转换时区（默认 True）
        timezone: 目标时区（默认 Asia/Shanghai）
        
    Returns:
        格式化后的字符串
    """
    import pandas as pd
    
    if not results:
        return "无查询结果"
    
    # 时区转换
    if convert_tz:
        results = convert_timezone(results, timezone)
    
    df = pd.DataFrame(results)
    
    # 截取显示行数
    display_df = df.head(max_rows)
    truncated = len(df) > max_rows
    
    if format == "markdown":
        output = display_df.to_markdown(index=False)
    elif format == "csv":
        output = display_df.to_csv(index=False)
    elif format == "json":
        output = display_df.to_json(orient="records", force_ascii=False, indent=2)
    else:  # table
        output = display_df.to_string(index=False)
    
    if truncated:
        output += f"\n... (共 {len(df)} 行，只显示前 {max_rows} 行)"
    
    return output


def plot_line_chart(
    results: list[dict[str, Any]],
    time_column: str = None,
    value_columns: list[str] = None,
    group_column: str = None,
    title: str = "查询结果",
    convert_tz: bool = True,
    timezone: str = DEFAULT_TIMEZONE,
) -> bool:
    """
    绘制折线图并弹出显示。
    
    Args:
        results: 查询结果列表
        time_column: 时间列名，如果为 None 则自动检测
        value_columns: 数值列名列表，如果为 None 则自动检测
        group_column: 分组列名（如 wan_name），用于绘制多条线
        title: 图表标题
        convert_tz: 是否自动转换时区
        timezone: 目标时区
        
    Returns:
        是否成功绘制图表
    """
    import matplotlib.pyplot as plt
    import pandas as pd
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False
    
    if not results or len(results) < 2:
        return False
    
    # 时区转换
    if convert_tz:
        results = convert_timezone(results, timezone)
    
    df = pd.DataFrame(results)
    
    # 自动检测时间列
    if time_column is None:
        time_cols = [c for c in df.columns if "time" in c.lower()]
        if time_cols:
            time_column = time_cols[0]
        else:
            return False  # 无时间列，无法绘制折线图
    
    # 自动检测数值列
    if value_columns is None:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        # 排除可能的 ID 列
        value_columns = [c for c in numeric_cols if not c.lower().endswith('_id') and c.lower() != 'id']
    
    if not value_columns:
        return False  # 无数值列，无法绘制
    
    # 转换时间列
    try:
        df[time_column] = pd.to_datetime(df[time_column])
    except Exception:
        return False
    
    # 创建图表
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # 如果有分组列，按分组绘制多条线
    if group_column and group_column in df.columns:
        groups = df[group_column].unique()
        for group in groups:
            group_df = df[df[group_column] == group].sort_values(time_column)
            for col in value_columns:
                ax.plot(group_df[time_column], group_df[col], label=f"{group} - {col}", marker='o', markersize=3)
    else:
        # 单组数据
        df = df.sort_values(time_column)
        for col in value_columns:
            ax.plot(df[time_column], df[col], label=col, marker='o', markersize=3)
    
    # 设置图表样式
    ax.set_xlabel("时间")
    ax.set_ylabel("值")
    ax.set_title(title)
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)
    
    # 旋转 x 轴标签
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # 弹出显示
    plt.show()
    
    return True
