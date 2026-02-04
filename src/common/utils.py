"""
工具函数模块
"""
import pytz
from datetime import datetime, timezone
from typing import Optional
from decimal import Decimal, ROUND_DOWN


# 时区定义
BEIJING_TZ = pytz.timezone('Asia/Shanghai')
UTC_TZ = pytz.timezone('UTC')


def beijing_now() -> datetime:
    """获取当前北京时间"""
    return datetime.now(BEIJING_TZ)


def utc_now() -> datetime:
    """获取当前UTC时间"""
    return datetime.now(timezone.utc)


def beijing_to_utc(dt: datetime) -> datetime:
    """北京时间转换为UTC时间"""
    if dt.tzinfo is None:
        dt = BEIJING_TZ.localize(dt)
    return dt.astimezone(UTC_TZ)


def utc_to_beijing(dt: datetime) -> datetime:
    """UTC时间转换为北京时间"""
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    return dt.astimezone(BEIJING_TZ)


def parse_time_str(time_str: str, timezone_str: str = 'Asia/Shanghai') -> datetime:
    """
    解析时间字符串（格式: HH:MM）
    返回指定时区的datetime对象（当天）
    """
    tz = pytz.timezone(timezone_str)
    now = datetime.now(tz)
    hour, minute = map(int, time_str.split(':'))
    dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    # 如果时间已过，则使用明天的这个时间
    if dt < now:
        from datetime import timedelta
        dt = dt + timedelta(days=1)
    
    return dt


def round_down(value: float, decimals: int = 8) -> float:
    """
    向下取整（用于价格和数量精度控制）
    """
    decimal_value = Decimal(str(value))
    decimal_places = Decimal(10) ** -decimals
    rounded = decimal_value.quantize(decimal_places, rounding=ROUND_DOWN)
    return float(rounded)


def round_price(price: float, tick_size: float) -> float:
    """
    按tick_size向下取整价格
    """
    return float(int(price / tick_size) * tick_size)


def round_qty(qty: float, step_size: float) -> float:
    """
    按step_size向下取整数量，确保精度符合Binance要求
    
    使用Decimal避免浮点数精度问题，确保结果符合step_size的精度要求
    """
    if step_size <= 0:
        return qty
    
    # 使用Decimal确保精度
    qty_decimal = Decimal(str(qty))
    step_decimal = Decimal(str(step_size))
    
    # 向下取整：计算能容纳多少个step_size
    steps = int(qty_decimal / step_decimal)
    
    # 计算最终数量
    result = steps * step_decimal
    
    # 转换为float，但先格式化为正确的小数位数
    # 计算step_size的小数位数
    step_str = str(step_size).rstrip('0')
    if '.' in step_str:
        decimals = len(step_str.split('.')[1])
    else:
        decimals = 0
    
    # 使用Decimal格式化，避免浮点数精度问题
    result_str = format(result, f'.{decimals}f')
    return float(result_str)


def format_symbol(symbol: str) -> str:
    """格式化交易对符号（统一为大写）"""
    return symbol.upper()

def to_system_symbol(symbol: str) -> str:
    """
    将交易对转换为系统内部展示格式：btc-usdt（小写 + 连字符）
    支持输入：BTCUSDT / btcusdt / BTC-USDT / btc-usdt
    """
    s = (symbol or "").strip()
    if not s:
        return s
    s = s.replace("_", "-").replace("/", "-")
    s_up = s.upper()
    if "-" in s_up:
        parts = s_up.split("-")
        if len(parts) == 2 and parts[0] and parts[1]:
            return f"{parts[0].lower()}-{parts[1].lower()}"
    try:
        base, quote = parse_symbol(s_up.replace("-", ""))
        return f"{base.lower()}-{quote.lower()}"
    except Exception:
        # 对于无法解析的符号（例如测试用的 NONEXISTENT），做一个保守的降级：
        # - 统一小写
        # - 将分隔符规范为连字符
        # 不抛错，保证上层 API 能返回空 DataFrame 而不是异常。
        return s_up.lower()

def to_exchange_symbol(symbol: str) -> str:
    """
    将系统内部格式 btc-usdt 转为交易所格式 BTCUSDT（大写无连字符）
    支持输入：BTCUSDT / BTC-USDT / btc-usdt
    """
    s = (symbol or "").strip()
    if not s:
        return s
    s = s.replace("_", "-").replace("/", "-")
    s_up = s.upper()
    if "-" in s_up:
        s_up = s_up.replace("-", "")
    return format_symbol(s_up)


def parse_symbol(symbol: str) -> tuple[str, str]:
    """
    解析交易对符号，返回基础资产和报价资产
    例如: BTCUSDT -> (BTC, USDT)
    """
    symbol = format_symbol(symbol)
    if symbol.endswith('USDT'):
        base = symbol[:-4]
        quote = 'USDT'
    elif symbol.endswith('BUSD'):
        base = symbol[:-4]
        quote = 'BUSD'
    else:
        # 处理其他格式，如ETHBTC
        if 'BTC' in symbol and symbol != 'BTCUSDT':
            parts = symbol.split('BTC')
            if len(parts) == 2 and parts[1] == '':
                base = parts[0]
                quote = 'BTC'
            else:
                raise ValueError(f"Unknown symbol format: {symbol}")
        else:
            raise ValueError(f"Unknown symbol format: {symbol}")
    
    return base, quote


def ensure_directory(path: str):
    """确保目录存在"""
    from pathlib import Path
    Path(path).mkdir(parents=True, exist_ok=True)


def safe_float(value: any, default: float = 0.0) -> float:
    """安全转换为float"""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def safe_int(value: any, default: int = 0) -> int:
    """安全转换为int"""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def clean_json_value(value):
    """
    清理JSON值，将NaN和Inf替换为None或0
    
    Args:
        value: 要清理的值
    
    Returns:
        清理后的值
    """
    import math
    import pandas as pd
    
    if isinstance(value, (float, int)):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    elif isinstance(value, pd.Series):
        # 处理pandas Series
        return value.replace([float('nan'), float('inf'), float('-inf')], None).tolist()
    elif isinstance(value, dict):
        return {k: clean_json_value(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [clean_json_value(item) for item in value]
    else:
        return value


def sanitize_for_json(obj):
    """
    清理对象中的所有NaN和Inf值，使其可以安全序列化为JSON
    
    Args:
        obj: 要清理的对象（dict, list等）
    
    Returns:
        清理后的对象
    """
    import math
    import pandas as pd
    
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, (float, int)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, pd.Series):
        return sanitize_for_json(obj.tolist())
    elif isinstance(obj, pd.DataFrame):
        return sanitize_for_json(obj.to_dict('records'))
    else:
        return obj