"""
网络工具模块
提供统一的网络错误处理、重试机制和日志记录
"""
import asyncio
import time
from typing import Callable, TypeVar, Optional, Any
from functools import wraps
import aiohttp
from websockets.exceptions import ConnectionClosed, WebSocketException
from collections import deque
import json

from .logger import get_logger
from .config import config

logger = get_logger('network_utils')

T = TypeVar('T')


class NetworkError(Exception):
    """网络错误基类"""
    pass


class ConnectionTimeoutError(NetworkError):
    """连接超时错误"""
    pass


class RetryableNetworkError(NetworkError):
    """可重试的网络错误"""
    pass


class IPBannedError(NetworkError):
    """IP被封禁错误（418）"""
    def __init__(self, message: str, banned_until: Optional[int] = None):
        super().__init__(message)
        self.banned_until = banned_until  # 解封时间戳（毫秒）


def retry_async(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    log_retries: bool = True
):
    """
    异步函数重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 初始延迟（秒）
        backoff: 延迟倍数
        exceptions: 需要重试的异常类型
        log_retries: 是否记录重试日志
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        if log_retries:
                            logger.warning(
                                f"网络操作失败 (尝试 {attempt + 1}/{max_retries + 1}): {func.__name__}, "
                                f"错误: {type(e).__name__}: {e}, "
                                f"{current_delay:.1f}秒后重试..."
                            )
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"网络操作最终失败 (已重试{max_retries}次): {func.__name__}, "
                            f"错误: {type(e).__name__}: {e}",
                            exc_info=True
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


async def safe_http_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    max_retries: int = 3,
    timeout: float = 30.0,
    return_json: bool = False,
    use_rate_limit: bool = True,
    **kwargs
) -> Any:
    """
    安全的HTTP请求，带重试机制和请求限流
    
    Args:
        session: aiohttp会话
        method: HTTP方法
        url: 请求URL
        max_retries: 最大重试次数
        timeout: 超时时间（秒）
        return_json: 是否返回JSON数据
        use_rate_limit: 是否使用请求限流（默认True）
        **kwargs: 其他aiohttp请求参数
    
    Returns:
        响应对象或JSON数据
    
    Raises:
        NetworkError: 网络错误
        ConnectionTimeoutError: 连接超时
        IPBannedError: IP被封禁
    """
    last_exception = None
    delay = 1.0
    rate_limiter = get_rate_limiter() if use_rate_limit else None
    
    for attempt in range(max_retries + 1):
        try:
            # 请求限流：在发送请求前等待
            if rate_limiter:
                await rate_limiter.acquire()
            
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            # 添加User-Agent头，避免某些服务器拒绝请求
            headers = dict(kwargs.get('headers', {}))
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            kwargs['headers'] = headers
            
            async with session.request(
                method=method,
                url=url,
                timeout=timeout_obj,
                **kwargs
            ) as response:
                # 检查HTTP状态码
                if response.status >= 500:
                    # 服务器错误，可重试
                    error_text = await response.text()
                    raise RetryableNetworkError(
                        f"服务器错误 {response.status}: {error_text[:200]}"
                    )
                elif response.status == 429:
                    # 限流，可重试
                    retry_after = response.headers.get('Retry-After', '60')
                    logger.warning(
                        f"API限流 (429), Retry-After: {retry_after}秒, "
                        f"尝试 {attempt + 1}/{max_retries + 1}"
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(float(retry_after) if retry_after.isdigit() else delay)
                        delay *= 2
                        continue
                    raise NetworkError(f"API限流，已重试{max_retries}次")
                elif response.status == 418:
                    # IP被封禁（418），需要等待解封
                    error_text = await response.text()
                    try:
                        error_data = json.loads(error_text)
                        msg = error_data.get('msg', '')
                        # 尝试从错误消息中提取解封时间
                        # Binance错误格式: "Way too many requests; IP(...) banned until 1769587194303"
                        banned_until = None
                        if 'banned until' in msg.lower():
                            import re
                            match = re.search(r'banned until (\d+)', msg, re.IGNORECASE)
                            if match:
                                banned_until = int(match.group(1))
                    except:
                        # 如果解析失败，使用默认值（1小时后解封）
                        banned_until = int(time.time() * 1000) + 3600000
                    
                    if rate_limiter and banned_until:
                        rate_limiter.set_ip_banned(banned_until)
                    
                    error_msg = f"IP被封禁 (418): {msg}"
                    if banned_until:
                        error_msg += f", 解封时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(banned_until / 1000))}"
                    
                    logger.error(error_msg)
                    raise IPBannedError(error_msg, banned_until)
                elif response.status == 408:
                    # 请求超时（408），可重试
                    error_text = await response.text()
                    logger.warning(
                        f"请求超时 (408), 尝试 {attempt + 1}/{max_retries + 1}: {error_text[:200]}"
                    )
                    if attempt < max_retries:
                        raise RetryableNetworkError(
                            f"请求超时 {response.status}: {error_text[:200]}"
                        )
                    raise NetworkError(f"请求超时，已重试{max_retries}次")
                elif response.status == 403:
                    # 403错误可能是临时的限流或权限问题，可以重试
                    error_text = await response.text()
                    if attempt < max_retries:
                        logger.warning(
                            f"403 Forbidden (尝试 {attempt + 1}/{max_retries + 1}): {url}, "
                            f"可能是临时限流，{delay:.1f}秒后重试..."
                        )
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue
                    raise NetworkError(
                        f"客户端错误 403: {error_text[:200]}"
                    )
                elif response.status >= 400:
                    # 其他客户端错误，不可重试
                    error_text = await response.text()
                    raise NetworkError(
                        f"客户端错误 {response.status}: {error_text[:200]}"
                    )
                
                # 根据return_json参数返回数据或response对象
                if return_json:
                    data = await response.json()
                    return data
                else:
                    # 返回response对象（调用者需要在使用时保持上下文）
                    return response
                
        except asyncio.TimeoutError as e:
            last_exception = ConnectionTimeoutError(f"请求超时: {url}")
            if attempt < max_retries:
                logger.warning(
                    f"HTTP请求超时 (尝试 {attempt + 1}/{max_retries + 1}): {url}, "
                    f"{delay:.1f}秒后重试..."
                )
                await asyncio.sleep(delay)
                delay *= 2
            else:
                logger.error(f"HTTP请求最终超时: {url}")
        except aiohttp.ClientError as e:
            last_exception = NetworkError(f"HTTP客户端错误: {e}")
            if attempt < max_retries:
                logger.warning(
                    f"HTTP客户端错误 (尝试 {attempt + 1}/{max_retries + 1}): {url}, "
                    f"错误: {e}, {delay:.1f}秒后重试..."
                )
                await asyncio.sleep(delay)
                delay *= 2
            else:
                logger.error(f"HTTP请求最终失败: {url}, 错误: {e}", exc_info=True)
        except IPBannedError as e:
            # IP被封禁，等待解封
            last_exception = e
            if e.banned_until:
                current_ms = int(time.time() * 1000)
                if current_ms < e.banned_until:
                    wait_seconds = (e.banned_until - current_ms) / 1000.0
                    logger.error(
                        f"IP被封禁，等待 {wait_seconds:.1f} 秒直到解封 "
                        f"(解封时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(e.banned_until / 1000))})"
                    )
                    await asyncio.sleep(wait_seconds + 1)  # 多等1秒确保解封
                    # 解封后，重置限流器状态
                    if rate_limiter:
                        rate_limiter.ip_banned_until = None
                    # 继续重试
                    if attempt < max_retries:
                        continue
            raise last_exception
        except RetryableNetworkError as e:
            last_exception = e
            if attempt < max_retries:
                logger.warning(
                    f"可重试的网络错误 (尝试 {attempt + 1}/{max_retries + 1}): {url}, "
                    f"错误: {e}, {delay:.1f}秒后重试..."
                )
                await asyncio.sleep(delay)
                delay *= 2
            else:
                logger.error(f"HTTP请求最终失败: {url}, 错误: {e}")
    
    raise last_exception


class RateLimiter:
    """
    请求限流器
    使用令牌桶算法限制请求频率，防止触发Binance API限流
    """
    
    def __init__(
        self,
        max_requests_per_second: float = 10.0,
        max_requests_per_minute: int = 1200,
        burst_size: int = 20
    ):
        """
        初始化限流器
        
        Args:
            max_requests_per_second: 每秒最大请求数（默认10，Binance推荐）
            max_requests_per_minute: 每分钟最大请求数（默认1200，Binance限制）
            burst_size: 突发请求允许数量
        """
        self.max_requests_per_second = max_requests_per_second
        self.max_requests_per_minute = max_requests_per_minute
        self.burst_size = burst_size
        
        # 令牌桶：每秒补充的令牌数
        self.tokens_per_second = max_requests_per_second
        self.max_tokens = burst_size
        
        # 当前令牌数
        self.tokens = float(burst_size)
        
        # 请求时间记录（用于分钟级限流）
        self.request_times: deque = deque()
        
        # 最后更新时间
        self.last_update = time.time()
        
        # 锁
        self.lock = asyncio.Lock()
        
        # IP封禁状态
        self.ip_banned_until: Optional[int] = None  # 解封时间戳（毫秒）
    
    async def acquire(self):
        """
        获取请求许可（等待直到可以发送请求）
        
        如果IP被封禁，会等待到解封时间
        """
        async with self.lock:
            current_time = time.time()
            
            # 检查IP封禁状态
            if self.ip_banned_until is not None:
                current_ms = int(current_time * 1000)
                if current_ms < self.ip_banned_until:
                    # 仍然被封禁，计算等待时间
                    wait_seconds = (self.ip_banned_until - current_ms) / 1000.0
                    logger.warning(
                        f"IP被封禁，等待 {wait_seconds:.1f} 秒直到解封 "
                        f"(解封时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.ip_banned_until / 1000))})"
                    )
                    await asyncio.sleep(wait_seconds + 1)  # 多等1秒确保解封
                    self.ip_banned_until = None
                else:
                    # 已解封
                    self.ip_banned_until = None
            
            # 更新令牌（基于时间流逝）
            time_passed = current_time - self.last_update
            self.tokens = min(
                self.max_tokens,
                self.tokens + time_passed * self.tokens_per_second
            )
            self.last_update = current_time
            
            # 清理超过1分钟的请求记录
            one_minute_ago = current_time - 60
            while self.request_times and self.request_times[0] < one_minute_ago:
                self.request_times.popleft()
            
            # 检查分钟级限流
            if len(self.request_times) >= self.max_requests_per_minute:
                # 需要等待到最早的请求超过1分钟
                oldest_request_time = self.request_times[0]
                wait_until = oldest_request_time + 60
                wait_seconds = wait_until - current_time
                if wait_seconds > 0:
                    logger.warning(
                        f"达到分钟级限流 ({len(self.request_times)}/{self.max_requests_per_minute})，"
                        f"等待 {wait_seconds:.1f} 秒..."
                    )
                    await asyncio.sleep(wait_seconds)
                    # 重新计算
                    current_time = time.time()
                    while self.request_times and self.request_times[0] < current_time - 60:
                        self.request_times.popleft()
            
            # 检查秒级限流（令牌桶）
            if self.tokens < 1.0:
                # 需要等待令牌补充
                tokens_needed = 1.0 - self.tokens
                wait_seconds = tokens_needed / self.tokens_per_second
                if wait_seconds > 0.1:  # 只有等待时间超过0.1秒才等待
                    logger.debug(
                        f"令牌不足，等待 {wait_seconds:.2f} 秒补充令牌 "
                        f"(当前令牌: {self.tokens:.2f}/{self.max_tokens})"
                    )
                    await asyncio.sleep(wait_seconds)
                    # 更新令牌
                    current_time = time.time()
                    time_passed = current_time - self.last_update
                    self.tokens = min(
                        self.max_tokens,
                        self.tokens + time_passed * self.tokens_per_second
                    )
                    self.last_update = current_time
            
            # 消耗令牌
            self.tokens -= 1.0
            self.request_times.append(current_time)
    
    def set_ip_banned(self, banned_until_ms: int):
        """
        设置IP封禁状态
        
        Args:
            banned_until_ms: 解封时间戳（毫秒）
        """
        self.ip_banned_until = banned_until_ms
        logger.error(
            f"IP被封禁，解封时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(banned_until_ms / 1000))}"
        )


# 全局限流器实例
_global_rate_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """获取全局限流器实例"""
    global _global_rate_limiter
    if _global_rate_limiter is None:
        # 从配置读取限流参数
        max_rps = config.get('network.max_requests_per_second', 10.0)
        max_rpm = config.get('network.max_requests_per_minute', 1200)
        burst = config.get('network.burst_size', 20)
        _global_rate_limiter = RateLimiter(
            max_requests_per_second=max_rps,
            max_requests_per_minute=max_rpm,
            burst_size=burst
        )
        logger.info(
            f"初始化请求限流器: {max_rps} req/s, {max_rpm} req/min, burst={burst}"
        )
    return _global_rate_limiter


def log_network_error(operation: str, error: Exception, context: Optional[dict] = None):
    """
    记录网络错误的详细信息
    
    Args:
        operation: 操作名称
        error: 错误对象
        context: 上下文信息（可选）
    """
    import socket
    
    error_type = type(error).__name__
    error_msg = str(error)
    
    # 判断错误类型
    if isinstance(error, (asyncio.TimeoutError, ConnectionTimeoutError)):
        level = "warning"
        error_category = "超时"
    elif isinstance(error, ConnectionClosed):
        level = "warning"
        error_category = "连接关闭"
    elif isinstance(error, WebSocketException):
        level = "warning"
        error_category = "WebSocket异常"
    elif isinstance(error, aiohttp.ClientError):
        level = "error"
        error_category = "HTTP客户端错误"
    elif isinstance(error, socket.gaierror):
        # DNS解析失败，通常是网络暂时性问题，可重试
        level = "warning"
        error_category = "DNS解析失败"
    elif isinstance(error, OSError) and "getaddrinfo" in str(error):
        # DNS解析失败（OSError形式）
        level = "warning"
        error_category = "DNS解析失败"
    else:
        level = "error"
        error_category = "未知网络错误"
    
    log_msg = (
        f"[网络错误] 操作: {operation}, 类型: {error_category}, "
        f"错误: {error_type}: {error_msg}"
    )
    
    if context:
        log_msg += f", 上下文: {context}"
    
    if level == "warning":
        logger.warning(log_msg)
    else:
        logger.error(log_msg, exc_info=True)
