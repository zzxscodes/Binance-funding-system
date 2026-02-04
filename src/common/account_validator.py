"""
账户配置验证工具
验证账户配置是否完整，确保只有配置完整的账户才会被启动
"""
import os
from typing import Dict, List, Tuple, Optional
from ..common.config import config
from ..common.logger import get_logger

logger = get_logger('account_validator')


def validate_account_config(account_config: Dict, execution_mode: str) -> Tuple[bool, Optional[str]]:
    """
    验证账户配置是否完整
    
    Args:
        account_config: 账户配置字典
        execution_mode: 执行模式 ('testnet', 'mock', 'live')
    
    Returns:
        (is_valid, error_message): 如果配置有效返回(True, None)，否则返回(False, error_message)
    """
    account_id = account_config.get('account_id')
    if not account_id:
        return False, "Account config missing 'account_id'"
    
    if execution_mode == 'testnet':
        # Testnet模式需要API密钥
        api_key_env = f"{account_id.upper()}_TESTNET_API_KEY"
        api_secret_env = f"{account_id.upper()}_TESTNET_API_SECRET"
        
        # 检查环境变量
        api_key = os.getenv(api_key_env, '')
        api_secret = os.getenv(api_secret_env, '')
        
        # 如果环境变量没有，检查配置文件
        if not api_key:
            api_key = account_config.get('api_key', '')
        if not api_secret:
            api_secret = account_config.get('api_secret', '')
        
        # 去除可能的注释标记
        api_key = str(api_key).strip()
        api_secret = str(api_secret).strip()
        
        if not api_key or not api_secret:
            return False, (
                f"Account '{account_id}' in testnet mode is missing API keys. "
                f"Please configure {api_key_env}/{api_secret_env} env vars or add api_key/api_secret in config. "
                f"Alternatively, comment out this account if not needed."
            )
        
        # 检查是否是占位符值
        placeholder_values = ['your_account', 'your_', 'here', 'xxx', 'xxx_here']
        if any(placeholder in api_key.lower() or placeholder in api_secret.lower() for placeholder in placeholder_values):
            return False, (
                f"Account '{account_id}' in testnet mode has placeholder API keys. "
                f"Please replace with real API keys or comment out this account."
            )
    
    elif execution_mode == 'live':
        # Live模式需要API密钥
        api_key_env = f"{account_id.upper()}_API_KEY"
        api_secret_env = f"{account_id.upper()}_API_SECRET"
        
        # 检查环境变量
        api_key = os.getenv(api_key_env, '')
        api_secret = os.getenv(api_secret_env, '')
        
        # 如果环境变量没有，检查配置文件
        if not api_key:
            api_key = account_config.get('api_key', '')
        if not api_secret:
            api_secret = account_config.get('api_secret', '')
        
        # 去除可能的注释标记
        api_key = str(api_key).strip()
        api_secret = str(api_secret).strip()
        
        if not api_key or not api_secret:
            return False, (
                f"Account '{account_id}' in live mode is missing API keys. "
                f"Please configure {api_key_env}/{api_secret_env} env vars or add api_key/api_secret in config. "
                f"Alternatively, comment out this account if not needed."
            )
        
        # 检查是否是占位符值
        placeholder_values = ['your_account', 'your_', 'here', 'xxx', 'xxx_here']
        if any(placeholder in api_key.lower() or placeholder in api_secret.lower() for placeholder in placeholder_values):
            return False, (
                f"Account '{account_id}' in live mode has placeholder API keys. "
                f"Please replace with real API keys or comment out this account."
            )
    
    elif execution_mode == 'mock':
        # Mock模式需要账户余额信息
        total_wallet_balance = account_config.get('total_wallet_balance')
        available_balance = account_config.get('available_balance')
        
        if total_wallet_balance is None or available_balance is None:
            return False, (
                f"Account '{account_id}' in mock mode is missing balance information. "
                f"Please configure total_wallet_balance and available_balance, "
                f"or comment out this account if not needed."
            )
        
        try:
            total_wallet_balance = float(total_wallet_balance)
            available_balance = float(available_balance)
            
            if total_wallet_balance < 0 or available_balance < 0:
                return False, (
                    f"Account '{account_id}' in mock mode has invalid balance values. "
                    f"Balance must be non-negative."
                )
            
            if available_balance > total_wallet_balance:
                return False, (
                    f"Account '{account_id}' in mock mode has invalid balance: "
                    f"available_balance ({available_balance}) > total_wallet_balance ({total_wallet_balance})"
                )
        except (ValueError, TypeError):
            return False, (
                f"Account '{account_id}' in mock mode has invalid balance values. "
                f"Balance must be numeric."
            )
    
    return True, None


def get_valid_accounts(execution_mode: Optional[str] = None) -> List[str]:
    """
    获取配置完整且有效的账户列表
    
    Args:
        execution_mode: 执行模式，如果不指定则从配置读取
    
    Returns:
        有效的账户ID列表
    """
    if execution_mode is None:
        execution_mode = config.get('execution.mode', 'mock')
    
    # 获取对应模式的配置
    mode_config = config.get(f'execution.{execution_mode}', {})
    if not mode_config:
        logger.warning(f"Configuration for mode '{execution_mode}' not found")
        return []
    
    # 获取账户列表
    accounts_config = mode_config.get('accounts', [])
    valid_accounts = []
    
    for account_config in accounts_config:
        account_id = account_config.get('account_id')
        if not account_id:
            continue
        
        is_valid, error_msg = validate_account_config(account_config, execution_mode)
        if is_valid:
            valid_accounts.append(account_id)
        else:
            logger.warning(f"Skipping account '{account_id}': {error_msg}")
    
    return valid_accounts


def validate_all_accounts(execution_mode: Optional[str] = None) -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    验证所有账户配置，返回有效账户和无效账户列表
    
    Args:
        execution_mode: 执行模式，如果不指定则从配置读取
    
    Returns:
        (valid_accounts, invalid_accounts): 
        - valid_accounts: 有效的账户ID列表
        - invalid_accounts: [(account_id, error_message), ...] 无效账户列表
    """
    if execution_mode is None:
        execution_mode = config.get('execution.mode', 'mock')
    
    # 获取对应模式的配置
    mode_config = config.get(f'execution.{execution_mode}', {})
    if not mode_config:
        return [], [('', f"Configuration for mode '{execution_mode}' not found")]
    
    # 获取账户列表
    accounts_config = mode_config.get('accounts', [])
    valid_accounts = []
    invalid_accounts = []
    
    for account_config in accounts_config:
        account_id = account_config.get('account_id')
        if not account_id:
            invalid_accounts.append(('', "Account config missing 'account_id'"))
            continue
        
        is_valid, error_msg = validate_account_config(account_config, execution_mode)
        if is_valid:
            valid_accounts.append(account_id)
        else:
            invalid_accounts.append((account_id, error_msg))
    
    return valid_accounts, invalid_accounts
