"""
测试目标持仓生成器
"""
import pytest
import pandas as pd
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import os

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategy.position_generator import PositionGenerator, get_position_generator


def test_position_generator_initialization():
    """测试持仓生成器初始化"""
    generator = PositionGenerator()
    
    assert generator.positions_dir.exists()
    
    print("✓ Position generator initialized successfully")


def test_save_target_positions():
    """测试保存目标持仓"""
    generator = PositionGenerator()
    
    # 创建测试持仓数据
    target_positions = {
        'account1': pd.DataFrame([
            {
                'symbol': 'BTCUSDT',
                'target_position': 0.1,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'latest_price': 50000.0,
                'short_ma': 50100.0,
                'long_ma': 49900.0,
            },
            {
                'symbol': 'ETHUSDT',
                'target_position': -0.05,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'latest_price': 3000.0,
                'short_ma': 2990.0,
                'long_ma': 3010.0,
            },
        ])
    }
    
    # 保存
    file_paths = generator.save_target_positions(target_positions)
    
    # 验证文件已创建
    assert 'account1' in file_paths
    file_path = Path(file_paths['account1'])
    assert file_path.exists()
    
    # 验证文件内容
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    assert data['account_id'] == 'account1'
    assert 'positions' in data
    assert len(data['positions']) == 2
    assert data['positions'][0]['symbol'] == 'BTCUSDT'
    
    print(f"✓ Target positions saved to {file_path}")
    
    # 清理
    file_path.unlink()


def test_load_target_positions():
    """测试加载目标持仓"""
    generator = PositionGenerator()
    
    # 先保存一个文件
    target_positions = {
        'account1': pd.DataFrame([
            {
                'symbol': 'BTCUSDT',
                'target_position': 0.1,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            },
        ])
    }
    
    file_paths = generator.save_target_positions(target_positions)
    file_path = file_paths['account1']
    
    # 加载
    loaded_data = generator.load_target_positions(file_path)
    
    # 验证
    assert loaded_data['account_id'] == 'account1'
    assert 'positions' in loaded_data
    assert len(loaded_data['positions']) == 1
    assert loaded_data['positions'][0]['symbol'] == 'BTCUSDT'
    
    print("✓ Target positions loaded successfully")
    
    # 清理
    Path(file_path).unlink()


def test_save_empty_positions():
    """测试保存空持仓"""
    generator = PositionGenerator()
    
    target_positions = {
        'account1': pd.DataFrame(columns=['symbol', 'target_position', 'timestamp']),
    }
    
    file_paths = generator.save_target_positions(target_positions)
    
    # 空持仓不应该创建文件
    assert 'account1' not in file_paths or not Path(file_paths.get('account1', '')).exists()
    
    print("✓ Empty positions handled correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Position Generator")
    print("=" * 60)
    
    test_position_generator_initialization()
    test_save_target_positions()
    test_load_target_positions()
    test_save_empty_positions()
    
    print("\n" + "=" * 60)
    print("All position generator tests completed!")
    print("=" * 60)
