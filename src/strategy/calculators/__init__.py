"""
Calculators（因子）模块。

研究员可以在此目录下创建独立的calculator文件，系统会自动发现并加载。

目录结构：
- calculators/
  - __init__.py (本文件，提供加载机制)
  - base.py (基类和工具，可选)
  - researcher1_factor1.py (研究员1的因子1)
  - researcher2_factor2.py (研究员2的因子2)
  - ...

每个calculator文件应：
1. 继承 AlphaCalculatorBase
2. 实现 run(view: AlphaDataView) -> Dict[str, float]
3. 设置 name 和 mutates_inputs 属性
4. 可选：导出 CALCULATOR_CLASS 或 CALCULATOR_INSTANCE

加载机制：
- 通过配置文件 strategy.calculators.enabled 指定要加载的calculators
- 支持通配符和模块路径（如 "researcher1.*" 或 "calculators.researcher1_factor1"）
- 如果未配置，则加载所有在 calculators 目录下的 calculator
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import List, Optional, Sequence, Type

from ..calculator import AlphaCalculatorBase
from ...common.config import config
from ...common.logger import get_logger

logger = get_logger("calculators_loader")

# Calculators目录路径
CALCULATORS_DIR = Path(__file__).parent


def _is_calculator_class(obj) -> bool:
    """检查对象是否是calculator类（继承自AlphaCalculatorBase但不是基类本身）。"""
    return (
        inspect.isclass(obj)
        and issubclass(obj, AlphaCalculatorBase)
        and obj is not AlphaCalculatorBase
        and obj.__module__.startswith("src.strategy.calculators")
    )


def _is_calculator_instance(obj) -> bool:
    """检查对象是否是calculator实例。"""
    return isinstance(obj, AlphaCalculatorBase) and obj.__class__.__module__.startswith(
        "src.strategy.calculators"
    )


def _discover_calculators_in_module(module_name: str) -> List[AlphaCalculatorBase]:
    """
    从指定模块中发现所有calculators。

    返回:
        List[AlphaCalculatorBase]: 发现的calculator实例列表
    """
    calculators: List[AlphaCalculatorBase] = []

    try:
        module = importlib.import_module(module_name)
    except Exception as e:
        logger.warning(f"无法导入模块 {module_name}: {e}")
        return calculators

    # 查找模块中导出的 CALCULATOR_CLASS 或 CALCULATOR_INSTANCE
    if hasattr(module, "CALCULATOR_INSTANCE"):
        calc = getattr(module, "CALCULATOR_INSTANCE")
        if _is_calculator_instance(calc):
            calculators.append(calc)
            logger.debug(f"从 {module_name} 加载calculator实例: {calc.name}")

    if hasattr(module, "CALCULATOR_CLASS"):
        calc_class = getattr(module, "CALCULATOR_CLASS")
        if _is_calculator_class(calc_class):
            try:
                instance = calc_class()
                calculators.append(instance)
                logger.debug(f"从 {module_name} 加载calculator类: {instance.name}")
            except Exception as e:
                logger.warning(f"无法实例化 {calc_class.__name__}: {e}")

    # 如果已经通过CALCULATOR_INSTANCE或CALCULATOR_CLASS加载了，就不再查找类
    # 避免重复加载
    if calculators:
        return calculators

    # 查找所有AlphaCalculatorBase的子类
    for name, obj in inspect.getmembers(module, _is_calculator_class):
        try:
            instance = obj()
            calculators.append(instance)
            logger.debug(f"从 {module_name} 发现calculator类: {instance.name}")
        except Exception as e:
            logger.warning(f"无法实例化 {name}: {e}")

    # 查找所有AlphaCalculatorBase的实例（如果还没有通过CALCULATOR_INSTANCE加载）
    if not calculators:
        for name, obj in inspect.getmembers(module, _is_calculator_instance):
            calculators.append(obj)
            logger.debug(f"从 {module_name} 发现calculator实例: {obj.name}")

    return calculators


def _discover_all_calculators() -> List[AlphaCalculatorBase]:
    """
    自动发现calculators目录下的所有calculators。

    返回:
        List[AlphaCalculatorBase]: 所有发现的calculator实例
    """
    calculators: List[AlphaCalculatorBase] = []
    calculators_pkg = "src.strategy.calculators"

    # 遍历calculators包下的所有模块
    try:
        package = importlib.import_module(calculators_pkg)
        package_path = Path(package.__file__).parent if hasattr(package, "__file__") else CALCULATORS_DIR

        for finder, name, ispkg in pkgutil.iter_modules([str(package_path)]):
            if name == "__init__" or ispkg:
                continue

            module_name = f"{calculators_pkg}.{name}"
            try:
                found = _discover_calculators_in_module(module_name)
                calculators.extend(found)
            except Exception as e:
                logger.warning(f"加载模块 {module_name} 时出错: {e}")

    except Exception as e:
        logger.error(f"发现calculators时出错: {e}", exc_info=True)

    return calculators


def _load_calculators_from_config() -> List[AlphaCalculatorBase]:
    """
    从配置文件加载指定的calculators。

    配置格式（config/default.yaml）:
        strategy:
          calculators:
            enabled:
              - "researcher1_factor1"  # 模块名（不含calculators前缀）
              - "calculators.researcher2_factor2"  # 完整模块路径
              - "researcher3.*"  # 通配符（暂不支持，未来可扩展）

    返回:
        List[AlphaCalculatorBase]: 加载的calculator实例列表
    """
    calculators: List[AlphaCalculatorBase] = []
    enabled = config.get("strategy.calculators.enabled", None)

    if enabled is None:
        # 未配置，返回空列表（将由_discover_all_calculators处理）
        return calculators

    if not isinstance(enabled, (list, tuple)):
        logger.warning(f"strategy.calculators.enabled 应为列表，当前为 {type(enabled)}")
        return calculators

    calculators_pkg = "src.strategy.calculators"

    for item in enabled:
        if not isinstance(item, str):
            logger.warning(f"跳过非字符串配置项: {item}")
            continue

        item = item.strip()
        if not item:
            continue

        # 处理完整模块路径
        if item.startswith("calculators.") or item.startswith("src.strategy.calculators."):
            module_name = item
        elif "." in item:
            # 假设是完整模块路径
            module_name = item
        else:
            # 假设是calculators下的模块名
            module_name = f"{calculators_pkg}.{item}"

        try:
            found = _discover_calculators_in_module(module_name)
            if found:
                calculators.extend(found)
                logger.info(f"从配置加载calculator: {module_name} -> {len(found)}个")
            else:
                logger.warning(f"配置的calculator未找到: {module_name}")
        except Exception as e:
            logger.error(f"加载配置的calculator失败 {module_name}: {e}", exc_info=True)

    return calculators


def load_calculators() -> Sequence[AlphaCalculatorBase]:
    """
    加载所有calculators。

    优先级：
    1. 如果配置了 strategy.calculators.enabled，只加载配置的
    2. 否则，自动发现calculators目录下的所有calculators

    返回:
        Sequence[AlphaCalculatorBase]: 加载的calculator实例列表
    """
    calculators: List[AlphaCalculatorBase] = []

    # 尝试从配置加载
    config_calcs = _load_calculators_from_config()
    if config_calcs:
        calculators.extend(config_calcs)
        logger.info(f"从配置加载了 {len(config_calcs)} 个calculators")
    else:
        # 自动发现
        discovered = _discover_all_calculators()
        calculators.extend(discovered)
        logger.info(f"自动发现了 {len(discovered)} 个calculators")

    # 去重（按name）
    seen_names = set()
    unique_calculators: List[AlphaCalculatorBase] = []
    for calc in calculators:
        if calc.name not in seen_names:
            seen_names.add(calc.name)
            unique_calculators.append(calc)
        else:
            logger.warning(f"发现重复的calculator名称: {calc.name}，跳过")

    logger.info(f"最终加载 {len(unique_calculators)} 个calculators: {[c.name for c in unique_calculators]}")
    return unique_calculators
