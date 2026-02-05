"""
Execution method selector

Goal:
- Keep strategy layer transparent: strategy only outputs target weight vector.
- Execution layer dynamically chooses execution method per symbol based on:
  - order/weight characteristics (order notional vs equity, reduce-only, etc.)
  - symbol historical features (volatility, dollar-volume, vwap deviation)
  - account state (available balance ratio)
  - performance state (return / drawdown) -> may trigger cancel_all_orders + stop

This module is deterministic and side-effect-free. It is used by both live execution
and backtest to ensure consistency.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Any


METHOD_MARKET = "MARKET"
METHOD_LIMIT = "LIMIT"
METHOD_TWAP = "TWAP"
METHOD_VWAP = "VWAP"
METHOD_CANCEL_ALL_ORDERS = "CANCEL_ALL_ORDERS"


@dataclass(frozen=True)
class PerformanceSnapshot:
    """
    A minimal performance snapshot (percentage values are fractions, e.g. 0.05 == 5%).
    """

    total_return_pct: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    data_points: int = 0


@dataclass(frozen=True)
class AccountSnapshot:
    """
    Percentage values are fractions, e.g. 0.1 == 10%.
    """

    total_wallet_balance: Optional[float] = None
    available_balance: Optional[float] = None

    @property
    def available_balance_pct(self) -> Optional[float]:
        if self.total_wallet_balance is None:
            return None
        if self.total_wallet_balance <= 0:
            return None
        if self.available_balance is None:
            return None
        return float(self.available_balance) / float(self.total_wallet_balance)


@dataclass(frozen=True)
class SymbolFeatures:
    """
    Percentage values are fractions, e.g. 0.02 == 2%.
    """

    volatility_pct: Optional[float] = None  # std of returns
    avg_dolvol: Optional[float] = None  # average dollar volume (quote volume) per bar
    vwap_deviation_pct: Optional[float] = None  # mean(|close-vwap|/close)


@dataclass(frozen=True)
class ExecutionDecision:
    """
    For ORDER execution:
    - method is one of MARKET/LIMIT/TWAP/VWAP
    - For LIMIT, limit_price is required.

    For GLOBAL action:
    - method may be CANCEL_ALL_ORDERS
    """

    method: str
    limit_price: Optional[float] = None
    reason: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class ExecutionMethodSelector:
    """
    Config-driven selector.

    IMPORTANT:
    - No hardcoded thresholds. All thresholds are read from config and must exist in default.yaml.
    - Percent thresholds are expressed as fractions, e.g. 0.05 means 5%.
    """

    def __init__(self, cfg):
        self._cfg = cfg

    def enabled(self) -> bool:
        return bool(self._cfg.get("execution.method_selection.enabled"))

    def default_method(self) -> str:
        default = self._cfg.get("execution.method_selection.default_method")
        if not default:
            raise ValueError("Missing config key: execution.method_selection.default_method")
        return str(default).upper()

    def select_global_action(
        self,
        account: AccountSnapshot,
        performance: Optional[PerformanceSnapshot],
    ) -> Optional[ExecutionDecision]:
        """
        Decide whether to trigger global cancel_all_orders + stop.
        """
        risk_cfg = self._cfg.get("execution.method_selection.risk_control")
        if not risk_cfg or not isinstance(risk_cfg, dict):
            return None
        if not bool(risk_cfg.get("enabled", False)):
            return None

        # Require keys (no numeric fallbacks in code)
        min_points = risk_cfg.get("min_data_points")
        if min_points is None:
            raise ValueError("Missing config key: execution.method_selection.risk_control.min_data_points")
        stop_loss_dd_pct = risk_cfg.get("stop_loss_max_drawdown_pct")
        if stop_loss_dd_pct is None:
            raise ValueError("Missing config key: execution.method_selection.risk_control.stop_loss_max_drawdown_pct")
        take_profit_ret_pct = risk_cfg.get("take_profit_total_return_pct")
        if take_profit_ret_pct is None:
            raise ValueError("Missing config key: execution.method_selection.risk_control.take_profit_total_return_pct")

        if performance is None or performance.data_points < int(min_points):
            return None

        # Note: equity tracker returns drawdown pct possibly negative; normalize to absolute loss fraction.
        dd = performance.max_drawdown_pct
        if dd is not None:
            dd_loss = abs(float(dd))
            if dd_loss >= float(stop_loss_dd_pct):
                return ExecutionDecision(
                    method=METHOD_CANCEL_ALL_ORDERS,
                    reason=f"risk_control.stop_loss triggered: max_drawdown_pct={dd_loss:.6f} >= {float(stop_loss_dd_pct):.6f}",
                )

        ret = performance.total_return_pct
        if ret is not None:
            if float(ret) >= float(take_profit_ret_pct):
                return ExecutionDecision(
                    method=METHOD_CANCEL_ALL_ORDERS,
                    reason=f"risk_control.take_profit triggered: total_return_pct={float(ret):.6f} >= {float(take_profit_ret_pct):.6f}",
                )

        return None

    def select_for_order(
        self,
        symbol: str,
        side: str,
        reduce_only: bool,
        order_notional: Optional[float],
        order_notional_pct_equity: Optional[float],
        liquidity_impact_pct: Optional[float],
        features: Optional[SymbolFeatures],
        account: AccountSnapshot,
    ) -> ExecutionDecision:
        """
        Select per-order execution method.

        Inputs percent values are fractions.
        """

        if not self.enabled():
            return ExecutionDecision(method=self.default_method(), reason="method_selection.disabled")

        ms_cfg = self._cfg.get("execution.method_selection")
        if not ms_cfg or not isinstance(ms_cfg, dict):
            raise ValueError("Missing config section: execution.method_selection")

        thresholds = ms_cfg.get("thresholds")
        if not thresholds or not isinstance(thresholds, dict):
            raise ValueError("Missing config section: execution.method_selection.thresholds")

        # Required threshold keys (no numeric fallbacks)
        small_order_pct = thresholds.get("small_order_notional_pct_equity")
        if small_order_pct is None:
            raise ValueError("Missing config key: execution.method_selection.thresholds.small_order_notional_pct_equity")
        large_order_pct = thresholds.get("large_order_notional_pct_equity")
        if large_order_pct is None:
            raise ValueError("Missing config key: execution.method_selection.thresholds.large_order_notional_pct_equity")
        high_vol_pct = thresholds.get("high_volatility_pct")
        if high_vol_pct is None:
            raise ValueError("Missing config key: execution.method_selection.thresholds.high_volatility_pct")
        high_impact_pct = thresholds.get("high_liquidity_impact_pct")
        if high_impact_pct is None:
            raise ValueError("Missing config key: execution.method_selection.thresholds.high_liquidity_impact_pct")
        low_avail_pct = thresholds.get("low_available_balance_pct")
        if low_avail_pct is None:
            raise ValueError("Missing config key: execution.method_selection.thresholds.low_available_balance_pct")

        # If margin is tight, prefer MARKET for reduce_only orders to quickly de-risk.
        avail_pct = account.available_balance_pct
        if reduce_only and avail_pct is not None and avail_pct <= float(low_avail_pct):
            return ExecutionDecision(
                method=METHOD_MARKET,
                reason=f"low_available_balance_pct={avail_pct:.6f} <= {float(low_avail_pct):.6f} and reduce_only",
            )

        vol = features.volatility_pct if features else None
        impact = liquidity_impact_pct

        # Large / illiquid / high volatility -> TWAP/VWAP (if enabled)
        if (order_notional_pct_equity is not None and float(order_notional_pct_equity) >= float(large_order_pct)) or (
            impact is not None and float(impact) >= float(high_impact_pct)
        ) or (vol is not None and float(vol) >= float(high_vol_pct)):
            vwap_cfg = ms_cfg.get("vwap", {}) if isinstance(ms_cfg.get("vwap", {}), dict) else {}
            twap_cfg = ms_cfg.get("twap", {}) if isinstance(ms_cfg.get("twap", {}), dict) else {}

            vwap_enabled = bool(vwap_cfg.get("enabled", True))
            twap_enabled = bool(twap_cfg.get("enabled", True))

            # Prefer VWAP when we have volume data and VWAP is enabled.
            if vwap_enabled and features and features.avg_dolvol is not None:
                return ExecutionDecision(
                    method=METHOD_VWAP,
                    reason="large_or_illiquid_or_high_vol -> VWAP",
                )
            if twap_enabled:
                return ExecutionDecision(
                    method=METHOD_TWAP,
                    reason="large_or_illiquid_or_high_vol -> TWAP",
                )
            return ExecutionDecision(method=METHOD_MARKET, reason="large_or_illiquid_or_high_vol but twap/vwap disabled -> MARKET")

        # Small orders -> LIMIT if enabled
        if order_notional_pct_equity is not None and float(order_notional_pct_equity) <= float(small_order_pct):
            limit_cfg = ms_cfg.get("limit", {}) if isinstance(ms_cfg.get("limit", {}), dict) else {}
            if bool(limit_cfg.get("enabled", True)):
                return ExecutionDecision(method=METHOD_LIMIT, reason="small_order -> LIMIT")

        # Default -> MARKET
        return ExecutionDecision(method=METHOD_MARKET, reason="default -> MARKET")
