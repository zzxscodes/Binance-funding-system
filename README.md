# Binance永续合约多空策略量化交易系统

基于Binance USDT本位永续合约的多空量化交易系统。采用多进程架构，实时接收归集逐笔成交数据，自行聚合生成多周期K线，支持多账户交易和多种订单执行策略。

## 核心特性

- **实时数据采集**: WebSocket实时接收归集逐笔成交数据（@aggTrade），自行聚合生成多周期K线
- **多周期支持**: 支持5min, 1h, 4h, 8h, 12h, 24h多周期K线聚合
- **多进程架构**: 5个独立进程，通过IPC和文件信号通信
- **多账户支持**: 支持配置多个交易账户，每个账户独立执行进程
- **Web监控界面**: 提供实时Web监控面板（http://localhost:8080），支持进程状态、账户信息、系统监控
- **多种订单策略**: 支持Market订单、TWAP（时间加权平均价格）、VWAP（成交量加权平均价格）
- **自动合约设置**: 自动应用保证金模式、持仓模式、杠杆倍数
- **Universe管理**: 每天23:55（北京时间）自动更新可交易资产列表，支持版本管理（v1, v2等）
- **溢价指数K线**: 自动采集和存储溢价指数K线数据
- **历史资金费率**: 自动采集和存储历史资金费率数据
- **防重复触发**: 策略计算防重复触发机制，确保计算完整性
- **数据完整性检查**: 多周期数据完整性检查，确保策略触发时机准确
- **无成交K线处理**: 自动生成无成交时段的K线数据
- **监控告警**: 实时监控持仓偏差、敞口、账户健康度
- **净值曲线追踪**: 自动记录账户净值曲线，支持历史数据查看和分析
- **持仓历史记录**: 自动记录所有开仓历史，用于策略分析和回测
- **策略报告生成**: 自动生成策略执行报告，包括交易统计、策略磨损分析等
- **统一策略API**: StrategyAPI封装数据层、系统层和执行层，简化策略开发
- **多因子架构**: 支持多研究员独立开发calculators（因子），系统自动发现、并发执行并求和
- **因子并发执行**: 默认使用线程池并发执行所有calculators，提升计算性能

## 系统架构

```
事件驱动协调进程 (Process 1) - IPC Server
           ↕ IPC通信
数据层进程 (Process 2) - Trade Collector + Kline Aggregator
           ↕ 文件信号
策略进程 (Process 3) - Position Calculator
           ↕ 文件信号
订单执行进程 (Process 4) - Order Manager (多实例，每账户一个)
           ↕ IPC通信
监控进程 (Process 5) - Metrics & Alerts + Web API
           ↕ HTTP API
Web监控界面 (http://localhost:8080)
```

**进程说明**：
1. **事件驱动协调进程**: 协调各进程间通信，作为IPC服务器
2. **数据层进程**: 实时接收归集逐笔成交（@aggTrade），聚合生成多周期K线，管理Universe，采集资金费率和溢价指数K线
3. **策略进程**: 执行策略计算，使用Alpha引擎并发运行多个calculators（因子），生成目标持仓文件，支持防重复触发机制
4. **订单执行进程**: 执行交易订单，支持Market/TWAP/VWAP多种订单策略，支持多账户（每个账户一个进程实例），自动记录持仓历史
5. **监控进程**: 监控系统状态、账户信息、持仓偏差、敞口等指标，记录净值曲线，生成策略报告，提供HTTP API和Web监控界面

## 快速开始

### 安装

**Windows**:
```cmd
setup_windows.bat
quant\Scripts\activate.bat
```

**Linux/macOS**:
```bash
sed -i 's/\r$//' setup_linux.sh
chmod +x setup_linux.sh
./setup_linux.sh
source quant/bin/activate
```

### 配置

编辑 `config/default.yaml`:

```yaml
strategy:
  history_days: 30           # 策略计算使用的历史数据天数
  calculation_interval: 5m   # 计算间隔
  
  # Alpha引擎并发配置
  alpha:
    concurrency: thread      # thread（线程池，默认）| process（进程池）| none（串行）
    max_workers: null         # 最大并发数，null表示自动设置为calculators数量
  
  # Calculator（因子）配置
  calculators:
    enabled:                  # 指定要加载的calculators，不配置则自动发现所有
      # - "mean_buy_dolvol4_over_dolvol_rank"
      # - "researcher1_momentum_factor"

execution:
  mode: testnet  # testnet (测试网), mock (模拟), live (实盘)
  dry_run: false  # 所有模式都可以配置dry-run开关
  
  contract_settings:
    margin_type: CROSSED      # CROSSED (全仓) 或 ISOLATED (逐仓)
    position_mode: one_way    # one_way (单向) 或 dual_side (双向)
    leverage: 20              # 杠杆倍数: 1-125
  
  testnet:
    accounts:
      - account_id: account1
        api_key: your_testnet_key
        api_secret: your_testnet_secret
```

**配置优先级**: 环境变量 > 配置文件
- Testnet: `{ACCOUNT_ID}_TESTNET_API_KEY` / `{ACCOUNT_ID}_TESTNET_API_SECRET`
- Live: `{ACCOUNT_ID}_API_KEY` / `{ACCOUNT_ID}_API_SECRET`

### 启动/停止

```bash
# 启动系统
python start_all.py

# 停止系统
python stop_all.py

# 访问Web监控界面
# http://localhost:8080
```

## 执行模式

### Testnet模式（推荐用于策略验证）
- 使用Binance测试网环境
- 需要Testnet API密钥
- 不会产生真实订单

### Mock模式（推荐用于开发测试）
- 完全离线模拟，不需要API密钥
- 所有订单都是模拟的
- 适合策略开发、测试和调试

### Live模式（实盘交易）
- **会产生真实订单和真实交易**
- 需要真实API密钥
- 建议先使用 `dry_run: true` 进行测试

**Dry-Run开关**: 所有模式都可以配置 `dry_run: true`，即使使用真实API也不会产生真实订单。

## Web监控界面

系统提供实时Web监控面板，访问 `http://localhost:8080` 即可查看：

- **系统进程状态**: 实时显示所有进程运行状态，支持多账户执行进程监控
- **系统信息**: CPU核心数、内存使用率等系统资源信息
- **账户监控**: 账户余额、持仓、敞口、偏差等实时指标
- **告警信息**: 持仓偏差、敞口、账户健康度告警
- **净值曲线**: 实时显示账户净值曲线，支持历史数据查看
- **策略报告**: 显示策略执行报告，包括交易对、开仓记录、策略磨损分析等
- **自动刷新**: 支持5秒自动刷新，实时更新监控数据

## 策略开发

### 多因子架构（Calculators）

系统采用多因子架构，支持多研究员独立开发calculators（因子），系统自动发现、并发执行并求和。

#### 快速开始

1. **创建新的Calculator文件**

```bash
# 复制模板文件
cp src/strategy/calculators/template.py src/strategy/calculators/your_name_factor_name.py
```

2. **实现Calculator类**

```python
from ..calculator import AlphaCalculatorBase, AlphaDataView
from typing import Dict

class YourFactorCalculator(AlphaCalculatorBase):
    def __init__(self):
        self.name = "your_name_factor_name"  # 必须唯一
        self.mutates_inputs = False  # 推荐False，性能更好

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """实现你的因子逻辑"""
        weights = {}
        for sym in view.iter_symbols():
            # 获取数据
            bar_df = view.get_bar(sym, tail=100)  # 最近100根K线
            tran_df = view.get_tran_stats(sym, tail=100)
            
            # 计算因子
            score = your_calculation(bar_df, tran_df)
            weights[sym] = score
        
        return weights

# 创建实例（系统会自动发现）
CALCULATOR_INSTANCE = YourFactorCalculator()
```

3. **系统自动加载**

系统会在启动时自动发现并加载你的Calculator，无需修改核心代码。

#### 并发执行

- **默认行为**: 当有多个calculators（>1）时，自动使用线程池并发执行
- **性能优势**: 2个calculators在0.73秒内完成，相比串行执行提升约2倍
- **数据安全**: 每个calculator获得独立的数据视图，互不影响
- **配置选项**: 可在`config/default.yaml`中配置并发模式（thread/process/none）

详细开发指南请参考：`src/strategy/calculators/README.md`

### 使用StrategyAPI（已废弃，推荐使用Calculators架构）

修改 `src/strategy/calculator.py` 中的 `_execute_strategy` 方法实现自定义策略。

```python
from src.api.strategy_api import get_strategy_api

strategy_api = get_strategy_api()

# 获取多周期bar数据
bars_5min = strategy_api.get_bar_between(begin_label, end_label, mode='5min')
bars_1h = strategy_api.get_bar_between(begin_label, end_label, mode='1h')

# 获取多周期tran_stats数据
tran_stats_5min = strategy_api.get_tran_stats_between(begin_label, end_label, mode='5min')

# 获取溢价指数K线
premium_index = strategy_api.get_premium_index_bar_between(begin_label, end_label, mode='5min')

# 获取资金费率
funding_rates = strategy_api.get_funding_rate_between(begin_label, end_label)

# 获取Universe（支持版本）
universe = strategy_api.get_last_universe(version='v1')
```

**参数格式**: 
- 时间标签：`'YYYY-MM-DD-HHH'`（例如：`'2025-12-22-004'`），其中HHH为1-288（每天288个5分钟窗口）
- 周期模式：`mode='5min'`（默认）、`'1h'`、`'4h'`、`'8h'`、`'12h'`、`'24h'`
- Universe版本：`version='v1'`（默认）、`'v2'`等

**返回格式**: `{'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}`

### 策略特性

- **防重复触发**: 策略计算过程中自动防止新的data_complete触发，确保计算完整性
- **数据完整性**: 系统检查所有周期（5min, 1h, 4h, 8h, 12h, 24h）的数据完整性后才触发策略
- **Universe自动更新**: 每天23:55自动更新Universe，WebSocket自动重新订阅新的交易对
- **测试策略**: testnet非dry-run模式下使用测试策略（按成交额排序，大的做空，小的做多）

## 系统特性

### 订单执行策略

系统支持多种订单执行策略：
- **Market订单**: 立即以市价执行
- **TWAP订单**: 时间加权平均价格，将订单按时间均匀分配到多个时间段
- **VWAP订单**: 成交量加权平均价格，根据历史成交量分布分配订单

### 数据完整性保障

- **多周期检查**: 检查所有周期（5min, 1h, 4h, 8h, 12h, 24h）的数据完整性
- **窗口关闭判断**: 通过窗口时间判断，确保时间段内的所有交易都已收到
- **无成交处理**: 自动生成无成交时段的K线，确保数据连续性
- **防重复触发**: 策略计算过程中防止新的data_complete触发

### 数据聚合

系统从归集逐笔成交数据（@aggTrade）自行聚合生成多周期K线：
- **数据源**: 使用Binance归集逐笔（@aggTrade），减少数据量，提高处理效率
- **支持周期**: 5min（基础），1h, 4h, 8h, 12h, 24h（从5min聚合）
- **基础字段**: `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`
- **扩展字段**: `vwap`, `dolvol`, `buydolvol`, `selldolvol`, `buyvolume`, `sellvolume`
- **时间字段**: `open_time`, `close_time`, `time_lable`（范围1-288，每天288个5分钟窗口）
- **无成交处理**: 无成交时段自动生成K线，ohlc=上一K线close，vwap=nan，成交额为0
- **Transtats阈值**: 人民币阈值自动转换为USD（除以7.0）

使用Polars进行高性能向量化计算，支持bar表和tran_stats表两种数据格式。

## 数据存储结构

```
data/
├── universe/                    # Universe文件（支持版本）
│   └── YYYY-MM-DD/
│       ├── v1/universe.csv
│       └── v2/universe.csv
├── klines/                      # K线数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── trades/                      # 归集逐笔成交数据
│   └── {SYMBOL}/YYYY-MM-DD-HHh.parquet
├── funding_rates/              # 资金费率数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── premium_index/              # 溢价指数K线数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── positions/                   # 目标持仓文件
│   └── {account_id}_target_positions_{timestamp}.json
├── signals/                     # 进程间信号文件
│   ├── strategy_trigger.json
│   └── execution_trigger_{account_id}.json
├── equity_curve/                # 净值曲线数据（JSON格式，单个文件覆盖模式）
│   └── {account_id}_equity_curve.json
├── position_history/            # 持仓历史数据（JSON格式，单个文件覆盖模式）
│   └── {account_id}_position_history.json
└── strategy_reports/            # 策略报告数据（JSON格式，单个文件覆盖模式）
    └── {account_id}_strategy_report_latest.json
```

## 项目结构

```
long-short-infra/
├── src/
│   ├── api/                  # 策略API：统一封装数据层、系统层、执行层
│   │   └── strategy_api.py   # StrategyAPI（推荐策略开发使用）
│   ├── data/                 # 数据层：采集、聚合、存储
│   │   ├── collector.py      # 归集逐笔成交采集器（@aggTrade）
│   │   ├── kline_aggregator.py  # K线聚合器（多周期支持）
│   │   ├── premium_index_collector.py    # 溢价指数K线采集器
│   │   ├── universe_manager.py  # Universe管理器（支持版本）
│   │   ├── storage.py         # 数据存储管理
│   │   └── api.py            # 数据查询API
│   ├── strategy/             # 策略层：计算、持仓生成
│   │   ├── alpha.py         # Alpha引擎：并发执行calculators
│   │   ├── calculator.py     # Calculator基类和工具
│   │   ├── calculators/      # Calculators（因子）目录
│   │   │   ├── __init__.py  # 自动加载机制
│   │   │   ├── template.py  # 开发模板
│   │   │   └── *.py         # 各研究员的因子文件
│   │   └── position_generator.py  # 持仓生成器
│   ├── execution/            # 执行层：订单管理、持仓管理
│   │   ├── order_manager.py  # 订单管理器（Market/TWAP/VWAP）
│   │   ├── position_manager.py  # 持仓管理器
│   │   └── binance_client.py  # Binance API客户端
│   ├── monitoring/           # 监控层：指标、告警
│   ├── common/               # 公共模块：配置、日志、IPC、网络工具
│   ├── processes/            # 进程入口
│   └── system_api.py         # 系统API
├── web/                      # Web监控界面
│   └── monitor.html          # 监控面板前端
├── config/
│   └── default.yaml          # 主配置文件
├── data/                     # 数据存储
├── logs/                     # 日志目录
├── start_all.py              # 启动脚本
└── stop_all.py               # 停止脚本
```

## 注意事项

1. **账户配置**: 如果配置了 `account_id`，必须填写完整的配置信息，否则该账户不会被启动
2. **API密钥安全**: 建议使用环境变量配置API密钥
3. **测试顺序**: 建议按 mock → testnet → live 的顺序进行测试
4. **实盘交易**: Live模式下会产生真实订单，请确保策略已经充分测试
5. **日志查看**: 系统日志保存在 `logs/` 目录，建议定期检查
6. **时间标签范围**: time_label范围为1-288（每天288个5分钟窗口）
7. **Universe更新**: 每天23:55自动更新Universe，WebSocket会自动重新订阅新的交易对
8. **数据完整性**: 系统会检查所有周期（5min, 1h, 4h, 8h, 12h, 24h）的数据完整性后才触发策略
9. **策略开发**: 推荐使用Calculators架构进行因子开发，支持多研究员协作和并发执行
10. **Web监控**: 访问 `http://localhost:8080` 查看实时监控界面，支持多账户执行进程状态监控
11. **Calculators并发**: 默认使用线程池并发执行所有calculators，可通过配置调整为进程池或串行模式
12. **因子开发**: 在`src/strategy/calculators/`目录下创建因子文件，系统会自动发现并加载

## 版本更新

### 最新功能

- ✅ **Web监控界面**: 提供实时Web监控面板，支持进程状态、账户信息、系统监控
- ✅ **多账户执行进程监控**: 支持监控多个账户的执行进程状态
- ✅ **归集逐笔数据**: 使用@aggTrade替代@trade，减少数据量，提高处理效率
- ✅ **多周期聚合**: 支持1h, 4h, 8h, 12h, 24h多周期K线聚合
- ✅ **溢价指数K线**: 新增溢价指数K线采集和查询接口
- ✅ **Universe版本支持**: 支持v1, v2等版本管理
- ✅ **StrategyAPI**: 统一封装数据层、系统层、执行层接口
- ✅ **TWAP/VWAP订单**: 支持时间加权和成交量加权平均价格订单
- ✅ **防重复触发**: 策略计算防重复触发机制
- ✅ **多周期数据完整性**: 检查所有周期的数据完整性
- ✅ **无成交K线处理**: 自动生成无成交时段的K线
- ✅ **Transtats阈值转换**: 人民币阈值自动转换为USD
- ✅ **多因子架构**: 支持多研究员独立开发calculators（因子），系统自动发现、并发执行并求和
- ✅ **因子并发执行**: 默认使用线程池并发执行所有calculators，提升计算性能