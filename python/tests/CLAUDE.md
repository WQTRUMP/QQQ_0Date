# tests/
> L2 | 父级: ../CLAUDE.md

binance_runtime_support.py: 非发现型 runtime fixture；提供闭柱历史、公开 Demo 客户端与可控 Testnet 普通/Algo 订单事实
test_binance_broker.py: 校验 paper 多空/reduce-only 会计、One-way 参数、MARKET/LIMIT RESULT 与未知提交只查不重投
test_binance_config.py: 校验固定 BTCUSDT/1m、Demo 域名、显式授权、时序边界、恢复库隔离及保护价格方向几何
test_binance_dashboard.py: 校验 perpetual/UTC 页面、固定快照、敏感字段屏蔽、CSP、Host/Origin 与 loopback HTTP 合同
test_binance_dispatch.py: 校验持久化先于派发、PREPARED 安全放弃及 DISPATCH_UNCERTAIN 跨重启只查询
test_binance_exchange.py: 校验 REST 签名/校时、普通与 Algo API、418/429 冷却、非法响应和 exchangeInfo 过滤器
test_binance_instance_lock.py: 校验规范路径及 inode 单实例所有权、释放后重获与锁文件权限
test_binance_ledger.py: 校验 paper SQLite 完整性、幂等成交、多空 PnL/费用、reduce-only 和 UTC 日初权益
test_binance_market_state.py: 校验 book update id 与 mark 时间的单调投影、冲突拒绝及估值因果
test_binance_market_stream.py: 校验 public/market 双路由、clean-close 退避、行情解析、symbol 隔离和历史顺序
test_binance_protection.py: 校验 STOP 优先布防、未知提交只查、限频同身份重试、部分成交和安全撤销
test_binance_protection_state.py: 校验入场/两腿原子登记、阶段与赢家 CAS、累计成交和跨重启恢复
test_binance_main.py: 校验 dotenv 0600、启动 umask、CLI 模式权威与安全停机
test_binance_risk.py: 校验权益风险定量及盘口、日损、持仓、名义价值与交易所步长硬门
test_binance_runtime.py: 串联真实 1m 交叉单次派发、极端 ATR 拒绝、周期校时、REST 冷却、paper 闭环和 worker 停机边界
test_binance_runtime_safety.py: 贯通保护同步闩、durable 未决阻断、精确余仓退出与 fallback 保腿重试
test_binance_state.py: 校验恢复库完整性、订单/计划原子应用、单向下修、并发删除和部分退出幂等
test_binance_strategy.py: 校验固定 1m EMA5/13 + ATR14、历史静默、闭柱幂等、冲突拒绝和断档重建

[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
