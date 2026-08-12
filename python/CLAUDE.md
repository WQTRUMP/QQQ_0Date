# python/
> L2 | 父级: ../CLAUDE.md

binance_trading/: BTCUSDT 单进程交易核心；从 Demo 行情到 1m 信号、风控、持久订单、托管保护和只读监控形成唯一生产闭包
tests/: Binance 无网络回归；覆盖配置、行情、策略、风控、派单恢复、账本、保护、运行时与 Dashboard
__init__.py: 将 python 声明为可导入包，不产生运行副作用
requirements.txt: 只锁定 websockets 与 python-dotenv，两者分别服务行情连接和私有配置加载

[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
