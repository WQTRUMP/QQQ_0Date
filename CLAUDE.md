# Binance BTCUSDT 1m Trading System - Demo/Testnet 分钟级永续合约交易系统

Python 3.9+ + Binance USDⓈ-M REST/WebSocket + SQLite

<directory>
configs/ - BTCUSDT 1m 运行配置样例与安全默认值
docs/ - Demo/Testnet 运行、限频、恢复与托管保护手册
python/ - 单进程交易核心及离线安全回归（2 个子目录: binance_trading、tests）
</directory>

<config>
Binance_Dashboard.html - BTCUSDT 行情、账户、仓位、风险与保护状态的 loopback 只读页面
requirements.txt - 转发 Python 精确依赖锁
start.sh - 仅支持 paper/testnet 的唯一启动入口
README.md - 安装、固定 1m 策略、风控与本地验证指南
</config>

[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
