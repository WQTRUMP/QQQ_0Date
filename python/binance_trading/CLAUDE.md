# binance_trading/
> L2 | 父级: ../CLAUDE.md

__init__.py: 包的窄公开入口；只导出配置与线性永续领域值对象，导入时不触发网络或数据库副作用
account_service.py: 账户同步应用服务；并发读取账户四类事实，以 durable 未决订单阻止过早清理，并在 Algo 部分成交后重投影需立即退出的保护余仓
broker.py: paper 与 Binance Demo 执行防腐层；统一 One-way/reduce-only、LIMIT IOC、36 字符订单身份及未知提交只查不重投
clock_sync.py: 单调时钟校时调度器；启动强制同步并最多每 300 秒重校，失败保持到期以让下一账户轮询继续重试
config.py: BTCUSDT/1m 配置真源；固定策略周期并在联网前拒绝主网、隐式授权、失真 HTTP/轮询/新鲜度窗口、账本别名与越界风险
dashboard.py: loopback 只读 HTTP 边界；把 runtime 快照与托管保护生命周期裁剪为固定 BTCUSDT perpetual/UTC schema，并屏蔽交易身份、凭证与跨站访问
dispatch.py: 持久化派单边界；先落 PREPARED/DISPATCH_UNCERTAIN 再调用 broker，重启后只按 clientOrderId 查单而不重投
exchange.py: Binance USDⓈ-M Demo REST 客户端；负责签名/时间同步、普通与 Algo service 请求、账户级活动单、非法响应保护、418/429 全局冷却及未知执行状态分类
instance_lock.py: 进程级恢复库所有权边界；以规范路径锁与 inode 锁同时覆盖别名/硬链接，在联网前拒绝重复实例，派发 CAS 作为第二道防线
ledger.py: paper SQLite 权威账本；通过共享 quick_check 门后，以单事务幂等成交维护线性多空、手续费、损益与 UTC 日初权益
main.py: paper/testnet 薄组合根；对任何存在的 dotenv 强制私有权限，构造 runtime 与 Dashboard，并以信号驱动幂等停机
market_stream.py: Binance 双路由 combined WebSocket/REST 行情防腐层；独立退避并合并 `/public` bookTicker 与 `/market` markPrice/闭合 K 线，只投影授权 symbol
market_state.py: 单调行情内存投影；以 update id/事件时间区分接受、忽略和冲突，并只让被接受的 mark 更新持仓估值与驱动保护
models.py: 线性永续领域契约；用 Decimal、UTC datetime 和冻结值对象表达行情、账户、持仓、规则、意图与订单结果
protection.py: Binance Algo 托管保护编排层；STOP 先布防、TARGET 后布防，未知提交只查原 clientAlgoId，明确 418/429 以相同身份回到可重试前态，累计 native fill 并只向下收敛余仓计划
protection_state.py: 托管保护持久化核心；把两腿身份/指纹、限频安全回退、累计成交、单赢家及 LOCAL 平仓确认后的延后撤单屏障组合进 SQLite 事务
read_model.py: 运行状态只读投影；统一计算依赖新鲜度与失败关闭原因，并生成含非敏感保护摘要的 Dashboard 原始快照
reconciliation.py: 账户/持仓/普通活动委托对账防腐层；把账户外订单、裸仓、跨 symbol、Hedge、cross margin、超杠杆和保护计划偏差转成阻断原因
risk.py: fail-closed 入场风控；以权益风险预算、止损距离、盘口新鲜度/点差、UTC 日损、持仓数及 exchangeInfo 动态量化数量
runtime.py: 单进程交易编排器；每根闭合 1m K 线接入唯一策略，按 SQLite 投影未决订单，并以行情/REST/保护闩约束交易和安全停机
safety_gates.py: 运行时授权纯函数；从 SQLite 未决订单投影 symbol 阻断与原因，并把 REST 冷却严格限定为 testnet 网络派发
sqlite_health.py: SQLite 启动完整性共享门；要求 quick_check 精确返回单行 ok，使账本与订单恢复库在任何写入前采用同一失败关闭语义
state.py: 跨重启恢复组合根；通过共享 quick_check 门后原子持久化订单阶段/结果，以 update-only CAS 下修计划并保存两腿保护身份与 UTC 基线
strategy.py: 固定 BTCUSDT 1m EMA5/13 + ATR14 内核；只在闭柱交叉发一次信号，历史静默、重复幂等、冲突拒绝且断档重建
trade_planning.py: 保护几何与成交计划工厂；入场前/成交后共用正价格方向校验，并用持久 attempt 序号派生 reduce-only 退出身份

[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
