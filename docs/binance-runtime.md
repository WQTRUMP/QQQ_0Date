<!--
[INPUT]: 依赖 start.sh、configs/binance.env.example 与 python/binance_trading 的固定 1m 策略、订单日志、Algo 保护、运行时和 Dashboard 合同
[OUTPUT]: 对外提供 BTCUSDT USDⓈ-M paper/testnet 的策略边界、私有配置、托管保护、启停、恢复与监控说明
[POS]: docs 的 Binance 运行手册；把 Testnet-only 产品承诺投影为可操作、可验证且失败关闭的现场流程
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
-->

# Binance USDⓈ-M Testnet 运行手册

默认产品面固定为 `BTCUSDT` USDⓈ-M 永续合约，只接受 `paper` 和 `testnet`。两种模式都从 Binance Demo 获取公开行情；`paper` 在本地 SQLite 模拟成交，`testnet` 才会向 Demo 撮合环境提交委托。配置层精确锁定 Demo REST/WSS 域名，本入口没有主网下单能力。

## 准备配置

```bash
cp configs/binance.env.example .env.binance
chmod 600 .env.binance
```

关键约束：

- 启动命令的模式是最终权威；`BINANCE_ENV` 必须是 `testnet`，`BINANCE_SYMBOLS` 必须精确为 `BTCUSDT`。
- REST/WSS 必须分别为 `https://demo-fapi.binance.com` 与 `wss://demo-fstream.binance.com`。
- `paper` 默认 `TRADING_ENABLED=false`，此时只观察行情；要让本地账本模拟交易，需显式改为 `true`。
- `testnet` 需同时提供 Demo API key/secret、`TRADING_ENABLED=true`，并把 `BINANCE_TESTNET_TRADING_CONFIRM` 精确设为 `TESTNET_ONLY`。
- `.env.binance` 只要存在，paper/testnet 都必须是组和其他用户不可读的 `0600`；启动脚本同时以 `umask 077` 保护新建运行文件。
- 运行节奏不是无界调优项：`BINANCE_REQUEST_TIMEOUT_SEC=1..30`、`ACCOUNT_POLL_SECONDS=1..15`、`MAX_BOOK_AGE_SECONDS=0.25..10`、`SIGNAL_MAX_AGE_SECONDS=1..60`；超界值在任何网络连接前拒绝。账户级 REST 审计默认 15 秒，实时保护仍由 mark 流驱动；调低轮询值会直接消耗更多 IP 权重。
- `BINANCE_PAPER_DB_PATH` 与 `BINANCE_TESTNET_DB_PATH` 必须分离；配置层会解析 `..`、相对/绝对路径和已有符号链接后比较有效路径，任何指向同一 SQLite 文件的别名都会被拒绝；通用 `BINANCE_DB_PATH` 也会被拒绝。
- Dashboard 只允许绑定 `127.0.0.1`、`localhost` 或 `::1`。

密钥只应保存在被 Git 忽略的 `.env.binance`，不得写入日志、Dashboard 或仓库。即使当前只运行 paper，文件中留存的 Demo 凭证也不能放宽权限。

## 启动

```bash
./start.sh                 # Binance paper
./start.sh paper
./start.sh testnet
```

这是项目唯一入口。进程前台运行，控制台默认位于 `http://127.0.0.1:8765/`；用 `Ctrl-C`、`SIGINT` 或 `SIGTERM` 停止。

## 固定 1m 入场逻辑

方向判断只有一条规则：系统只消费 `BTCUSDT` 已闭合的 `1m` K 线，EMA5 上穿 EMA13 时做多、下穿时做空，同一根闭柱最多产生一个信号。ATR14 不参与方向判断，只提供止损距离；指标周期不接受环境变量调节。历史柱只水合状态，未闭柱、重复柱、乱序柱和断档后的首批重建柱都不会开仓。

这属于分钟级中高频执行：每分钟评估一次，但只有真实交叉且全部安全门通过时才下单。系统固定单标的、单仓位，不为了提高交易次数叠加打分器、预测模型或并行策略。

## 运行时因果与风险边界

启动按以下顺序完成，任一步失败都会保持 `risk.can_open=false` 并重试水合：

1. 同步 Demo server time，读取 `exchangeInfo`，验证 `TRADING + PERPETUAL + USDT` 并加载 tick/step/min notional；运行期间以 event-loop 单调时钟最多每 300 秒重校，不依赖可能跳变的宿主墙钟决定 deadline。失败不会推进 deadline，下一次账户轮询继续重试；`-1021` 会强制立即重校，重校失败仍保持到期。
2. 获取历史 `1m` K 线，按当前 UTC 丢弃 `close_time >= now` 的未完成柱；历史只水合 EMA5/EMA13 与 Wilder ATR14，不发信号。
3. `testnet` 验证 One-way，恢复 SQLite 普通订单阶段；`DISPATCH_UNCERTAIN` 只能按原 `clientOrderId` 查单，运行期间也持续查询直到终态，永远不能重投。同一 symbol 的阻断集合每次从全部 SQLite 未决记录重建，一笔明确终态不能替另一笔未知订单恢复交易权限。
4. 先只读拉取账户、全部持仓、账户级 `openOrders` 与 `openAlgoOrders`；只有账户确认为空、没有本地计划/未决订单/远端活动单时，才幂等设置 `ISOLATED` 与 `MAX_LEVERAGE`，禁止启动过程改动人工或遗留仓位。
5. 对账普通活动委托、交易所/账本持仓、保护计划及 Algo 两腿；任何人工活动单、裸仓、跨 symbol、Hedge、cross margin、超杠杆、数量/方向偏差或不匹配的 Algo 单全部阻断。
6. `bookTicker` 走 `/public`，`markPrice` 与 `1m` 闭柱走 `/market`，两条 combined stream 独立退避并合并。只有 update id/事件时间严格更新的 book、mark 才能进入状态并驱动保护；旧帧被忽略，同身份冲突载荷会永久锁死 readiness。收到新鲜 book、mark 和账户快照后才进入 ready；只有新鲜 live 闭柱允许产生 EMA5/EMA13 交叉信号。

入场数量取“权益风险预算 ÷ ATR 止损距离、权益名义价值上限 ÷ 入场价、exchange 最大数量”的最小值并向下量化。系统同时检查 UTC 日损、盘口年龄/点差、可用保证金、持仓上限和 signal 年龄；止损或目标非正、量化后不满足方向几何时，在创建订单日志前直接拒绝。成交后仍用同一几何函数围绕实际成交价重新冻结保护。

`testnet` 入场明确成交后，会在应用 entry journal 的同一 SQLite 事务内写入 PositionPlan、STOP/TARGET 两腿及稳定 `clientAlgoId`。随后按 STOP 优先顺序调用 Binance Algo Service 的 `POST /fapi/v1/algoOrder`：两腿分别使用 `STOP_MARKET`、`TAKE_PROFIT_MARKET`，固定 `positionSide=BOTH`、`workingType=MARK_PRICE`、`closePosition=true`，不同时发送 quantity/reduceOnly。自 2025-12-09 起条件单已迁移到 Algo Service，旧 `/fapi/v1/order` 会以 `-4120` 拒绝，系统不会使用旧路径。[Binance 变更日志](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log#2025-11-06) · [Algo Order API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#new-algo-order)

两腿没有原生 OCO。任一 native 腿进入触发态后，SQLite 单赢家 CAS 冻结 STOP/TARGET，兄弟腿先进入 `CANCEL_UNKNOWN` 再撤销；查询返回的 `actualOrderId/actualQty/actualPrice` 以累计量幂等落盘，计划数量只能向权威 `abs(positionAmt)` 下调，且下调必须是 update-only CAS——较新的平仓清理一旦删除计划，较旧账户快照不得重新插入。残余仓位立即按收敛后的精确数量走 reduce-only MARKET。重启后必须先查询，不能盲撤或重发。

交易所保护未完全 ARMED 时，本地 mark stop/target 仍有效；若 Algo 明确拒绝或无法完成布防，runtime 尝试 `MARKET + reduceOnly + positionSide=BOTH + newOrderRespType=RESULT` 平仓。若入场已成交但保护同步被 REST 限频或读取异常打断，runtime 先锁存 `protection_sync_deferred`：冷却时间自然结束不能解除它，也不能授权下一笔入场；只有一次完整成功的账户/保护刷新可清闩并继续布防。该闩是进程内 readiness 状态；跨重启安全不依赖它，而由 SQLite 中已原子落盘的 plan/legs 经 bootstrap 重新对账并阻断。Testnet REST 冷却期间不会抢占 LOCAL 赢家或制造注定失败的退出记录，paper 的 SQLite 本地止损/目标不受该网络冷却影响。

LOCAL 赢家只先冻结退出所有权，不会立刻撤原生腿；只有账户权威确认 `positionAmt=0` 后才越过撤单屏障。因此本地 MARKET 明确拒绝或提交未知时，已有 STOP/TARGET 仍留在交易所；明确零成交会在下一次账户事实刷新后重试，提交未知则只恢复原订单身份。账户已归零但实际普通订单仍活动时，保护证据同样不会清理。

普通 MARKET/LIMIT 与 Algo POST 都必须拿到明确事实后才能推进状态。HTTP 408、`-1006/-1007`、传输超时、截断/不可解析响应、无法证明失败的 5xx、非终态或查单失败都保持未知；普通单查原 `clientOrderId`，Algo 查原 `clientAlgoId`，禁止第二次提交。只有 Binance 明确标为 100% 失败的 503 变体（`Service Unavailable`、指定 internal error、`-1008`）可安全记为未成交。[Binance 错误码](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/error-code)

HTTP 418/429 是明确拒绝并会打开线程安全的客户端全局冷却闩：合法数字 `Retry-After` 优先，缺失或非法时分别采用 120/60 秒保守值；冷却期所有 REST 在 transport 前本地拒绝，账户循环可被 stop 中断地等待，Testnet 入场和 LOCAL 网络退出也不会制造无意义的新 journal。Algo POST 在明确限频后把腿从派发屏障退回 `PREPARED`，但保留冻结的同一 `clientAlgoId`，冷却后只能以相同身份重试。账户级 `positionMode/account/positionRisk/openOrders/openAlgoOrders` 默认每 15 秒审计一次，调低会提高 IP 权重占用。[Binance 限频与签名时序](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)

官方当前只明确公布生产 User Data Stream 的私有 WSS 地址，没有明确公布 Demo 私有流完整 URL。本系统不会猜测地址，当前以 signed REST 账户/普通单/Algo 轮询作为权威恢复通道；后续只有在官方 Demo 文档补齐或受控验证形成独立契约后才接入 `ALGO_UPDATE`。

## 崩溃恢复合同

组合根在构造 REST 客户端前同时持有规范数据库路径锁与 inode 锁；相对路径、符号链接或已有硬链接都不能启动第二实例。即使进程边界被误用，每笔订单仍通过 SQLite CAS 选出唯一派发者，并在触碰 broker 前写入独立日志：

```text
PREPARED -> DISPATCH_UNCERTAIN -> RESULT -> APPLIED
```

- `PREPARED` 尚未越过派发屏障，重启时可证明未发送并安全关闭记录。
- `DISPATCH_UNCERTAIN` 可能已进入撮合；重启只查询原订单身份。
- `RESULT` 已有明确成交/未成交事实，但尚未应用到保护计划。
- `APPLIED` 与 entry plan 创建或 exit plan 缩减/删除在同一事务完成，重复恢复不会二次扣减。

托管保护使用独立状态机；入场应用时两腿必须原子出现：

```text
ProtectionSet: PREPARED -> ARMING -> ARMED -> EXITING -> CANCELING -> CLOSED
Leg: PREPARED -> SUBMIT_UNKNOWN -> OPEN -> TRIGGERED/FILLED
       ^             |
       +-- 418/429 --+  (same clientAlgoId, definitely not submitted)
                                      \-> CANCEL_UNKNOWN -> CANCELED
```

`SUBMIT_UNKNOWN` 只能查询稳定 `clientAlgoId`；唯一反向边是 HTTP 418/429 已明确证明请求未提交时回到 `PREPARED`，且身份不得变化。这不是未知提交的盲重投。STOP 未明确 OPEN 前不会发送 TARGET。任何额外、重复、字段变形或跨 symbol 的活动 Algo 单都会失败关闭。平仓后只有“positionAmt=0、普通 openOrders 为空、两腿均终态”同时成立，才能删除保护束并允许下一次入场。

paper 成交另由幂等 ledger 证明；testnet 普通订单与 Algo 订单分别由 Binance 查询事实证明。普通 `openOrders` 稳态必须为空；`openAlgoOrders` 只允许与本地冻结 spec 精确一致的两腿。若远端存在其他活动委托或没有本地保护身份的仓位，系统不会猜测其意图，也不会继续开仓，需人工核对后再处理。

paper 账本与 testnet/runtime 状态库都在任何 schema、初始资金或订单事实写入前执行完整 `PRAGMA quick_check`，且只接受精确单行 `ok`。损坏、空结果或检查异常会关闭连接并拒绝启动；系统不会自动修复、删除或覆盖恢复真源。

## Dashboard 合同

Dashboard 只有 GET 接口，没有下单、撤单、修改风控或写配置能力。

| 路径 | 用途 | 状态语义 |
|---|---|---|
| `/healthz` | HTTP 进程存活 | 静态资源已装载时返回 200 |
| `/readyz` | runtime 依赖就绪 | 历史、行情、账户和对账全部满足时返回 200，否则 503 |
| `/api/snapshot` | 有界只读快照 | 固定输出 market/account/positions/protection/risk/events，异常时失败关闭 |
| `/` | BTCUSDT perpetual 操作台 | 展示 UTC book、mark/index/funding、资金、杠杆、清算价、托管保护和事件 |

页面屏蔽密钥、签名与 token，只保留 BTCUSDT 持仓，使用严格 CSP，并校验 Host/Origin 为 loopback。`TRADING_ENABLED=false` 不影响依赖就绪，但 `risk.can_open` 固定为 false。

## 组合根与本地验证

`python.binance_trading.main` 依赖 `runtime.create_runtime(config)`；缺失会直接拒绝启动，不会伪装成可用系统。runtime 提供 `start()`、`stop()`、`snapshot()` 与 `ready()/is_ready()`。若停机等待结束时交易 worker 仍活着，或调用发生在 worker 自身线程，`stop()` 会保留线程句柄和 SQLite、明确报错，绝不伪装停机成功后关闭恢复真源。

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover -s python/tests -p 'test_binance_*.py' -v
bash -n start.sh
./start.sh --help
git diff --check
```

运行后可检查：

```bash
curl --fail http://127.0.0.1:8765/healthz
curl --include http://127.0.0.1:8765/readyz
curl --fail http://127.0.0.1:8765/api/snapshot
```

组件未水合、行情/账户陈旧或对账失败时，`/readyz` 返回 503 是正确的安全结果。
