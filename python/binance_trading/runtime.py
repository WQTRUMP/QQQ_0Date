"""
[INPUT]: 依赖 clock_sync/market_state/safety_gates/trade_planning 安全边界、Demo REST/WebSocket、闭柱策略、fail-closed 风控、账户活动委托事实、paper/testnet broker 与 SQLite 恢复真源
[OUTPUT]: 对外提供 BinanceRuntime 与 create_runtime，串联固定 1m EMA5/13 入场、周期校时、限频/保护闩、持久订单恢复、托管退出与只读快照
[POS]: binance_trading 的单进程应用核心；只编排领域组件，不绕过 Testnet-only 配置或复制交易所/账本真相
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""
from __future__ import annotations
import asyncio
import logging
import threading
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Deque, Dict, Optional, Set, Tuple
from .account_service import refresh_paper_account, refresh_testnet_account
from .broker import RETRYABLE_SERVER_FAILURE, BinanceTestnetBroker, PaperBroker, normalize_client_order_id
from .clock_sync import ExchangeClockSync, utc_now
from .config import BinanceConfig
from .dispatch import DurableOrderDispatcher, RecoveryItem, TERMINAL_NO_FILL
from .exchange import BinanceApiError, BinanceFuturesClient, BinanceRateLimitError, parse_symbol_rules
from .ledger import PaperLedger
from .market_stream import BinanceMarketStream, MarketEvent, parse_rest_klines
from .market_state import ACCEPTED, CONFLICT, apply_book_update, apply_mark_update
from .models import (
    BookTicker,
    Direction,
    ExecutionResult,
    Kline,
    MarkPrice,
    OrderIntent,
    Position,
    SymbolRules,
)
from .protection import ProtectionCoordinator, build_protection_legs
from .read_model import build_runtime_snapshot, readiness_reason
from .risk import RiskManager
from .safety_gates import durable_order_symbols, order_unknown_reason, rest_cooling_down
from .state import PositionPlan, RuntimeStateStore
from .strategy import OneMinuteEmaStrategy, TradeSignal
from .trade_planning import build_exit_intent, build_filled_position_plan, protection_prices
LOGGER = logging.getLogger(__name__)
ZERO, TEN_THOUSAND = Decimal("0"), Decimal("10000")
class BinanceRuntime:
    """Testnet-only orchestration with one event-loop thread and read-only snapshots."""
    def __init__(
        self,
        config: BinanceConfig,
        *,
        client: Optional[Any] = None,
        market_stream: Optional[Any] = None,
        ledger: Optional[PaperLedger] = None,
        state_store: Optional[RuntimeStateStore] = None,
        broker: Optional[Any] = None,
        clock: Any = utc_now,
        monotonic: Optional[Callable[[], float]] = None,
    ) -> None:
        if not isinstance(config, BinanceConfig):
            raise TypeError("BinanceRuntime 只接受已验证 BinanceConfig")
        self.config = config
        self.client = client or BinanceFuturesClient(config)
        self.market_stream = market_stream or BinanceMarketStream(config)
        self.clock = clock
        self._ledger = ledger
        self._state_store = state_store
        self._broker = broker
        self._dispatcher: Optional[DurableOrderDispatcher] = None
        self._protection: Optional[ProtectionCoordinator] = None
        self._risk = RiskManager(config)
        self._lock = threading.RLock()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=100)
        self._books: Dict[str, BookTicker] = {}
        self._marks: Dict[str, MarkPrice] = {}
        self._rules: Dict[str, SymbolRules] = {}
        self._strategies: Dict[str, OneMinuteEmaStrategy] = {}
        self._plans: Dict[str, PositionPlan] = {}
        self._positions: Dict[str, Position] = {}
        self._account: Optional[AccountSnapshot] = None
        self._ledger_snapshot: Dict[str, Any] = {}
        self._pending_symbols: Set[str] = set()
        self._uncertain_symbols: Set[str] = set()
        self._clock_sync = ExchangeClockSync(self.client.sync_time, monotonic)
        self._bootstrapped = False
        self._bootstrap_reason = "runtime_not_started"
        self._reconcile_reason = "account_not_hydrated"
        self._sticky_reason = ""
        self._order_unknown_reason = ""
        self._started = False
        self._closed = False
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_stop: Optional[asyncio.Event] = None
        self._stop_requested = threading.Event()
    # ── lifecycle ──────────────────────────────────────────────────────
    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("runtime 已关闭，不能重新启动")
            if self._thread is not None and self._thread.is_alive():
                return
            self._started = True
            self._stop_requested.clear()
            self._thread = threading.Thread(
                target=self._thread_main,
                name="binance-runtime",
                daemon=False,
            )
            self._thread.start()
    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except BaseException as exc:
            LOGGER.exception("Binance runtime thread failed")
            with self._lock:
                self._sticky_reason = "runtime_thread_failed:%s" % type(exc).__name__
            self._record_event("runtime", self._sticky_reason, "error")
    async def _run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._async_stop = asyncio.Event()
        if self._stop_requested.is_set():
            self._async_stop.set()
        delay = 1.0
        while not self._async_stop.is_set():
            try:
                await self.bootstrap_once()
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._set_bootstrap_failure(exc)
                try:
                    await asyncio.wait_for(self._async_stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    delay = min(delay * 2, 30.0)
                continue
            market_task = asyncio.create_task(self._consume_market(), name="binance-market")
            account_task = asyncio.create_task(self._account_loop(), name="binance-account")
            stop_task = asyncio.create_task(self._async_stop.wait(), name="binance-stop")
            done, pending = await asyncio.wait(
                (market_task, account_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            if self._async_stop.is_set():
                break
            failed = next((task for task in done if task is not stop_task), None)
            error = failed.exception() if failed is not None and not failed.cancelled() else None
            self._set_bootstrap_failure(error or RuntimeError("runtime task stopped"))
    def stop(self) -> None:
        self._stop_requested.set()
        loop = self._loop
        stop_event = self._async_stop
        if loop is not None and stop_event is not None and loop.is_running():
            loop.call_soon_threadsafe(stop_event.set)
        thread = self._thread
        if thread is threading.current_thread():
            with self._lock:
                self._sticky_reason = "shutdown_incomplete:self_thread"
            raise RuntimeError("Binance worker 不得从自身线程关闭持久化存储")
        if thread is not None:
            thread.join(timeout=float(self.config.request_timeout_sec) + 10.0)
            if thread.is_alive():
                # 交易线程仍可能在 submit→query 的不确定提交边界内。此时关闭
                # SQLite 会让稍后返回的权威结果无法落盘，所以停机必须明确失败。
                with self._lock:
                    self._sticky_reason = "shutdown_incomplete:worker_alive"
                raise RuntimeError("Binance worker 仍在运行，拒绝关闭持久化存储")
        with self._lock:
            self._bootstrapped = False
            self._bootstrap_reason = "runtime_stopped"
            self._started = False
            self._thread = None
        self._close_stores()
    def _close_stores(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            ledger, store = self._ledger, self._state_store
        if ledger is not None:
            ledger.close()
        if store is not None:
            store.close()
    # ── bootstrap and reconciliation ──────────────────────────────────
    def _ensure_components(self) -> None:
        if self._state_store is None:
            self._state_store = RuntimeStateStore(self.config.database_path)
        if self.config.mode == "paper" and self._ledger is None:
            self._ledger = PaperLedger(
                self.config.database_path,
                initial_balance=self.config.paper_starting_equity,
            )
        if self._broker is None:
            if self.config.mode == "paper":
                if self._ledger is None:
                    raise RuntimeError("paper ledger 未初始化")
                self._broker = PaperBroker(
                    self._ledger,
                    fee_bps=self.config.paper_fee_bps,
                    clock=self._exchange_now,
                )
            else:
                self._broker = BinanceTestnetBroker(self.client, clock=self._exchange_now)
        if self._dispatcher is None:
            self._dispatcher = DurableOrderDispatcher(
                mode=self.config.mode,
                state_store=self._state_store,
                broker=self._broker,
                ledger=self._ledger,
                clock=self._exchange_now,
            )
        if self.config.mode == "testnet" and self._protection is None:
            if self._state_store is None:
                raise RuntimeError("runtime state store 未初始化")
            self._protection = ProtectionCoordinator(
                self.client,
                self._state_store,
                clock=self._exchange_now,
            )
    async def bootstrap_once(self) -> None:
        self._ensure_components()
        await self._clock_sync.synchronize(force=True)
        exchange_info = await asyncio.to_thread(self.client.exchange_info)
        rules: Dict[str, SymbolRules] = {}
        strategies: Dict[str, OneMinuteEmaStrategy] = {}
        for symbol in self.config.symbols:
            rules[symbol] = parse_symbol_rules(exchange_info, symbol)
            strategy = OneMinuteEmaStrategy(symbol=symbol)
            bars = await self._history(symbol)
            strategy.hydrate(bars)
            if not strategy.ready:
                raise RuntimeError("%s 历史闭柱不足以水合策略" % symbol)
            strategies[symbol] = strategy
        with self._lock:
            self._rules = rules
            self._strategies = strategies
        if self.config.mode == "testnet":
            if await asyncio.to_thread(self.client.position_mode):
                raise RuntimeError("Binance 账户必须手工切换为 One-way 模式")
            await self._recover_orders()
            safe_to_configure = await self._refresh_testnet_account()
            # 账户事实必须先于任何写操作。已有仓位、活动单或本地计划时，
            # runtime 只读对账并失败关闭，绝不改动人工/遗留风险参数。
            if safe_to_configure:
                for symbol in self.config.symbols:
                    change_margin = getattr(self.client, "change_margin_type", None)
                    change_leverage = getattr(self.client, "change_leverage", None)
                    if not callable(change_margin) or not callable(change_leverage):
                        raise RuntimeError("Demo client 缺少隔离保证金/杠杆配置能力")
                    await asyncio.to_thread(change_margin, symbol, "ISOLATED")
                    await asyncio.to_thread(change_leverage, symbol, self.config.max_leverage)
                await self._refresh_testnet_account()
        else:
            await self._recover_orders()
            self._refresh_paper_account()
        with self._lock:
            self._bootstrapped = True
            self._bootstrap_reason = ""
        self._record_event("runtime", "bootstrap_complete", "info")
    async def _history(self, symbol: str) -> Tuple[Kline, ...]:
        rows = await asyncio.to_thread(
            self.client.klines,
            symbol,
            self.config.interval,
            self.config.history_limit,
        )
        now = self._exchange_now()
        bars = parse_rest_klines(rows, symbol, self.config.interval)
        closed = tuple(bar for bar in bars if bar.close_time < now)
        if not closed:
            raise RuntimeError("%s 没有已完成的历史 K 线" % symbol)
        return closed
    def _set_bootstrap_failure(self, exc: BaseException) -> None:
        reason = "bootstrap_failed:%s:%s" % (type(exc).__name__, str(exc))
        with self._lock:
            changed = reason != self._bootstrap_reason
            self._bootstrapped = False
            self._bootstrap_reason = reason
        if changed:
            self._record_event("runtime", reason, "error")
    async def _recover_orders(self) -> None:
        if self._dispatcher is None or self._state_store is None:
            raise RuntimeError("durable dispatcher 未初始化")
        items = await asyncio.to_thread(self._dispatcher.recover)
        for item in items:
            symbol = item.record.intent.symbol
            with self._lock:
                self._pending_symbols.add(symbol)
            try:
                await self._apply_recovery(item)
            finally:
                with self._lock:
                    self._pending_symbols.discard(symbol)
    async def _apply_recovery(self, item: RecoveryItem) -> None:
        intent, result = item.record.intent, item.result
        if result.submission_unknown or result.status == "UNKNOWN":
            self._mark_uncertain(intent.symbol, result.reason or "RECOVERY_UNKNOWN")
            return
        if intent.reduce_only:
            if self._state_store is None:
                raise RuntimeError("runtime state store 未初始化")
            plan = self._state_store.get_plan(intent.symbol)
            if plan is None:
                if result.executed_quantity == ZERO:
                    self._state_store.mark_applied(intent.client_order_id, result.observed_at)
                    return
                self._mark_uncertain(intent.symbol, "FILLED_EXIT_WITHOUT_PLAN")
                return
            await self._apply_exit_result(plan, result, intent.reason or "RECOVERY")
            return
        if intent.stop_price is None or intent.target_price is None:
            self._mark_uncertain(intent.symbol, "ENTRY_RECOVERY_MISSING_GEOMETRY")
            return
        distance = abs(intent.target_price - intent.stop_price) / (
            Decimal("1") + self.config.reward_risk_ratio
        )
        await self._apply_entry_result(intent, result, distance)
    async def _account_loop(self) -> None:
        seconds = float(self.config.account_poll_seconds)
        while self._async_stop is not None and not self._async_stop.is_set():
            wait_seconds = seconds
            try:
                await self._clock_sync.synchronize()
                with self._lock:
                    dispatch_in_flight = bool(self._pending_symbols)
                if not dispatch_in_flight:
                    # 未知提交并非只在启动时恢复；每轮都只按原 client id 查单，
                    # 让稍后 FILLED/CANCELED 的事实可以自动收敛且永不重投。
                    await self._recover_orders()
                if self.config.mode == "testnet":
                    await self._refresh_testnet_account()
                else:
                    self._refresh_paper_account()
                wait_seconds = min(seconds, self._clock_sync.remaining_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                with self._lock:
                    self._reconcile_reason = "account_poll_failed:%s" % type(exc).__name__
                self._record_event(
                    "account",
                    "account_poll_failed:%s:%s" % (type(exc).__name__, exc),
                    "error",
                )
                if isinstance(exc, BinanceRateLimitError):
                    wait_seconds = max(seconds, exc.retry_after_seconds)
                elif isinstance(exc, BinanceApiError) and exc.code == -1021:
                    await self._clock_sync.retry_after_timestamp_error()
            try:
                await asyncio.wait_for(self._async_stop.wait(), timeout=wait_seconds)
            except asyncio.TimeoutError:
                pass
    def _refresh_paper_account(self) -> None:
        if self._ledger is None or self._state_store is None:
            raise RuntimeError("paper stores 未初始化")
        with self._lock:
            marks = dict(self._marks)
            pending = set(self._pending_symbols)
        refreshed = refresh_paper_account(
            config=self.config,
            ledger=self._ledger,
            state_store=self._state_store,
            marks=marks,
            pending_symbols=pending,
            now=self._exchange_now(),
        )
        with self._lock:
            self._plans = refreshed.plans
            self._positions = refreshed.positions
            self._account = refreshed.account
            self._ledger_snapshot = dict(refreshed.ledger_snapshot)
            self._reconcile_reason = refreshed.reason
    async def _refresh_testnet_account(self) -> bool:
        if self._state_store is None or self._protection is None:
            raise RuntimeError("testnet account components 未初始化")
        durable = durable_order_symbols(self._state_store)
        with self._lock:
            pending = set(self._pending_symbols) | durable
        refreshed = await refresh_testnet_account(
            config=self.config,
            client=self.client,
            state_store=self._state_store,
            protection=self._protection,
            pending_symbols=pending,
            clock=self._exchange_now,
        )
        durable = durable_order_symbols(self._state_store)
        with self._lock:
            self._plans = refreshed.plans
            self._positions = refreshed.positions
            self._account = refreshed.account
            self._reconcile_reason = refreshed.reason
            blocked = self._pending_symbols | self._uncertain_symbols | durable
        for symbol in refreshed.protection_exit_symbols:
            plan = refreshed.plans.get(symbol)
            if plan is not None and symbol not in blocked:
                await self._fail_closed_exit(plan, "PROTECTION_RESIDUAL")
        return refreshed.safe_to_configure
    # ── market events and trading ─────────────────────────────────────
    async def _consume_market(self) -> None:
        async for event in self.market_stream.events():
            if self._async_stop is not None and self._async_stop.is_set():
                return
            try:
                await self.handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._record_event(
                    "market",
                    "event_failed:%s:%s" % (type(exc).__name__, exc),
                    "error",
                )
    async def handle_event(self, event: MarketEvent) -> None:
        if isinstance(event, BookTicker):
            with self._lock:
                outcome = apply_book_update(self._books, event)
                if outcome == CONFLICT:
                    self._sticky_reason = "market_data_conflict:%s:book_ticker" % event.symbol
            if outcome == CONFLICT:
                raise ValueError("bookTicker 相同 update id 出现冲突载荷")
            return
        if isinstance(event, MarkPrice):
            with self._lock:
                outcome = apply_mark_update(self._marks, self._positions, event)
                if outcome == CONFLICT:
                    self._sticky_reason = "market_data_conflict:%s:mark_price" % event.symbol
            if outcome == CONFLICT:
                raise ValueError("MarkPrice 相同时间戳出现冲突载荷")
            if outcome != ACCEPTED:
                return
            await self._protect_position(event)
            if self.config.mode == "paper":
                self._refresh_paper_account()
            return
        if isinstance(event, Kline):
            await self._handle_kline(event)
            return
        raise TypeError("不支持的市场事件: %s" % type(event).__name__)
    async def _handle_kline(self, bar: Kline) -> None:
        if not bar.closed or bar.close_time >= self._exchange_now():
            return
        strategy = self._strategies.get(bar.symbol)
        if strategy is None:
            raise RuntimeError("%s 策略尚未水合" % bar.symbol)
        before_gaps = strategy.snapshot().gap_resets
        try:
            raw_signal = strategy.update(bar)
        except ValueError as exc:
            with self._lock:
                self._sticky_reason = "market_data_conflict:%s:%s" % (
                    bar.symbol,
                    type(exc).__name__,
                )
            raise
        if strategy.snapshot().gap_resets > before_gaps:
            self._record_event("strategy", "kline_gap_rehydrate", "warning", bar.symbol)
            strategy.hydrate(await self._history(bar.symbol))
            return
        if raw_signal is None:
            return
        age = Decimal(str((self._exchange_now() - bar.close_time).total_seconds()))
        if age < ZERO or age > self.config.signal_max_age_seconds:
            self._record_event("signal", "stale_closed_bar_signal", "warning", bar.symbol)
            return
        await self._enter_from_signal(raw_signal)
    async def _enter_from_signal(self, signal: TradeSignal) -> None:
        now = self._exchange_now()
        durable = durable_order_symbols(self._state_store)
        with self._lock:
            reason = self._readiness_reason(now)
            book = self._books.get(signal.symbol)
            account = self._account
            rules = self._rules.get(signal.symbol)
            position_count = len(self._positions)
            blocked = signal.symbol in (self._pending_symbols | self._uncertain_symbols | durable) or rest_cooling_down(self.config.mode, self.client)
        if not self.config.trading_enabled:
            self._record_event("signal", "trading_disabled", "info", signal.symbol)
            return
        if reason or blocked or book is None or account is None or rules is None:
            self._record_event("risk", reason or "entry_pending", "warning", signal.symbol)
            return
        entry = book.ask_price if signal.direction is Direction.LONG else book.bid_price
        distance = signal.atr * self.config.atr_stop_multiple
        try:
            stop, target = protection_prices(
                entry, signal.direction, distance, self.config.reward_risk_ratio, rules
            )
        except ValueError:
            self._record_event("risk", "INVALID_PROTECTION_GEOMETRY", "warning", signal.symbol)
            return
        decision = self._risk.evaluate(
            equity=account.equity,
            entry_price=entry,
            stop_price=stop,
            book=book,
            exchange_filters=rules,
            day_start_equity=account.day_start_equity,
            open_positions=position_count,
            now=now,
        )
        if not decision.allowed:
            self._record_event("risk", decision.reason, "warning", signal.symbol)
            return
        required_margin = decision.notional / Decimal(self.config.max_leverage)
        if required_margin > account.available_balance:
            self._record_event("risk", "INSUFFICIENT_AVAILABLE_BALANCE", "warning", signal.symbol)
            return
        slippage = self.config.entry_slippage_bps / TEN_THOUSAND
        raw_limit = entry * (Decimal("1") + slippage)
        if signal.direction is Direction.SHORT:
            raw_limit = entry * (Decimal("1") - slippage)
        limit_price = rules.price_for_side(raw_limit, signal.direction.entry_side)
        limit_geometry = (
            signal.direction is Direction.LONG and stop < limit_price < target
        ) or (
            signal.direction is Direction.SHORT and target < limit_price < stop
        )
        if not limit_geometry:
            self._record_event("risk", "INVALID_ENTRY_GEOMETRY", "warning", signal.symbol)
            return
        client_id = normalize_client_order_id(
            "e-%s-%d-%s" % (signal.symbol, signal.bar_start_time, signal.direction.value[0])
        )
        intent = OrderIntent(
            client_order_id=client_id,
            symbol=signal.symbol,
            direction=signal.direction,
            side=signal.direction.entry_side,
            quantity=decision.quantity,
            order_type="LIMIT",
            limit_price=limit_price,
            reduce_only=False,
            created_at=now,
            signal_id=signal.signal_id,
            stop_price=stop,
            target_price=target,
            reason="ONE_MINUTE_EMA_CROSS",
        )
        with self._lock:
            self._pending_symbols.add(signal.symbol)
        try:
            result = await asyncio.to_thread(self._submit, intent, entry)
            await self._apply_entry_result(intent, result, distance)
        finally:
            with self._lock:
                self._pending_symbols.discard(signal.symbol)
    def _submit(self, intent: OrderIntent, paper_price: Decimal) -> ExecutionResult:
        if self._dispatcher is None:
            raise RuntimeError("durable dispatcher 未初始化")
        with self._lock:
            marks = {symbol: mark.mark_price for symbol, mark in self._marks.items()}
        return self._dispatcher.submit(
            intent,
            paper_price=paper_price,
            mark_prices=marks,
        )
    async def _apply_entry_result(
        self, intent: OrderIntent, result: ExecutionResult, distance: Decimal
    ) -> None:
        if result.submission_unknown or result.status == "UNKNOWN":
            self._mark_uncertain(intent.symbol, result.reason or "SUBMISSION_UNCERTAIN")
            return
        if result.executed_quantity <= ZERO:
            if result.status not in TERMINAL_NO_FILL:
                self._mark_uncertain(intent.symbol, "ENTRY_NOT_TERMINAL:%s" % result.status)
            else:
                if self._state_store is None:
                    raise RuntimeError("runtime state store 未初始化")
                self._state_store.mark_applied(intent.client_order_id, result.observed_at)
                self._record_event("order", "entry_not_filled:%s" % result.status, "info", intent.symbol)
                self._clear_uncertain(intent.symbol)
            return
        if result.average_price <= ZERO:
            self._mark_uncertain(intent.symbol, "ENTRY_FILL_WITHOUT_PRICE")
            return
        plan = build_filled_position_plan(
            intent=intent,
            result=result,
            distance=distance,
            reward_risk_ratio=self.config.reward_risk_ratio,
            rules=self._rules[intent.symbol],
        )
        if self._state_store is None:
            raise RuntimeError("runtime state store 未初始化")
        protection_legs = (
            build_protection_legs(plan) if self.config.mode == "testnet" else ()
        )
        try:
            self._state_store.apply_entry_plan(
                intent.client_order_id,
                plan,
                protection_legs=protection_legs,
            )
        except Exception as exc:
            self._mark_uncertain(intent.symbol, "PLAN_PERSIST_FAILED:%s" % type(exc).__name__)
            raise
        self._record_event("order", "entry_filled", "info", intent.symbol)
        self._clear_uncertain(intent.symbol)
        if self.config.mode == "paper":
            self._refresh_paper_account()
        else:
            try:
                await self._refresh_testnet_account()
            except Exception as exc:
                with self._lock:
                    self._reconcile_reason = "protection_sync_deferred:%s:%s" % (plan.symbol, type(exc).__name__)
                await self._fail_closed_exit(
                    plan, "PROTECTION_SYNC_FAILED:%s" % type(exc).__name__
                )
                raise
            with self._lock:
                protection_reason = self._reconcile_reason
            if protection_reason and not protection_reason.startswith(
                ("order_pending:", "protection_exit_pending:")
            ):
                await self._fail_closed_exit(plan, protection_reason)
    async def _claim_local_protection(self, plan: PositionPlan) -> None:
        if self.config.mode != "testnet" or self._protection is None:
            return
        try:
            await asyncio.to_thread(self._protection.claim_local_exit, plan)
        except Exception as exc:
            self._record_event(
                "protection",
                "local_winner_claim_failed:%s" % type(exc).__name__,
                "error",
                plan.symbol,
            )
    async def _fail_closed_exit(self, plan: PositionPlan, reason: str) -> None:
        if rest_cooling_down(self.config.mode, self.client):
            self._record_event("protection", "fail_closed_exit_deferred:rest_cooldown", "warning", plan.symbol)
            return
        self._record_event("protection", "fail_closed_exit:%s" % reason, "error", plan.symbol)
        await self._claim_local_protection(plan)
        await self._exit(plan, "PROTECTION_FAIL", plan.entry_price)
    async def _protect_position(self, mark: MarkPrice) -> None:
        durable = durable_order_symbols(self._state_store)
        with self._lock:
            plan = self._plans.get(mark.symbol)
            pending = mark.symbol in self._pending_symbols
            uncertain = mark.symbol in self._uncertain_symbols or mark.symbol in durable
        if plan is None or pending or uncertain:
            return
        if plan.direction is Direction.LONG:
            reason = "STOP" if mark.mark_price <= plan.stop_price else (
                "TARGET" if mark.mark_price >= plan.target_price else ""
            )
        else:
            reason = "STOP" if mark.mark_price >= plan.stop_price else (
                "TARGET" if mark.mark_price <= plan.target_price else ""
            )
        if not reason or rest_cooling_down(self.config.mode, self.client):
            return
        await self._claim_local_protection(plan)
        await self._exit(plan, reason, mark.mark_price)
    async def _exit(self, plan: PositionPlan, reason: str, fallback_price: Decimal) -> None:
        now = self._exchange_now()
        if self._state_store is None:
            raise RuntimeError("runtime state store 未初始化")
        attempt = self._state_store.exit_attempt_count(plan.symbol, plan.signal_id) + 1
        intent = build_exit_intent(plan, reason, now, attempt)
        with self._lock:
            self._pending_symbols.add(plan.symbol)
            book = self._books.get(plan.symbol)
        paper_price = fallback_price
        if book is not None:
            paper_price = book.bid_price if plan.direction is Direction.LONG else book.ask_price
        try:
            result = await asyncio.to_thread(self._submit, intent, paper_price)
            await self._apply_exit_result(plan, result, reason)
        finally:
            with self._lock:
                self._pending_symbols.discard(plan.symbol)
    async def _apply_exit_result(
        self, plan: PositionPlan, result: ExecutionResult, reason: str
    ) -> None:
        if result.submission_unknown or result.status == "UNKNOWN":
            self._mark_uncertain(plan.symbol, result.reason or "EXIT_SUBMISSION_UNCERTAIN")
            return
        if self._state_store is None:
            raise RuntimeError("runtime state store 未初始化")
        if result.executed_quantity <= ZERO:
            if result.status not in TERMINAL_NO_FILL:
                self._mark_uncertain(plan.symbol, "EXIT_NOT_TERMINAL:%s" % result.status)
                return
            self._state_store.apply_exit_fill(
                result.client_order_id,
                plan.symbol,
                plan.entry_order_id,
                plan.quantity,
                result.observed_at,
            )
            protected = self.config.mode == "testnet" and self._state_store.get_protection_set(plan.entry_order_id) is not None
            if result.reason == RETRYABLE_SERVER_FAILURE or reason == "PROTECTION_FAIL" or protected:
                self._record_event("order", "exit_definitive_no_fill_retry", "warning", plan.symbol)
                self._clear_uncertain(plan.symbol)
                return
            self._mark_uncertain(plan.symbol, "EXIT_NOT_FILLED:%s" % result.status)
            return
        remaining = max(plan.quantity - result.executed_quantity, ZERO)
        self._state_store.apply_exit_fill(
            result.client_order_id,
            plan.symbol,
            plan.entry_order_id,
            remaining,
            result.observed_at,
        )
        self._record_event("order", "exit_filled:%s" % reason, "info", plan.symbol)
        self._clear_uncertain(plan.symbol)
        if self.config.mode == "paper":
            self._refresh_paper_account()
        else:
            await self._refresh_testnet_account()
    def _mark_uncertain(self, symbol: str, reason: str) -> None:
        message = "order_state_unknown:%s:%s" % (symbol, reason)
        durable = durable_order_symbols(self._state_store)
        durable.add(symbol)
        with self._lock:
            self._uncertain_symbols = durable
            self._order_unknown_reason = message
        self._record_event("order", message, "error", symbol)
    def _clear_uncertain(self, _symbol: str) -> None:
        durable = durable_order_symbols(self._state_store)
        reason = order_unknown_reason(durable)
        with self._lock:
            self._uncertain_symbols = durable
            self._order_unknown_reason = reason
    def _record_event(
        self, kind: str, message: str, level: str = "info", symbol: Optional[str] = None
    ) -> None:
        event = {
            "timestamp": self._exchange_now(),
            "kind": kind,
            "level": level,
            "message": str(message),
        }
        if symbol:
            event["symbol"] = symbol
        with self._lock:
            self._events.append(event)
    def _exchange_now(self) -> datetime:
        return self.clock() + timedelta(milliseconds=self._clock_sync.offset_ms)
    # ── read model ────────────────────────────────────────────────────
    def _readiness_reason(self, now: datetime) -> str:
        return readiness_reason(
            config=self.config,
            now=now,
            started=self._started,
            sticky_reason=self._sticky_reason or self._order_unknown_reason or ("rest_rate_limit_cooldown" if rest_cooling_down(self.config.mode, self.client) else ""),
            bootstrapped=self._bootstrapped,
            bootstrap_reason=self._bootstrap_reason,
            reconcile_reason=self._reconcile_reason,
            account=self._account,
            strategies=self._strategies,
            books=self._books,
            marks=self._marks,
        )
    def ready(self) -> bool:
        with self._lock:
            return self._readiness_reason(self._exchange_now()) == ""
    is_ready = ready
    def snapshot(self) -> Dict[str, Any]:
        now = self._exchange_now()
        with self._lock:
            readiness_reason = self._readiness_reason(now)
            primary = self.config.symbols[0]
            book = self._books.get(primary)
            mark = self._marks.get(primary)
            account = self._account
            positions = list(self._positions.values())
            events = list(self._events)
            ledger_snapshot = dict(self._ledger_snapshot)
            strategies = {
                symbol: strategy.snapshot() for symbol, strategy in self._strategies.items()
            }
            protection = self._protection.snapshot(primary) if self._protection else {"state": "LOCAL_ONLY", "winner": None, "legs": []}
        return dict(
            build_runtime_snapshot(
                config=self.config,
                now=now,
                readiness=readiness_reason,
                book=book,
                mark=mark,
                account=account,
                positions=positions,
                ledger=ledger_snapshot,
                protection=protection,
                strategies=strategies,
                events=events,
            )
        )
def create_runtime(config: BinanceConfig) -> BinanceRuntime:
    return BinanceRuntime(config)
__all__ = ["BinanceRuntime", "create_runtime"]
