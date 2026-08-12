"""
[INPUT]: 依赖 Binance runtime 测试所需的 UTC 时钟、公开行情转换契约与可控 testnet 账户/交易所行为
[OUTPUT]: 提供 NOW、PublicDemoClient、UnusedStream、TestnetAccountClient 与 TestnetTradingClient 共享测试桩
[POS]: python/tests 的非发现型 runtime fixture 模块，统一 paper 组合回归与 testnet 安全回归的确定性边界
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal


NOW = datetime(2026, 8, 11, 12, 0, 30, tzinfo=timezone.utc)


class PublicDemoClient:
    def __init__(self) -> None:
        self.rows = self._rows()

    @staticmethod
    def _rows():
        rows = []
        first = NOW.replace(second=0, microsecond=0) - timedelta(minutes=49)
        for index in range(50):
            opened = first + timedelta(minutes=index)
            price = Decimal("59000") + Decimal(index * 20)
            rows.append(
                [
                    int(opened.timestamp() * 1000),
                    str(price),
                    str(price + 25),
                    str(price - 25),
                    str(price + 10),
                    "12",
                    int((opened + timedelta(seconds=59, milliseconds=999)).timestamp() * 1000),
                    "720000",
                    100,
                    "0",
                    "0",
                ]
            )
        return rows

    def sync_time(self) -> int:
        return 0

    def exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                            "maxQty": "100",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

    def klines(self, symbol: str, interval: str, limit: int):
        self.last_request = (symbol, interval, limit)
        return self.rows


class UnusedStream:
    async def events(self):
        if False:
            yield None


class TestnetAccountClient(PublicDemoClient):
    def __init__(self, open_orders):
        super().__init__()
        self._open_orders = list(open_orders)
        self.open_orders_error = None
        self.config_changes = []

    def position_mode(self):
        return False

    def change_margin_type(self, symbol, margin_type):
        self.config_changes.append(("margin", symbol, margin_type))
        return {"symbol": symbol, "marginType": margin_type}

    def change_leverage(self, symbol, leverage):
        self.config_changes.append(("leverage", symbol, leverage))
        return {"symbol": symbol, "leverage": leverage}

    def account(self):
        return {
            "totalWalletBalance": "10000",
            "totalUnrealizedProfit": "0",
            "totalMarginBalance": "10000",
            "availableBalance": "10000",
        }

    def positions(self):
        return []

    def open_orders(self):
        if self.open_orders_error is not None:
            raise self.open_orders_error
        return list(self._open_orders)

    def open_algo_orders(self):
        return []

    def new_algo_order(self, params):
        raise AssertionError("空仓测试不得创建 Algo order")

    def query_algo_order(self, client_algo_id=None, algo_id=None):
        raise AssertionError("空仓测试不得查询 Algo order")

    def cancel_algo_order(self, client_algo_id=None, algo_id=None):
        raise AssertionError("空仓测试不得撤销 Algo order")


class TestnetTradingClient(TestnetAccountClient):
    def __init__(self):
        super().__init__([])
        self.position_amount = Decimal("0")
        self.entry_price = Decimal("0")
        self.mark_price = Decimal("60000")
        self.normal_orders = {}
        self.normal_submissions = []
        self.algo_orders = {}
        self.algo_submissions = []
        self.algo_cancellations = []

    def positions(self):
        return [
            {
                "symbol": "BTCUSDT",
                "positionSide": "BOTH",
                "positionAmt": str(self.position_amount),
                "entryPrice": str(self.entry_price),
                "markPrice": str(self.mark_price),
                "liquidationPrice": "30000" if self.position_amount else "0",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            }
        ]

    def new_order(self, params):
        params = dict(params)
        self.normal_submissions.append(params)
        quantity = Decimal(params["quantity"])
        price = Decimal(params.get("price") or self.mark_price)
        if params.get("reduceOnly"):
            executed = min(quantity, abs(self.position_amount))
            self.position_amount = Decimal("0")
        else:
            executed = quantity
            self.position_amount = quantity if params["side"] == "BUY" else -quantity
            self.entry_price = price
        payload = {
            "clientOrderId": params["newClientOrderId"],
            "orderId": len(self.normal_submissions),
            "status": "FILLED",
            "executedQty": str(executed),
            "avgPrice": str(price),
            "updateTime": int(NOW.timestamp() * 1000),
        }
        self.normal_orders[params["newClientOrderId"]] = payload
        return dict(payload)

    def query_order(self, symbol, client_order_id):
        self.assert_symbol = symbol
        return dict(self.normal_orders[client_order_id])

    @staticmethod
    def _algo_payload(params, status="NEW"):
        return {
            "algoId": len(params["clientAlgoId"]) + 100,
            "clientAlgoId": params["clientAlgoId"],
            "algoType": "CONDITIONAL",
            "symbol": params["symbol"],
            "side": params["side"],
            "positionSide": "BOTH",
            "orderType": params["type"],
            "triggerPrice": params["triggerPrice"],
            "workingType": "MARK_PRICE",
            "closePosition": True,
            "priceProtect": False,
            "algoStatus": status,
        }

    def new_algo_order(self, params):
        params = dict(params)
        self.algo_submissions.append(params)
        payload = self._algo_payload(params)
        self.algo_orders[params["clientAlgoId"]] = payload
        return dict(payload)

    def query_algo_order(self, client_algo_id=None, algo_id=None):
        del algo_id
        return dict(self.algo_orders[client_algo_id])

    def cancel_algo_order(self, client_algo_id=None, algo_id=None):
        del algo_id
        self.algo_cancellations.append(client_algo_id)
        payload = dict(self.algo_orders[client_algo_id])
        payload["algoStatus"] = "CANCELED"
        self.algo_orders[client_algo_id] = payload
        return {
            "algoId": payload["algoId"],
            "clientAlgoId": payload["clientAlgoId"],
            "code": "200",
            "msg": "success",
        }

    def open_algo_orders(self):
        return [
            dict(row)
            for row in self.algo_orders.values()
            if row["algoStatus"] == "NEW"
        ]


__all__ = [
    "NOW",
    "PublicDemoClient",
    "TestnetAccountClient",
    "TestnetTradingClient",
    "UnusedStream",
]
