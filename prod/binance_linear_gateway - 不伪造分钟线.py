import json
import urllib
import hashlib
import hmac
import time
import sys
from copy import copy
from enum import Enum
from time import sleep
from datetime import datetime, timedelta
# from asyncio import run_coroutine_threadsafe
# https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
from threading import Lock

# from aiohttp import ClientSSLError
# https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
from numpy import format_float_positional
from vnpy_evo.trader.database import DB_TZ
# from vnpy.usertools.task_db_manager import task_db_manager

from vnpy_evo.event import Event, EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    Offset,
    OrderType,
    Interval
)
from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy_evo.trader.event import EVENT_TIMER
from vnpy_evo.trader.utility import round_to, ZoneInfo
# from vnpy_rest import Request, RestClient, Response
from .rest_client import Request, RestClient, Response
# from vnpy_websocket import WebsocketClient
# from vnpy_evo.rest import Request, RestClient, Response
# from vnpy_evo.websocket import WebsocketClient
# from .qfw.websocket_client import WebsocketClient
from .websocket_client import WebsocketClient
from .barGen_redis_engine import EVENT_BAR

# Timezone constant
UTC_TZ = ZoneInfo("UTC")

# Real server hosts
F_REST_HOST: str = "https://fapi.binance.com"
F_WEBSOCKET_TRADE_HOST: str = "wss://fstream.binance.com/ws/"
F_WEBSOCKET_DATA_HOST: str = "wss://fstream.binance.com/stream"

# Testnet server hosts
F_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"
F_TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://stream.binancefuture.com/ws/"
F_TESTNET_WEBSOCKET_DATA_HOST: str = "wss://stream.binancefuture.com/stream"

# Order status map
STATUS_BINANCES2VT: dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED
}

# Order type map
ORDERTYPE_VT2BINANCES: dict[OrderType, tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_BINANCES2VT: dict[tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCES.items()}

# Direction map
DIRECTION_VT2BINANCES: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCES2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCES.items()}

# Kline interval map
INTERVAL_VT2BINANCES: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# Timedelta map
TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# Set weboscket timeout to 24 hour
WEBSOCKET_TIMEOUT = 24 * 60 * 60

# Global dict for contract data
symbol_contract_map: dict[str, ContractData] = {}


# Authentication level
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinanceLinearGateway(BaseGateway):
    """
    The Binance linear trading gateway for VeighNa.

    1. Only support crossed position
    2. Only support one-way mode
    """

    default_name: str = "BINANCE_LINEAR"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL", "TESTNET"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: BinanceLinearTradeWebsocketApi = BinanceLinearTradeWebsocketApi(self)
        self.market_ws_api: BinanceLinearDataWebsocketApi = BinanceLinearDataWebsocketApi(self)
        self.rest_api: BinanceLinearRestApi = BinanceLinearRestApi(self)
        
        self.orders: dict[str, OrderData] = {}
        self.positions: dict[str, PositionData] = {}
        self.incomes: dict[str, dict[int, dict]] = {}
        self.get_server_time_interval: int = 0
        
        self.trade_history_ready: bool = False
        self.trade_history_url: str = ""

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.market_ws_api.connect(server, kline_stream, proxy_host, proxy_port)

        self.event_engine.unregister(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.market_ws_api.subscribe(req)

    def subscribe_reqs(self, reqs: list[SubscribeRequest]) -> None:
        """Subscribe multiple symbols in one websocket request"""
        self.market_ws_api.subscribe_reqs(reqs)

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """Unsubscribe market data"""
        self.market_ws_api.unsubscribe(req)

    def unsubscribe_reqs(self, reqs: list[SubscribeRequest]) -> None:
        """Unsubscribe multiple symbols in one websocket request"""
        self.market_ws_api.unsubscribe_reqs(reqs)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.rest_api.cancel_order(req)
        self.write_log(f"取消订单 {req.orderid}")

    def query_account(self) -> None:
        """query account"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """query position"""
        self.rest_api.query_position()

    def query_funding_rate(self) -> None:
        """query premium rate/index"""
        self.rest_api.query_funding_rate()

    def query_latest_kline(self, req: HistoryRequest) -> None:
        self.rest_api.query_latest_kline(req)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """query historical kline data"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """Process timer task"""
        self.rest_api.keep_user_stream()
        self.get_server_time_interval += 1

        if self.get_server_time_interval >= 200:
            self.rest_api.query_time()
            # self.rest_api.query_position()
            self.get_server_time_interval = 0

    def on_order(self, order: OrderData) -> None:
        """on order update"""
        order.update_time = generate_datetime(time.time() * 1000)
        self.orders[order.orderid] = order
        super().on_order(copy(order))

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)

    def on_position(self, position: PositionData) -> None:
        self.positions[position.symbol] = position
        super().on_position(position)

    def get_position(self, symbol: str):
        return self.positions.get(symbol, None)

    def on_income(self, data: dict) -> None:
        """on income update"""
        if data["symbol"] not in self.incomes:
            self.incomes[data["symbol"]] = {}
        self.incomes[data["symbol"]][data["tranId"]] = data

    def on_bar(self, bar: BarData) -> None:
        """
        Bar event push.
        """
        self.on_event(EVENT_BAR, bar)
        self.on_event(EVENT_BAR + bar.vt_symbol, bar)

    def on_trade_history_url(self, url: str) -> None:
        """Callback when trade history url is received"""
        self.trade_history_url = url
        self.trade_history_ready = True

    def write_log(self, msg: str) -> None:
        """
        Write a log event from gateway with caller information.
        """
        if msg.startswith("["):
            super().write_log(msg)
        else:
            func_name = sys._getframe(1).f_code.co_name
            class_name = self.__class__.__name__
            formatted_msg = f"[{class_name}.{func_name}] {msg}"
            super().write_log(formatted_msg)


class BinanceLinearRestApi(RestClient):
    """Binance USDT/BUSD future rest api"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceLinearTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.keep_alive_failed_count: int = 0
        self.recv_window: int = 5000 * 10
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.order_prefix: str = ""

    def sign(self, request: Request) -> Request:
        """Standard callback for signing a request"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            signature: bytes = hmac.new(
                self.secret,
                query.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            query += "&signature={}".format(signature)
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # Add header to the request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = datetime.now().strftime("%y%m%d%H%M%S")

        if self.server == "REAL":
            self.init(F_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()

        self.write_log("REST API started")

        self.query_time()
        self.query_position_side()
        self.query_contract()
        self.start_user_stream()

    def query_time(self) -> None:
        """Query server time"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/time"

        self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            on_failed=self.on_query_time_failed,
            on_error=self.on_query_time_error,
            data=data
        )

    def query_account(self) -> None:
        """Query account balance"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v3/account"
        # https://github.com/veighna-global/vnpy_binance/commit/378a61dcb6e2e1c750d9874ad766a2bba5f6e876

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )

    def query_position_side(self):
        """query position side/mode"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/positionSide/dual"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position_side,
            data=data
        )
    
    def query_position(self) -> None:
        """Query holding positions"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v3/positionRisk"
        # https://github.com/veighna-global/vnpy_binance/commit/378a61dcb6e2e1c750d9874ad766a2bba5f6e876

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

    def query_income(self) -> None:
        """query account income history
        https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/account/rest-api/Get-Income-History
        """
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/income"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_income,
            data=data
        )

    def query_order(self) -> None:
        """Query open orders"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self) -> None:
        """Query available contracts"""
        data: dict = {"security": Security.NONE}

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def query_funding_rate(self) -> None:
        data = {
            "security": Security.NONE
        }

        path = "/fapi/v1/premiumIndex"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_funding_rate,
            data=data
        )

    def _new_order_id(self) -> int:
        """generate customized order id"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        # Generate new order id
        # self.order_count += 1
        orderid: str = self.order_prefix + str(self._new_order_id())

        # Push a submitting order event
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        # Create order parameters
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINANCES[req.direction],
            "quantity": format_float(req.volume),
            "newClientOrderId": orderid,
            "newOrderRespType": "RESULT"
        }

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"
        elif req.type == OrderType.STOP:
            params["type"] = "STOP_MARKET"
            params["stopPrice"] = format_float(req.price)
        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = format_float(req.price)
        
        if req.offset == Offset.CLOSE:
            params['reduceOnly'] = True

        path: str = "/fapi/v1/order"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": req.symbol,
            "origClientOrderId": req.orderid
        }

        path: str = "/fapi/v1/order"

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def start_user_stream(self) -> None:
        """Create listen key for user stream"""
        data: dict = {"security": Security.API_KEY}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
            on_failed=self.on_start_user_stream_failed,
            on_error=self.on_start_user_stream_error,
            data=data
        )

    def keep_user_stream(self) -> None:
        """Extend listen key validity"""
        if not self.user_stream_key:
            # [Mod] only keep user stream when key is provided
            # https://github.com/veighna-global/vnpy_binance/commit/800989fac92d13181aba626b46fe43571811ce93
            return

        self.keep_alive_count += 1
        if self.keep_alive_count < 300:
            return None
        self.keep_alive_count = 0

        data: dict = {"security": Security.API_KEY}

        params: dict = {"listenKey": self.user_stream_key}

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_failed=self.on_keep_user_stream_failed,
            on_error=self.on_keep_user_stream_error
        )

    def on_query_time(self, data: dict, request: Request) -> None:
        """Callback of server time query"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

        # self.write_log(f"Server time updated, local offset: {self.time_offset}ms")

        # Query private data after time offset is calculated
        if self.key and self.secret:
            self.query_account()
            self.query_position()
            self.query_order() #remove this 0917
            self.start_user_stream()
    
    def on_query_time_failed(self, status_code: int, request: Request):
        self.query_time()

    def on_query_time_error(self,  exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        self.query_time()

    def on_query_account(self, data: dict, request: Request) -> None:
        """Callback of account balance query"""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"],
                # balance=float(asset["walletBalance"]),
                balance=float(asset["marginBalance"]),
                # frozen=float(asset["maintMargin"]),
                frozen=float(asset["initialMargin"]),
                # https://binance-docs.github.io/apidocs/futures/cn/#v3-user_data-2
                gateway_name=self.gateway_name
            )
            # https://github.com/veighna-global/vnpy_binance/commit/378a61dcb6e2e1c750d9874ad766a2bba5f6e876
            self.gateway.on_account(account)

        # self.write_log("Account balance data is received")

    def on_query_position_side(self, data: dict, request: Request) -> None:
        if data.get("dualSidePosition", False):  # true will means dual position side
            self.set_position_side()

    def set_position_side(self) -> None:
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/positionSide/dual"

        params: dict = {
            "dualSidePosition": False
        }

        self.add_request(
            method="POST",
            path=path,
            params=params,
            callback=self.on_set_position_side,
            data=data
        )

    def on_set_position_side(self, data: dict, request: Request) -> None:
        self.write_log("set position side to one-way mode")

    def on_query_position(self, data: list, request: Request) -> None:
        """Callback of holding positions query"""
        for d in data:
            # self.write_log(f"pos data received raw: {d}")
            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                direction=Direction.NET,
                volume=float(d["positionAmt"]),
                frozen=float(d["initialMargin"]), # https://binance-docs.github.io/apidocs/futures/cn/#v3-user_data-3
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )
            # self.write_log(f"pos data received: {position}")

            # https://github.com/veighna-global/vnpy_binance/commit/378a61dcb6e2e1c750d9874ad766a2bba5f6e876
            self.gateway.on_position(position)
            # self.write_log(f" call on_position {position}")

        # self.write_log("Holding positions data is received")

    def on_query_income(self, data: list, request: Request) -> None:
        """Callback of account income history query"""
        for d in data:
            if d.get("incomeType") not in ("COMMISSION", "FUNDING_FEE"):
                continue

            # self.write_log(f"income data received: {d}")
            d["time"] = generate_datetime(d["time"])
            d["income"] = float(d["income"])
            self.gateway.on_income(d)

    def on_query_order(self, data: list, request: Request) -> None:
        """Callback of open orders query"""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
            if not order_type:
                continue

            order: OrderData = OrderData(
                orderid=d["clientOrderId"],
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=float(d["price"]),
                volume=float(d["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCES2VT[d["side"]],
                traded=float(d["executedQty"]),
                status=STATUS_BINANCES2VT.get(d["status"], None),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.write_log("Open orders data is received")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    pricetick = float(f["tickSize"])
                elif f["filterType"] == "LOT_SIZE":
                    min_volume = float(f["stepSize"])
                elif f["filterType"] == "MIN_NOTIONAL":
                    min_notional = float(f["notional"])

            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            contract.extra = {
                "min_notional": min_notional if min_notional else 0
            }
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.write_log("Available contracts data is received")

    def on_send_order(self, data: dict, request: Request) -> None:
        """Successful callback of send_order"""
        self.write_log(f"订单发送成功，订单号：{request.extra.orderid}，交易所返回：{data}")

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of send_order"""
        self.failed_with_timestamp(request)
        if request.extra:
            order: OrderData = copy(request.extra)
            order.status = Status.REJECTED
            order.rejected_reason = request.response.text if request.response.text else ""
            self.gateway.on_order(order)

            msg: str = f"send order failed, orderid: {order.orderid} {order.symbol}, status code:{status_code}, msg:{request.response.text}"
            self.write_log(msg)
            # task_db_manager.report_alert(content=msg, assigned_to="vigar")

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of send_order"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, ConnectionError):
        # https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Successful callback of cancel_order"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of cancel_order"""
        if request.extra:
            # order = request.extra
            # order.status = Status.REJECTED
            # self.gateway.on_order(order)
            # self.query_order()
            pass

        msg: str = f"Cancel order failed, status code: {status_code} , message: {request.response.text}, order: {request.extra} "
        self.write_log(msg)
        # task_db_manager.report_alert(content=msg, assigned_to="vigar")

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """Successful callback of start_user_stream"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0

        if self.server == "REAL":
            url = F_WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = F_TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        # 检查是否需要连接trade_ws_api
        if not self.trade_ws_api._ws and not self.trade_ws_api._connecting:
            self.write_log(f"连接trade_ws_api")
            self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)
        
        # 检查是否需要连接market_ws_api
        if not self.gateway.market_ws_api._ws and not self.gateway.market_ws_api._connecting:
            self.write_log(f"连接market_ws_api")
            self.gateway.market_ws_api.connect(self.server, self.gateway.market_ws_api.kline_stream, self.proxy_host, self.proxy_port)

    def on_start_user_stream_failed(self, status_code: int, request: Request):
        self.failed_with_timestamp(request)
        self.start_user_stream()

    def on_start_user_stream_error(self, exception_type: type, exception_value: Exception, tb, request: Request):
        self.start_user_stream()

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """extend the listen key expire time"""
        self.keep_alive_failed_count = 0

    def on_keep_user_stream_failed(self, status_code: int, request: Request):
        self.failed_with_timestamp(request)
        self.keep_alive_failed_count += 1
        if self.keep_alive_failed_count <= 3:
            self.keep_alive_count = 1200000
            self.keep_user_stream()
        else:
            self.keep_alive_failed_count = 0
            self.start_user_stream()

    def on_keep_user_stream_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """put the listen key failed"""
        self.keep_alive_failed_count += 1
        if self.keep_alive_failed_count <= 3:
            self.keep_alive_count = 1200000
            self.keep_user_stream()
        else:
            self.keep_alive_failed_count = 0
            self.start_user_stream()

        if not issubclass(exception_type, TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []
        limit: int = 1500

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # Create query parameters
            params: dict = {
                "symbol": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit
            }

            params["startTime"] = start_time * 1000
            path: str = "/fapi/v1/klines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000     # Convert to milliseconds

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # Break the loop if request failed
            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"No kline history data is received, start time: {start_time}"
                    self.write_log(msg)
                    break

                buf: list[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    bar.extra = {
                        "trade_count": int(row[8]),
                        "active_volume": float(row[9]),
                        "active_turnover": float(row[10]),
                    }
                    # https://github.com/veighna-global/vnpy_binance/commit/c830c4f06276e35feb2b08254900df8978c4e6ac
                    # https://github.com/veighna-global/vnpy_binance/commit/a5f9c98c3a571d99e630740b62eea13eca6fb5ca
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.write_log(msg)

                # Break the loop if the latest data received
                # https://github.com/veighna-global/vnpy_binance/commit/45190e9813baf419c53dc2b13bc726da0e233bb3
                if (
                    len(data) < limit
                    or (req.end and end >= req.end)
                ):
                    break

                # Update query start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            # Wait to meet request flow limit
            sleep(0.5)

        # Remove the unclosed kline
        if history:
            history.pop(-1)

        return history

    def failed_with_timestamp(self, request: Request):
        # request.response.text
        # -1021 INVALID_TIMESTAMP
        try:
            if request and request.response and request.response.text:
                resp = json.loads(request.response.text)
                if resp.get('code') == -1021:
                    self.query_time()
        except Exception:
            pass

    def query_trade_history_id(self) -> None:
        """Query trade history download id for last month"""
        data: dict = {"security": Security.SIGNED}
        
        # 使用UTC时间
        end_time = int(datetime.now(UTC_TZ).timestamp() * 1000)
        start_time = int((datetime.now(UTC_TZ) - timedelta(days=30)).timestamp() * 1000)
        
        params: dict = {
            "startTime": start_time,
            "endTime": end_time
        }
        
        path: str = "/fapi/v1/trade/asyn"
        
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_trade_history_id,
            params=params,
            data=data
        )

    def query_trade_history_url(self, download_id: str) -> None:
        """Query trade history download url by id"""
        data: dict = {"security": Security.SIGNED}
        
        params: dict = {
            "downloadId": download_id
        }
        
        path: str = "/fapi/v1/trade/asyn/id"
        
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_trade_history_url,
            params=params,
            data=data
        )

    def on_query_trade_history_id(self, data: dict, request: Request) -> None:
        """Callback after querying trade history download id"""
        download_id = data["downloadId"]
        self._current_download_id = download_id  # 保存ID供重试使用
        self.query_trade_history_url(download_id)
        self.write_log(f"获取到交易历史下载ID: {download_id}")

    def on_query_trade_history_url(self, data: dict, request: Request) -> None:
        """Callback after querying trade history download url"""
        if data["status"] == "completed":
            self.write_log(f"获取到交易历史下载链接: {data['url']}")  # 记录下载链接
            self.gateway.on_trade_history_url(data["url"])
        else:
            self.write_log(f"交易历史下载链接尚未准备好，状态: {data['status']}")
            sleep(3)  # 等待3秒后重试
            self.query_trade_history_url(self._current_download_id)

    def set_leverage(self, symbol: str, leverage: int) -> None:
        """设置合约杠杆倍数"""
        data: dict = {"security": Security.SIGNED}
        
        params: dict = {
            "symbol": symbol,
            "leverage": leverage
        }
        
        path: str = "/fapi/v1/leverage"
        
        self.add_request(
            method="POST",
            path=path,
            callback=self.on_set_leverage,
            params=params,
            data=data
        )

    def on_set_leverage(self, data: dict, request: Request) -> None:
        """设置杠杆倍数回调"""
        if data:
            msg = f"设置杠杆倍数成功 - {data['symbol']}: {data['leverage']}倍"
            self.write_log(msg)

    def write_log(self, msg: str) -> None:
        """重写日志方法，添加REST API标识"""
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        self.gateway.write_log(formatted_msg)


class BinanceLinearTradeWebsocketApi(WebsocketClient):
    """The trade websocket API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def write_log(self, msg: str) -> None:
        """输出日志"""
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        self.gateway.write_log(formatted_msg)

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        """Start server connection"""
        self.init(url, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.write_log("Trade Websocket API is connected")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        if packet["e"] == "ACCOUNT_UPDATE":
            self.on_account(packet)
        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)
        elif packet["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired"""
        self.write_log("Listen key is expired")
    #     self.disconnect()
    # https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
    # def disconnect(self) -> None:
    #     """"Close server connection"""
    #     self._active = False
    #     ws = self._ws
    #     if ws:
    #         coro = ws.close()
    #         run_coroutine_threadsafe(coro, self._loop)

    def on_account(self, packet: dict) -> None:
        """Callback of account balance and holding position update"""
        for acc_data in packet["a"]["B"]:
            account: AccountData = AccountData(
                accountid=acc_data["a"],
                balance=float(acc_data["wb"]),
                frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            if pos_data["ps"] == "BOTH":
                volume = pos_data["pa"]
                if '.' in volume:
                    volume = float(volume)
                else:
                    volume = int(volume)

                position: PositionData = PositionData(
                    symbol=pos_data["s"],
                    exchange=Exchange.BINANCE,
                    direction=Direction.NET,
                    volume=volume,
                    price=float(pos_data["ep"]),
                    pnl=float(pos_data["up"]),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """Callback of order and trade update"""
        ord_data: dict = packet["o"]
        key: tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        # Get last order for offset and comparison
        last_order = self.gateway.get_order(ord_data["c"])
        offset = last_order.offset if last_order else None

        # Get price and traded price
        price = float(ord_data["p"])
        traded_price = float(ord_data["L"]) if float(ord_data["L"]) > 0 else float(ord_data["ap"])
        if price <= 0:
            price = traded_price

        # Create order data object
        order: OrderData = OrderData(
            symbol=ord_data["s"],
            exchange=Exchange.BINANCE,
            orderid=str(ord_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[ord_data["S"]],
            price=price,
            volume=float(ord_data["q"]),
            traded=float(ord_data["z"]),
            status=STATUS_BINANCES2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
            offset=offset
        )

        # If no last order, always push order update
        if not last_order:
            self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
            self.gateway.on_order(order)
            return

        # Calculate traded change
        traded_change = order.traded - last_order.traded
        status_change = order.status != last_order.status

        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            original_traded_change = traded_change
            traded_change = round_to(traded_change, contract.min_volume)
            if traded_change != original_traded_change:
                self.write_log(f"[warning] {order.symbol} traded_change rounded: {original_traded_change} -> {traded_change}")

        # If no trade change and no status change, skip update
        # if traded_change <= 0 and not status_change:
        #     return
        if traded_change < 0:
            return
        if traded_change == 0 and not status_change:
            return

        self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
        self.gateway.on_order(order)

        # Push trade update on new trade
        if traded_change > 0:
            trade: TradeData = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                orderid=order.orderid,
                tradeid=ord_data["t"],
                direction=order.direction,
                price=float(ord_data["L"]),
                volume=traded_change,
                datetime=generate_datetime(ord_data["T"]),
                gateway_name=self.gateway_name,
                offset=offset
            )
            self.write_log(f"收到成交回报，订单号：{trade.orderid}，成交号：{trade.tradeid}，价格：{trade.price}，数量：{trade.volume}")
            self.gateway.on_trade(trade)

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected"""
        self.write_log(f"Trade Websocket API is disconnected, code: {status_code}, msg: {msg}")
        self.gateway.rest_api.start_user_stream()


class BinanceLinearDataWebsocketApi(WebsocketClient):
    """The data websocket API of BinanceLinearGateway"""

    def __init__(self, gateway: BinanceLinearGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BinanceLinearGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.bars: dict[str, BarData] = {}
        self.reqid: int = 0
        self.kline_stream: bool = False

        # 添加批量订阅控制参数
        self.max_channels_per_request = 200  # 每个请求最大channel数量
        self.subscribe_interval = 0.3  # 订阅请求间隔(秒)

    def write_log(self, msg: str) -> None:
        """输出日志"""
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        self.gateway.write_log(formatted_msg)

    def connect(
        self,
        server: str,
        kline_stream: bool,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""
        self.kline_stream = kline_stream

        if server == "REAL":
            self.init(F_WEBSOCKET_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        else:
            self.init(F_TESTNET_WEBSOCKET_DATA_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.write_log("Data Websocket API is connected")

        # 计算需要订阅的channels
        all_channels = []
        for symbol in self.ticks.keys():
            channels = [
                f"{symbol}@ticker",
                f"{symbol}@depth5"
            ]
            if self.kline_stream:
                channels.append(f"{symbol}@kline_1m")
            all_channels.extend(channels)

        # 发送订阅请求
        if all_channels:
            self._send_subscribe_request(all_channels)
            self.write_log(f"订阅channels: {all_channels}")

    def _send_subscribe_request(self, channels: list) -> None:
        """发送订阅请求"""
        # 分批发送订阅请求
        for i in range(0, len(channels), self.max_channels_per_request):
            batch_channels = channels[i:i + self.max_channels_per_request]
            self.reqid += 1
            req: dict = {
                "method": "SUBSCRIBE",
                "params": batch_channels,
                "id": self.reqid
            }
            self.send_packet(req)
            time.sleep(self.subscribe_interval)

    def _send_unsubscribe_request(self, channels: list) -> None:
        """发送退订请求"""
        # 分批发送退订请求
        for i in range(0, len(channels), self.max_channels_per_request):
            batch_channels = channels[i:i + self.max_channels_per_request]
            self.reqid += 1
            req: dict = {
                "method": "UNSUBSCRIBE",
                "params": batch_channels,
                "id": self.reqid
            }
            self.send_packet(req)
            time.sleep(self.subscribe_interval)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        if req.symbol in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.write_log(f"Symbol not found {req.symbol}")
            return

        # Initialize tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(UTC_TZ),
            gateway_name=self.gateway_name,
        )
        tick.extra = {}
        self.ticks[req.symbol.lower()] = tick

        # Initialize bar object
        bar: BarData = BarData(
            symbol=req.symbol,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(UTC_TZ),
            interval=Interval.MINUTE,
            gateway_name=self.gateway_name
        )
        self.bars[req.symbol.lower()] = bar

        # 构造需要订阅的channels
        channels = [
            f"{req.symbol.lower()}@ticker",
            f"{req.symbol.lower()}@depth5"
        ]
        if self.kline_stream:
            channels.append(f"{req.symbol.lower()}@kline_1m")

        self._send_subscribe_request(channels)
        self.write_log(f"订阅channels: {channels}")

    def unsubscribe(self, req: SubscribeRequest) -> None:
        """Unsubscribe market data"""
        symbol = req.symbol.lower()

        # 构造需要退订的channels
        channels = [
            f"{symbol}@ticker",
            f"{symbol}@depth5"
        ]
        if self.kline_stream:
            channels.append(f"{symbol}@kline_1m")

        # 发送退订请求
        self._send_unsubscribe_request(channels)
        self.write_log(f"退订channels: {channels}")

        # 清除相关数据对象
        if symbol in self.ticks:
            del self.ticks[symbol]
        if symbol in self.bars:
            del self.bars[symbol]

    def subscribe_reqs(self, reqs: list[SubscribeRequest]) -> None:
        """Subscribe multiple symbols in one websocket request"""
        if not reqs:
            return
            
        all_channels = []
        for req in reqs:
            if req.symbol in self.ticks:
                continue

            if req.symbol not in symbol_contract_map:
                self.write_log(f"Symbol not found {req.symbol}")
                continue

            # Initialize tick object
            tick: TickData = TickData(
                symbol=req.symbol,
                name=symbol_contract_map[req.symbol].name,
                exchange=Exchange.BINANCE,
                datetime=datetime.now(UTC_TZ),
                gateway_name=self.gateway_name,
            )
            tick.extra = {}
            self.ticks[req.symbol.lower()] = tick

            # Initialize bar object
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=Exchange.BINANCE,
                datetime=datetime.now(UTC_TZ),
                interval=Interval.MINUTE,
                gateway_name=self.gateway_name
            )
            self.bars[req.symbol.lower()] = bar

            # Add channels for this symbol
            channels = [
                f"{req.symbol.lower()}@ticker",
                f"{req.symbol.lower()}@depth5"
            ]
            if self.kline_stream:
                channels.append(f"{req.symbol.lower()}@kline_1m")
            
            all_channels.extend(channels)

        # 发送订阅请求
        if all_channels:
            self._send_subscribe_request(all_channels)
            self.write_log(f"订阅channels: {all_channels}")

    def unsubscribe_reqs(self, reqs: list[SubscribeRequest]) -> None:
        """Unsubscribe multiple symbols in one websocket request"""
        if not reqs:
            return

        all_channels = []
        for req in reqs:
            symbol = req.symbol.lower()

            # Add channels for this symbol
            channels = [
                f"{symbol}@ticker",
                f"{symbol}@depth5"
            ]
            if self.kline_stream:
                channels.append(f"{symbol}@kline_1m")

            all_channels.extend(channels)

            # 清除相关数据对象
            if symbol in self.ticks:
                del self.ticks[symbol]
            if symbol in self.bars:
                del self.bars[symbol]

        # 发送退订请求
        if all_channels:
            self._send_unsubscribe_request(all_channels)
            self.write_log(f"退订channels: {all_channels}")

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        stream: str = packet.get("stream", None)
        # self.write_log(f"收到数据：{packet}")
        if not stream:
            return
        
        data: dict = packet["data"]
        symbol, channel = stream.split("@")

        if channel in ("ticker", "depth5"):
            tick = self.ticks.get(symbol)
            if not tick:
                self.write_log(f"收到未订阅的tick: {symbol}")
                return
            if channel == "ticker":
                tick.volume = float(data['v'])
                tick.turnover = float(data['q'])
                tick.open_price = float(data['o'])
                tick.high_price = float(data['h'])
                tick.low_price = float(data['l'])
                tick.last_price = float(data['c'])
                tick.datetime = generate_datetime(float(data['E']))
            else:
                bids: list = data["b"]
                for n in range(min(5, len(bids))):
                    price, volume = bids[n]
                    tick.__setattr__("bid_price_" + str(n + 1), float(price))
                    tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

                asks: list = data["a"]
                for n in range(min(5, len(asks))):
                    price, volume = asks[n]
                    tick.__setattr__("ask_price_" + str(n + 1), float(price))
                    tick.__setattr__("ask_volume_" + str(n + 1), float(volume))

            if tick.last_price:
                tick.localtime = datetime.now()
                self.gateway.on_tick(copy(tick))
                
        else:
            kline_data: dict = data["k"]

            # Check if bar is closed
            bar_ready: bool = kline_data.get("x", False)
            if not bar_ready:
                return

            dt: datetime = generate_datetime(float(kline_data['t']))
            
            # 检查是否是已处理过的K线
            bar = self.bars.get(symbol)
            if not bar:
                self.write_log(f"收到未订阅的bar: {symbol}")
                return
            if bar.datetime == dt.replace(second=0, microsecond=0):
                return
            
            self.write_log(f'当前时间：{datetime.now()}，收到K线时间：{dt}，K线已收尾：{bar_ready}，symbol：{symbol}，o:{kline_data["o"]}，h:{kline_data["h"]}，l:{kline_data["l"]}，c:{kline_data["c"]}')
            
            bar.datetime = dt.replace(second=0, microsecond=0)
            bar.volume = float(kline_data["v"])
            bar.turnover = float(kline_data["q"])
            bar.open_price = float(kline_data["o"])
            bar.high_price = float(kline_data["h"])
            bar.low_price = float(kline_data["l"])
            bar.close_price = float(kline_data["c"])

            self.gateway.on_bar(bar)

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected"""
        self.write_log(f"Data Websocket API is disconnected, code: {status_code}, msg: {msg}")
        self.gateway.rest_api.start_user_stream()


def generate_datetime(timestamp: float) -> datetime:
    """Generate datetime object from Binance timestamp"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000, tz=DB_TZ)
    # dt: datetime = dt.replace(tzinfo=UTC_TZ)
    return dt


def format_float(f: float) -> str:
    """
    Convert float number to string with correct precision.

    Fix potential error -1111: Parameter 'quantity' has too much precision
    """
    return format_float_positional(f, trim='-')


class BinanceUsdtGateway(BinanceLinearGateway):
    """Compatibility interface for the old BinanceUsdtGateway"""

    default_name: str = "BINANCE_USDT"
