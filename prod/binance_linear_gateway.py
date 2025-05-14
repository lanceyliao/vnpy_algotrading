__version__ = "2025-4-18 22:45:30"
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
from collections import defaultdict

# from aiohttp import ClientSSLError
# https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
from numpy import format_float_positional
from vnpy_evo.trader.database import DB_TZ
# from vnpy.usertools.task_db_manager import task_db_manager

from requests.exceptions import SSLError
from vnpy_evo.trader.event import EVENT_CONTRACT
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
from vnpy_evo.trader.utility import round_to, ZoneInfo, floor_to

# from vnpy_rest import Request, RestClient, Response
from .rest_client import Request, RestClient, Response
# from vnpy_websocket import WebsocketClient
# from vnpy_evo.rest import Request, RestClient, Response
# from vnpy_evo.websocket import WebsocketClient
# from .qfw.websocket_client import WebsocketClient
from .websocket_client import WebsocketClient
from .event import EVENT_BAR, EVENT_MINIBAR

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
# Global dict for gateway extra info: {symbol: {"leverage": {gateway_name: val}, "max_notional_value": {gateway_name: val}, "leverage_brackets": []}}
symbol_gateway_extra = defaultdict(lambda: {"leverage": {}, "max_notional_value": {}, "leverage_brackets": []})

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

        # 添加用于跟踪所有交易过的交易对集合
        self.active_symbols: set = set()

        self.trade_ws_api: BinanceLinearTradeWebsocketApi = BinanceLinearTradeWebsocketApi(self)
        self.market_ws_api: BinanceLinearDataWebsocketApi = BinanceLinearDataWebsocketApi(self)
        self.rest_api: BinanceLinearRestApi = BinanceLinearRestApi(self)

        self.orders: dict[str, OrderData] = {}
        self.positions: dict[str, PositionData] = {}
        self.incomes: dict[str, dict[int, dict]] = {}
        self.get_server_time_interval: int = 0

        self.trade_history_ready: bool = False
        self.trade_history_url: str = ""

        # 添加last_bar字典用于记录每个symbol的上一根bar
        self.last_bars: dict[str, BarData] = {}

        # 添加用于跟踪minibar的字典和相关属性
        self.last_minibars: dict[str, BarData] = {}  # 按vt_symbol存储上一个minibar
        
        # 添加用于跟踪ticker的volume和turnover变化的属性
        self.last_tickers: dict[str, TickData] = {}  # 按symbol存储上一个tick
        self.current_minutes: dict[str, datetime] = {}  # 当前处理的分钟
        self.minute_first_volumes: dict[str, float] = {}  # 每分钟第一个ticker的volume
        self.minute_first_turnovers: dict[str, float] = {}  # 每分钟第一个ticker的turnover
        self.minute_first_ticker_times: dict[str, datetime] = {}  # 每分钟第一个ticker的时间
        self.minute_last_volumes: dict[str, float] = {}  # 每分钟最后一个ticker的volume
        self.minute_last_turnovers: dict[str, float] = {}  # 每分钟最后一个ticker的turnover
        self.last_bar_volumes: dict[str, float] = {}  # 上一分钟K线的成交量
        self.last_bar_turnovers: dict[str, float] = {}  # 上一分钟K线的成交额

        self.mini: bool = False

        self.write_log(f"Gateway Version: {__version__}")

    def is_connected(self) -> bool:
        """检查网关连接状态"""
        # 检查 Trade WebSocket API 连接状态
        trade_ws_connected = (
            self.trade_ws_api._active  # WebSocket客户端是否激活
            and self.trade_ws_api._ws is not None  # 是否有WebSocket连接对象
            and not self.trade_ws_api._connecting  # 不在连接过程中
        )

        # 检查 Market WebSocket API 连接状态
        market_ws_connected = (
            self.market_ws_api._active  # WebSocket客户端是否激活
            and self.market_ws_api._ws is not None  # 是否有WebSocket连接对象
            and not self.market_ws_api._connecting  # 不在连接过程中
        )

        is_connected = trade_ws_connected and market_ws_connected

        if not self.trade_ws_api._active:
            self.write_log(f'trade_ws_api._active: {self.trade_ws_api._active}')
        if self.trade_ws_api._ws is None:
            self.write_log(f'trade_ws_api._ws: {self.trade_ws_api._ws}')
        if self.trade_ws_api._connecting:
            self.write_log(f'trade_ws_api._connecting: {self.trade_ws_api._connecting}')
        if not self.market_ws_api._active:
            self.write_log(f'market_ws_api._active: {self.market_ws_api._active}')
        if self.market_ws_api._ws is None:
            self.write_log(f'market_ws_api._ws: {self.market_ws_api._ws}')
        if self.market_ws_api._connecting:
            self.write_log(f'market_ws_api._connecting: {self.market_ws_api._connecting}')

        # 所有API都正常连接才返回True
        return is_connected

    def connect(self, setting: dict) -> None:
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.event_engine.unregister(EVENT_CONTRACT, self.save_contract)
        self.event_engine.register(EVENT_CONTRACT, self.save_contract)

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
        # 记录交易过的symbol
        self.active_symbols.add(req.symbol)
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.rest_api.cancel_order(req)
        self.write_log(f"取消订单 {req.orderid}")

    def cancel_all(self) -> None:
        """撤销所有活跃合约的挂单"""
        for symbol in self.active_symbols:
            self.rest_api.cancel_symbol_orders(symbol)
            self.write_log(f"撤销{symbol}所有挂单")

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

    def save_contract(self, event: Event) -> None:
        """
        Save contract event.
        """
        contract: ContractData = event.data
        symbol_contract_map[contract.symbol] = contract

    def on_contract(self, contract: ContractData) -> None:
        """
        on contract update
        """
        # 保留原有 extra 字段
        if contract.extra is None:
            contract.extra = {}

        # 添加min_notional到extra字典中
        contract.extra.update(symbol_gateway_extra[contract.symbol])

        # if contract.symbol in { 'BTCUSDT', 'ETHUSDT' }:
        #     self.write_log(f'id {contract.symbol}: {id(contract.extra["leverage"])}')
        symbol_contract_map[contract.symbol] = contract
        super().on_contract(contract)

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
        self.write_log(f"symbol: {position.symbol}, volume: {position.volume}")
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
        # 先检查时间差
        last_bar = self.last_bars.get(bar.vt_symbol)
        if last_bar:
            minutes_diff = int((bar.datetime - last_bar.datetime).total_seconds() / 60)
            if minutes_diff > 1:
                # 设置固定属性
                last_bar.volume = 0
                last_bar.turnover = 0
                last_bar.open_price = last_bar.close_price
                last_bar.high_price = last_bar.close_price
                last_bar.low_price = last_bar.close_price

                # 生成并推送缺失的bar
                for _ in range(minutes_diff-1):
                    last_bar.datetime = last_bar.datetime + timedelta(minutes=1)

                    self.write_log(f"补充缺失的bar: {last_bar.vt_symbol}, 时间: {last_bar.datetime}, 价格: {last_bar.close_price}")

                    # 发送事件时复制bar对象
                    bar_copy = copy(last_bar)
                    self.on_event(EVENT_BAR, bar_copy)
                    self.on_event(EVENT_BAR + last_bar.vt_symbol, bar_copy)

        # 更新last_bar
        self.last_bars[bar.vt_symbol] = bar

        # 发送当前bar的事件
        bar_copy = copy(bar)
        self.on_event(EVENT_BAR, bar_copy)
        self.on_event(EVENT_BAR + bar.vt_symbol, bar_copy)

    def on_minibar(self, minibar: BarData) -> None:
        """
        处理minibar事件推送，实现以下功能：
        1. 调整minibar.datetime的秒数为floor_to(秒数,2)
        2. 填充缺失的minibar
        """
        # 获取vt_symbol
        vt_symbol = minibar.vt_symbol
        
        # 1. 调整minibar.datetime的秒数为floor_to(秒数,2)
        original_dt = minibar.datetime
        seconds = original_dt.second
        adjusted_seconds = floor_to(seconds, 2)
        minibar.datetime = original_dt.replace(second=int(adjusted_seconds), microsecond=0)
        
        # 2. 处理minibar填充
        last_minibar = self.last_minibars.get(vt_symbol)
        
        if last_minibar:
            # 计算时间差（秒）
            seconds_diff = (minibar.datetime - last_minibar.datetime).total_seconds()
            
            # 如果seconds_diff > 2，说明有缺失的minibar，需要填充
            if seconds_diff > 2:
                # 设置固定属性
                last_minibar.volume = 0
                last_minibar.turnover = 0
                last_minibar.open_price = last_minibar.close_price
                last_minibar.high_price = last_minibar.close_price
                last_minibar.low_price = last_minibar.close_price
                
                # 生成并推送缺失的minibar
                for i in range(int(seconds_diff / 2) - 1):
                    last_minibar.datetime = last_minibar.datetime + timedelta(seconds=2)
                    
                    self.write_log(f"补充缺失的minibar: {last_minibar.vt_symbol}, 时间: {last_minibar.datetime}, 价格: {last_minibar.close_price}")
                    
                    # 发送事件时复制minibar对象
                    minibar_copy = copy(last_minibar)
                    self.on_event(EVENT_MINIBAR, minibar_copy)
                    self.on_event(EVENT_MINIBAR + vt_symbol, minibar_copy)
            
            # 如果seconds_diff == 0，说明2s内收到了两个ticker，第二个应该是下一个2s的
            elif seconds_diff == 0:
                # 将datetime调整为last_minibar的datetime + 2s
                minibar.datetime = last_minibar.datetime + timedelta(seconds=2)
                self.write_log(f"调整minibar时间: {vt_symbol}, 从 {original_dt} 到 {minibar.datetime}")
        
        # 更新last_minibar
        self.last_minibars[vt_symbol] = minibar
        
        # 推送处理后的minibar
        minibar_copy = copy(minibar)
        self.on_event(EVENT_MINIBAR, minibar_copy)
        self.on_event(EVENT_MINIBAR + vt_symbol, minibar_copy)

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

        if not self.order_prefix:
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
        self.query_symbol_config()
        self.query_leverage_bracket()
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

    def query_orders(self) -> None:
        """Query open orders"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_orders,
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
            self.query_orders() #remove this 0917
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

    def on_query_orders(self, data: list, request: Request) -> None:
        """Callback of open orders query"""
        for d in data:
            key: tuple[str, str] = (d["type"], d["timeInForce"])
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
            if not order_type:
                continue

            # Get last order first for offset information
            order_id: str = d["clientOrderId"]
            last_order = self.gateway.get_order(order_id)
            offset = last_order.offset if last_order else None

            # Get price and traded price
            price = float(d["price"])
            traded = float(d.get('executedQty', '0'))
            traded_price = float(d.get('avgPrice', '0'))
            if price <= 0 and traded_price > 0:
                price = traded_price

            # Create order data object
            order: OrderData = OrderData(
                orderid=order_id,
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                price=price,
                volume=float(d["origQty"]),
                type=order_type,
                direction=DIRECTION_BINANCES2VT[d["side"]],
                traded=traded,
                status=STATUS_BINANCES2VT.get(d["status"], None),
                datetime=generate_datetime(d["time"]),
                gateway_name=self.gateway_name,
                offset=offset
            )

            # Always push first order update
            if not last_order:
                self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
                self.gateway.on_order(order)
                continue

            # Calculate traded change
            traded_change = order.traded - last_order.traded
            status_change = order.status != last_order.status

            # Round traded change if needed
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                original_traded_change = traded_change
                traded_change = round_to(traded_change, contract.min_volume)
                if traded_change != original_traded_change:
                    self.write_log(f"[warning] {order.symbol} traded_change rounded: {original_traded_change} -> {traded_change}")

            if traded_change < 0:
                continue
            if traded_change == 0 and not status_change:
                continue

            self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
            self.gateway.on_order(order)

            # Push trade update on new trade
            if traded_change > 0:
                trade: TradeData = TradeData(
                    symbol=order.symbol,
                    exchange=order.exchange,
                    orderid=order.orderid,
                    tradeid=d["orderId"],  # Use orderId as tradeid for REST API trades
                    direction=order.direction,
                    price=traded_price,
                    volume=traded_change,
                    datetime=generate_datetime(d.get("updateTime", d["time"])),
                    gateway_name=self.gateway_name,
                    offset=offset
                )
                self.write_log(f"收到成交回报，订单号：{trade.orderid}，成交号：{trade.tradeid}，价格：{trade.price}，数量：{trade.volume}")
                self.gateway.on_trade(trade)

        self.write_log("Open orders data is received")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1
            min_notional: int = 0

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

            # 确保contract的extra属性是字典类型
            if contract.extra is None:
                contract.extra = {}

            # 添加min_notional到extra字典中
            contract.extra["min_notional"] = min_notional

            self.gateway.on_contract(contract)

            # symbol_contract_map[contract.symbol] = contract

        self.write_log("Available contracts data is received")

    def on_send_order(self, data: dict, request: Request) -> None:
        """Successful callback of send_order"""
        self.write_log(f"订单发送成功，订单号：{request.extra.orderid}，交易所返回：{data}")
        if request.extra:
            order: OrderData = copy(request.extra)

            # Get price and traded price
            price = float(data["price"])
            traded = float(data.get('executedQty', '0'))
            traded_price = float(data.get('avgPrice', '0'))
            if price <= 0 and traded_price > 0:
                price = traded_price

            # Update order info
            order.traded = traded
            order.price = price
            order.volume = float(data["origQty"])  # 使用交易所返回的委托数量
            order.status = STATUS_BINANCES2VT.get(data.get('status'), Status.NOTTRADED)

            # Get last order for comparison
            last_order = self.gateway.get_order(order.orderid)
            last_order.volume = order.volume

            # Always push first order update
            if not last_order:
                self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
                self.gateway.on_order(order)
                return

            # Calculate traded change
            traded_change = order.traded - last_order.traded
            status_change = order.status != last_order.status

            # Round traded change if needed
            contract: ContractData = symbol_contract_map.get(order.symbol, None)
            if contract:
                original_traded_change = traded_change
                traded_change = round_to(traded_change, contract.min_volume)
                if traded_change != original_traded_change:
                    self.write_log(f"[warning] {order.symbol} traded_change rounded: {original_traded_change} -> {traded_change}")

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
                    tradeid=data["orderId"],  # Use orderId as tradeid for REST API trades
                    direction=order.direction,
                    price=traded_price,
                    volume=traded_change,
                    datetime=generate_datetime(data.get("updateTime", int(time.time()*1000))),
                    gateway_name=self.gateway_name,
                    offset=order.offset
                )
                self.write_log(f"收到成交回报，订单号：{trade.orderid}，成交号：{trade.tradeid}，价格：{trade.price}，数量：{trade.volume}")
                self.gateway.on_trade(trade)

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

        if not issubclass(exception_type, (ConnectionError, SSLError)):
        # https://github.com/veighna-global/vnpy_binance/commit/8fe3acc40410a8d9bf75c860b193f565cc596aa3
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Successful callback of cancel_order"""
        key: tuple[str, str] = (data["type"], data["timeInForce"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        # Get last order for offset information
        order_id: str = data["clientOrderId"]
        last_order = self.gateway.get_order(order_id)
        offset = last_order.offset if last_order else None

        # Get price and traded price
        price = float(data["price"])
        traded = float(data.get('executedQty', '0'))
        traded_price = float(data.get('avgPrice', '0'))
        if price <= 0 and traded_price > 0:
            price = traded_price

        # Create order data object
        order: OrderData = OrderData(
            orderid=order_id,
            symbol=data["symbol"],
            exchange=Exchange.BINANCE,
            price=price,
            volume=float(data["origQty"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[data["side"]],
            traded=traded,
            status=STATUS_BINANCES2VT.get(data["status"], Status.CANCELLED),
            datetime=generate_datetime(data["time"]),
            gateway_name=self.gateway_name,
            offset=offset
        )

        # Always push first order update
        if not last_order:
            self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
            self.gateway.on_order(order)
            return

        # Calculate traded change
        traded_change = order.traded - last_order.traded
        status_change = order.status != last_order.status

        # Round traded change if needed
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            original_traded_change = traded_change
            traded_change = round_to(traded_change, contract.min_volume)
            if traded_change != original_traded_change:
                self.write_log(f"[warning] {order.symbol} traded_change rounded: {original_traded_change} -> {traded_change}")

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
                tradeid=data["orderId"],  # Use orderId as tradeid for REST API trades
                direction=order.direction,
                price=traded_price,
                volume=traded_change,
                datetime=generate_datetime(data.get("updateTime", data["time"])),
                gateway_name=self.gateway_name,
                offset=offset
            )
            self.write_log(f"收到成交回报，订单号：{trade.orderid}，成交号：{trade.tradeid}，价格：{trade.price}，数量：{trade.volume}")
            self.gateway.on_trade(trade)

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of cancel_order"""
        if request.extra:
            order: OrderData = request.extra
            # order.status = Status.REJECTED
            # self.gateway.on_order(order)
            self.query_order(order) # 可能导致无法识别账户断连，其实应该告警

        msg: str = f"Cancel order failed, status code: {status_code} , message: {request.response.text}, order: {request.extra} "
        self.write_log(msg)
        # task_db_manager.report_alert(content=msg, assigned_to="vigar")

    def query_order(self, order: OrderData) -> None:
        """查询特定订单
        
        参数:
            order: OrderData对象
        """
        data: dict = {
            "security": Security.SIGNED
        }

        params: dict = {
            "symbol": order.symbol,
            "origClientOrderId": order.orderid
        }

        path: str = "/fapi/v1/order"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            params=params,
            data=data,
            extra=order
        )

    def on_query_order(self, data: dict, request: Request) -> None:
        """查询订单回报"""
        key: tuple[str, str] = (data["type"], data["timeInForce"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
        if not order_type:
            return

        # Get price and traded price
        price = float(data["price"])
        traded = float(data.get('executedQty', '0'))
        traded_price = float(data.get('avgPrice', '0'))
        if price <= 0 and traded_price > 0:
            price = traded_price

        # Get last order for offset information
        order_id: str = data["clientOrderId"]
        last_order = self.gateway.get_order(order_id)
        offset = last_order.offset if last_order else None

        # Create order data object
        order: OrderData = OrderData(
            orderid=order_id,
            symbol=data["symbol"],
            exchange=Exchange.BINANCE,
            price=price,
            volume=float(data["origQty"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[data["side"]],
            traded=traded,
            status=STATUS_BINANCES2VT.get(data["status"], None),
            datetime=generate_datetime(data["time"]),
            gateway_name=self.gateway_name,
            offset=offset
        )

        # Always push first order update
        if not last_order:
            self.write_log(f"收到订单回报，订单号：{order.orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
            self.gateway.on_order(order)
            return

        # Calculate traded change
        traded_change = order.traded - last_order.traded
        status_change = order.status != last_order.status

        # Round traded change if needed
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            original_traded_change = traded_change
            traded_change = round_to(traded_change, contract.min_volume)
            if traded_change != original_traded_change:
                self.write_log(f"[warning] {order.symbol} traded_change rounded: {original_traded_change} -> {traded_change}")

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
                tradeid=data["orderId"],  # Use orderId as tradeid for REST API trades
                direction=order.direction,
                price=traded_price,
                volume=traded_change,
                datetime=generate_datetime(data.get("updateTime", data["time"])),
                gateway_name=self.gateway_name,
                offset=offset
            )
            self.write_log(f"收到成交回报，订单号：{trade.orderid}，成交号：{trade.tradeid}，价格：{trade.price}，数量：{trade.volume}")
            self.gateway.on_trade(trade)

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
        
    def query_symbol_config(self, symbol: str = "") -> None:
        """获取交易对配置"""
        data: dict = {"security": Security.SIGNED}
        
        params: dict = {}
        if symbol:
            params["symbol"] = symbol
        
        path: str = "/fapi/v1/symbolConfig"
        
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_symbol_config,
            params=params,
            data=data
        )
        
    def query_leverage_bracket(self, symbol: str = "") -> None:
        """查询杠杆梯度"""
        data: dict = {"security": Security.SIGNED}
        
        params: dict = {}
        if symbol:
            params["symbol"] = symbol
            
        path: str = "/fapi/v1/leverageBracket"
        
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_leverage_bracket,
            params=params,
            data=data
        )
        
    def on_set_leverage(self, data: dict, request: Request) -> None:
        """设置杠杆倍数回调"""
        if data:
            symbol = data['symbol']
            leverage = data['leverage']
            max_notional_value = data['maxNotionalValue']
            gateway_name = self.gateway_name

            # 赋值到新结构
            symbol_gateway_extra[symbol]["leverage"][gateway_name] = leverage
            symbol_gateway_extra[symbol]["max_notional_value"][gateway_name] = max_notional_value

            if contract := symbol_contract_map.get(symbol, None):
                self.gateway.on_contract(contract)

            msg = f"设置杠杆倍数成功 - {symbol}: {leverage}倍，最大名义价值：{max_notional_value}, 用户: {gateway_name}"
            self.write_log(msg)
            
    def on_query_symbol_config(self, data: list, request: Request) -> None:
        """获取交易对配置回调"""
        if data:
            gateway_name = self.gateway_name
            for config in data:
                symbol = config.get('symbol')
                # margin_type = config.get('marginType', '')
                # is_auto_add_margin = config.get('isAutoAddMargin', 'false')
                leverage = config.get('leverage', 0)
                max_notional_value = config.get('maxNotionalValue', '0')
                
                symbol_gateway_extra[symbol]["leverage"][gateway_name] = leverage
                symbol_gateway_extra[symbol]["max_notional_value"][gateway_name] = max_notional_value
                
                # 更新事件引擎中保存的合约信息
                if contract := symbol_contract_map.get(symbol, None):
                    self.gateway.on_contract(contract)
                    
                # self.write_log(f"获取交易对配置 - {symbol}: 杠杆倍数={leverage}倍，保证金类型={margin_type}，自动追加保证金={is_auto_add_margin}，最大名义价值={max_notional_value}")
                # self.write_log(f"获取交易对配置 - {symbol}: 杠杆倍数={leverage}倍，最大名义价值={max_notional_value}")
                
    def on_query_leverage_bracket(self, data: list, request: Request) -> None:
        """查询杠杆梯度回调"""
        if data:
            gateway_name = self.gateway_name
            for item in data:
                symbol = item.get("symbol", "")
                # 用户bracket相对默认bracket的倍数，仅在和交易对默认不一样时显示
                notionalCoef = item.get("notionalCoef", 1) 
                brackets = item.get("brackets", [])
                
                if symbol and brackets:
                    # 将bracket信息存储到symbol_gateway_extra中
                    symbol_gateway_extra[symbol]["leverage_brackets"] = brackets
                    symbol_gateway_extra[symbol]["notionalCoef"][gateway_name] = notionalCoef
                    
                    # 更新事件引擎中保存的合约信息
                    if contract := symbol_contract_map.get(symbol, None):
                        self.gateway.on_contract(contract)
                        
                    self.write_log(f"获取{symbol}杠杆梯度成功，共{len(brackets)}级")

    def cancel_symbol_orders(self, symbol: str) -> None:
        """撤销指定交易对的所有挂单"""
        data: dict = {"security": Security.SIGNED}

        params: dict = {
            "symbol": symbol
        }

        path: str = "/fapi/v1/allOpenOrders"

        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_symbol_orders,
            params=params,
            data=data,
            on_failed=self.on_cancel_symbol_failed
        )

    def on_cancel_symbol_orders(self, data: dict, request: Request) -> None:
        """撤销所有订单回调"""
        self.write_log(f"撤销所有订单成功: {data}")

    def on_cancel_symbol_failed(self, status_code: str, request: Request) -> None:
        """撤销所有订单失败回调"""
        msg: str = f"撤销所有订单失败，错误码：{status_code}，信息：{request.response.text}"
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
        self.minibars: dict[str, BarData] = {}
        self.reqid: int = 0
        self.kline_stream: bool = False

        # 添加批量订阅控制参数
        self.max_channels_per_request = 200  # 每个请求最大channel数量
        self.subscribe_interval = 0.3  # 订阅请求间隔(秒)

        self.log_mode = False

    def enable_log_mode(self):
        self.log_mode = True
        # 添加时间戳跟踪变量
        self.last_ticker_time: dict[str, datetime] = {}  # 记录每个symbol最后一次ticker的时间
        self.last_depth_time: dict[str, datetime] = {}  # 记录每个symbol最后一次depth的时间
        self.ticker_count: dict[str, int] = {}  # 记录每个symbol的ticker计数
        self.depth_count: dict[str, int] = {}  # 记录每个symbol的depth计数
        self.start_time: dict[str, datetime] = {}  # 记录每个symbol的开始时间

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

        # Initialize minibars object
        minibars: BarData = BarData(
            symbol=req.symbol,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(UTC_TZ),
            interval=Interval.SECOND_2,
            gateway_name=self.gateway_name
        )
        self.minibars[req.symbol.lower()] = minibars

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
        if symbol in self.minibars:
            del self.minibars[symbol]

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

            # Initialize minibars object
            minibars: BarData = BarData(
                symbol=req.symbol,
                exchange=Exchange.BINANCE,
                datetime=datetime.now(UTC_TZ),
                interval=Interval.SECOND_2,
                gateway_name=self.gateway_name
            )
            self.minibars[req.symbol.lower()] = minibars

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
            if symbol in self.minibars:
                del self.minibars[symbol]

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
            if self.log_mode and symbol.upper() != 'ETHUSDT':
                return
            tick = self.ticks.get(symbol)
            minibar = self.minibars.get(symbol)
            if not tick:
                self.write_log(f"收到未订阅的tick: {symbol}")
                return
            if not minibar and self.gateway.mini and channel == "ticker":
                self.write_log(f"收到未订阅的minibar: {symbol}")
                return

            if self.log_mode:
                current_time = datetime.now()

                # 初始化计数器和开始时间
                if symbol not in self.start_time:
                    self.start_time[symbol] = current_time
                    self.ticker_count[symbol] = 0
                    self.depth_count[symbol] = 0
                
            if channel == "ticker":
                # 获取上一个tick数据
                last_ticker = self.gateway.last_tickers.get(symbol)
                
                # 更新当前tick数据
                tick.volume = float(data['v'])
                tick.turnover = float(data['q'])
                tick.open_price = float(data['o'])
                tick.high_price = float(data['h'])
                tick.low_price = float(data['l'])
                tick.last_price = float(data['c'])
                tick.datetime = generate_datetime(float(data['E']))

                if self.log_mode:
                    # 更新ticker计数和时间
                    self.ticker_count[symbol] += 1
                    last_time = self.last_ticker_time.get(symbol)
                    self.last_ticker_time[symbol] = current_time

                    # 计算并记录ticker频率
                    elapsed_seconds = (current_time - self.start_time[symbol]).total_seconds()
                    ticker_frequency = self.ticker_count[symbol] / elapsed_seconds if elapsed_seconds > 0 else 0

                    # 记录ticker信息
                    s = f"Ticker推送 - {symbol} - 交易所时间: {tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}，统计开始时间: {data['O']}，结束时间：{data['C']}, 成交量: {tick.volume}, 成交额： {tick.turnover}, 频率: {ticker_frequency:.2f}次/秒"

                    if last_time:
                        interval = (current_time - last_time).total_seconds()
                        self.write_log(f"{s}, 间隔{interval:.3f}秒")
                    else:
                        self.write_log(s)
                
                # 处理minibar数据
                if self.gateway.mini:
                    # 设置minibar的价格数据
                    minibar.open_price = tick.last_price
                    minibar.high_price = tick.last_price
                    minibar.low_price = tick.last_price
                    minibar.close_price = tick.last_price
                    minibar.datetime = tick.datetime
                    
                    # 计算实际volume和turnover变化
                    real_volume_change = 0
                    real_turnover_change = 0
                    
                    if last_ticker:
                        # 获取当前分钟
                        new_minute = tick.datetime.replace(second=0, microsecond=0)
                        current_minute = self.gateway.current_minutes.get(symbol)
                        
                        # 如果是新的一分钟
                        if current_minute is None or new_minute > current_minute:
                            # 检查是否只差一分钟
                            minute_diff = 1 if current_minute is None else int((new_minute - current_minute).total_seconds() / 60)
                            
                            # 新的一分钟，且只差1分钟
                            if (minute_diff == 1 
                                and self.gateway.last_bar_volumes.get(symbol, 0) > 0
                                and self.gateway.last_bar_turnovers.get(symbol, 0) > 0
                                and self.gateway.minute_first_ticker_times.get(symbol) 
                                and self.gateway.minute_first_ticker_times[symbol].minute == current_minute.minute  # 确保是上一分钟的数据
                                and self.gateway.minute_first_ticker_times[symbol].second <= 5  # 确保是在前5秒内收到的
                            ):
                                # 只有当上一分钟的第一个ticker是在前5秒内收到时，才使用bar数据修正volume_change
                                minute_volume = self.gateway.minute_last_volumes.get(symbol, 0) - self.gateway.minute_first_volumes.get(symbol, 0)
                                minute_turnover = self.gateway.minute_last_turnovers.get(symbol, 0) - self.gateway.minute_first_turnovers.get(symbol, 0)
                                
                                real_volume_change = self.gateway.last_bar_volumes.get(symbol, 0) - minute_volume
                                real_turnover_change = self.gateway.last_bar_turnovers.get(symbol, 0) - minute_turnover
                            else:
                                real_volume_change = 0
                                real_turnover_change = 0
                            
                            # 更新分钟相关数据
                            self.gateway.minute_first_volumes[symbol] = tick.volume
                            self.gateway.minute_first_turnovers[symbol] = tick.turnover
                            self.gateway.minute_first_ticker_times[symbol] = tick.datetime
                            self.gateway.current_minutes[symbol] = new_minute
                        else:
                            # 同一分钟内
                            real_volume_change = tick.volume - last_ticker.volume
                            real_turnover_change = tick.turnover - last_ticker.turnover
                        
                        # 更新最后一个tick的volume和turnover
                        self.gateway.minute_last_volumes[symbol] = tick.volume
                        self.gateway.minute_last_turnovers[symbol] = tick.turnover
                    else:
                        # 第一个tick
                        self.gateway.current_minutes[symbol] = tick.datetime.replace(second=0, microsecond=0)
                        self.gateway.minute_first_volumes[symbol] = tick.volume
                        self.gateway.minute_first_turnovers[symbol] = tick.turnover
                        self.gateway.minute_first_ticker_times[symbol] = tick.datetime
                        self.gateway.minute_last_volumes[symbol] = tick.volume
                        self.gateway.minute_last_turnovers[symbol] = tick.turnover
                    
                    # 设置minibar的volume和turnover为实际变化量
                    if real_volume_change > 0:
                        minibar.volume = real_volume_change
                        minibar.turnover = real_turnover_change
                    else:
                        minibar.volume = 0
                        minibar.turnover = 0
                
                # 更新last_tick
                self.gateway.last_tickers[symbol] = copy(tick)
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

                if self.log_mode:
                    # 更新depth计数和时间
                    self.depth_count[symbol] += 1
                    last_time = self.last_depth_time.get(symbol)
                    self.last_depth_time[symbol] = current_time

                    # 计算并记录depth频率
                    elapsed_seconds = (current_time - self.start_time[symbol]).total_seconds()
                    depth_frequency = self.depth_count[symbol] / elapsed_seconds if elapsed_seconds > 0 else 0

                    # 记录depth信息
                    s = f"Depth推送 - {symbol} - 交易所时间: {tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}, 频率: {depth_frequency:.2f}次/秒"

                    if last_time:
                        interval = (current_time - last_time).total_seconds()
                        self.write_log(f"{s}, 间隔{interval:.3f}秒")
                    else:
                        self.write_log(s)

            if tick.last_price:
                tick.localtime = datetime.now()
                self.gateway.on_tick(copy(tick))
                
                # 当mini为True时，同时触发minibar事件
                if self.gateway.mini and channel == "ticker":
                    self.gateway.on_minibar(copy(minibar))

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

            # 记录当前K线的成交量和成交额，用于minibar的volume和turnover修正
            self.gateway.last_bar_volumes[symbol] = bar.volume
            self.gateway.last_bar_turnovers[symbol] = bar.turnover
            
            self.gateway.on_bar(copy(bar))

    def on_disconnected(self, status_code: int, msg: str) -> None:
        """Callback when server is disconnected"""
        self.write_log(f"Data Websocket API is disconnected, code: {status_code}, msg: {msg}")
        self.gateway.rest_api.start_user_stream()


def generate_datetime(timestamp: float) -> datetime:
    """Generate datetime object from Binance timestamp"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000, tz=DB_TZ)
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
