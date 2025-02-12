from collections import defaultdict
from typing import Optional, Type

from vnpy.event import EventEngine, Event
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (
    EVENT_TICK,
    EVENT_TIMER,
    EVENT_ORDER,
    EVENT_TRADE
)
from vnpy.trader.constant import Direction, Offset, OrderType, Exchange
from vnpy.trader.object import (
    SubscribeRequest,
    OrderRequest,
    LogData,
    ContractData,
    OrderData,
    TickData,
    TradeData,
    CancelRequest
)
from vnpy.trader.utility import round_to

from .template import AlgoTemplate
from .base import (
    EVENT_ALGO_LOG,
    EVENT_ALGO_UPDATE,
    APP_NAME,
    AlgoStatus
)
from .converter import PositionManager
import sys

class AlgoEngine(BaseEngine):
    """算法引擎"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """构造函数"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.algo_templates: dict[str, Type[AlgoTemplate]] = {}

        self.algos: dict[int, AlgoTemplate] = {}  # todo_id: algo
        self.symbol_algo_map: dict[str, set[AlgoTemplate]] = defaultdict(set)
        self.orderid_algo_map: dict[str, AlgoTemplate] = {}

        self.position_manager: PositionManager = PositionManager(event_engine)

        self.load_algo_template()
        self.register_event()

    def init_engine(self) -> None:
        """初始化引擎"""
        self.write_log("算法交易引擎启动")

    def close(self) -> None:
        """关闭引擎"""
        self.stop_all()

    def load_algo_template(self) -> None:
        """载入算法类"""
        from .algos.twap_algo import TwapAlgo
        from .algos.iceberg_algo import IcebergAlgo
        from .algos.sniper_algo import SniperAlgo
        from .algos.stop_algo import StopAlgo
        from .algos.best_limit_algo import BestLimitAlgo
        from .algos.volume_follow_algo import VolumeFollowAlgo

        self.add_algo_template(TwapAlgo)
        self.add_algo_template(IcebergAlgo)
        self.add_algo_template(SniperAlgo)
        self.add_algo_template(StopAlgo)
        self.add_algo_template(BestLimitAlgo)
        self.add_algo_template(VolumeFollowAlgo)

    def add_algo_template(self, template: AlgoTemplate) -> None:
        """添加算法类"""
        self.algo_templates[template.__name__] = template

    def get_algo_template(self) -> dict:
        """获取算法类"""
        return self.algo_templates

    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)

    def process_tick_event(self, event: Event) -> None:
        """处理行情事件"""
        tick: TickData = event.data
        algos: set[AlgoTemplate] = self.symbol_algo_map[tick.vt_symbol]

        for algo in algos:
            algo.update_tick(tick)

    def process_timer_event(self, event: Event) -> None:
        """处理定时事件"""
        # 生成列表避免字典改变
        algos: list[AlgoTemplate] = list(self.algos.values())

        for algo in algos:
            algo.update_timer()

    def process_trade_event(self, event: Event) -> None:
        """处理成交事件"""
        trade: TradeData = event.data

        algo: Optional[AlgoTemplate] = self.orderid_algo_map.get(trade.vt_orderid, None)

        if algo and algo.status in {AlgoStatus.RUNNING, AlgoStatus.PAUSED}:
            algo.update_trade(trade)

    def process_order_event(self, event: Event) -> None:
        """处理委托事件"""
        order: OrderData = event.data

        algo: Optional[AlgoTemplate] = self.orderid_algo_map.get(order.vt_orderid, None)

        if algo and algo.status in {AlgoStatus.RUNNING, AlgoStatus.PAUSED}:
            algo.update_order(order)

    def start_algo(
        self,
        template_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        setting: dict,
        todo_id: int = 0
    ) -> int:
        """启动算法"""
        contract: Optional[ContractData] = self.main_engine.get_contract(vt_symbol)
        if not contract:
            self.write_log(f'算法启动失败，找不到合约：{vt_symbol}')
            return -1

        algo_template: AlgoTemplate = self.algo_templates[template_name]

        # 创建算法实例
        algo_template._count += 1
        algo_name: str = f"{algo_template.__name__}_{algo_template._count}"
        algo: AlgoTemplate = algo_template(
            self,
            algo_name,
            vt_symbol,
            direction,
            offset,
            price,
            volume,
            setting,
            todo_id
        )

        # 订阅行情
        algos: set = self.symbol_algo_map[algo.vt_symbol]
        if not algos:
            self.subscribe(contract.symbol, contract.exchange, contract.gateway_name)
        algos.add(algo)

        # 启动算法
        algo.start()
        self.algos[todo_id] = algo

        return todo_id

    def pause_algo(self, todo_id: int) -> None:
        """暂停算法"""
        algo: Optional[AlgoTemplate] = self.algos.get(todo_id, None)
        if algo:
            algo.pause()

    def resume_algo(self, todo_id: int) -> None:
        """恢复算法"""
        algo: Optional[AlgoTemplate] = self.algos.get(todo_id, None)
        if algo:
            algo.resume()

    def stop_algo(self, todo_id: int) -> None:
        """停止算法"""
        algo: Optional[AlgoTemplate] = self.algos.get(todo_id, None)
        if algo:
            algo.stop()

    def stop_all(self) -> None:
        """停止全部算法"""
        for todo_id in list(self.algos.keys()):
            self.stop_algo(todo_id)

    def subscribe(self, symbol: str, exchange: Exchange, gateway_name: str) -> None:
        """订阅行情"""
        req: SubscribeRequest = SubscribeRequest(
            symbol=symbol,
            exchange=exchange
        )
        self.main_engine.subscribe(req, gateway_name)

    def send_order(
        self,
        algo: AlgoTemplate,
        direction: Direction,
        price: float,
        volume: float,
        order_type: OrderType,
        offset: Offset
    ) -> str:
        """委托下单"""
        contract: Optional[ContractData] = self.main_engine.get_contract(algo.vt_symbol)
        volume: float = round_to(volume, contract.min_volume)
        price: float = round_to(price, contract.pricetick)
        min_notional: float = contract.extra.get("min_notional", 0)

        if not volume:
            self.write_log(f"委托数量为0，不生成委托")
            return ""
        if not price:
            self.write_log(f"委托价格为0，不生成委托")
            return ""
        if price * volume < min_notional:
            self.write_log(f"委托数量{volume}和价格{price}乘积小于最小名义价值{min_notional}，不生成委托")
            return ""

        req: OrderRequest = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            type=order_type,
            volume=volume,
            price=price,
            offset=offset,
            reference=f"{APP_NAME}_{algo.todo_id}"  # 使用todo_id作为引用
        )
        vt_orderid: str = self.main_engine.send_order(req, contract.gateway_name)

        msg: str = f"委托下单: {algo.vt_symbol}, {direction}, {offset}, {volume}@{price}"
        self.write_log(msg, algo)

        self.orderid_algo_map[vt_orderid] = algo
        return vt_orderid

    def cancel_order(self, algo: AlgoTemplate, vt_orderid: str) -> None:
        """委托撤单"""
        order: Optional[OrderData] = self.main_engine.get_order(vt_orderid)

        if not order:
            self.write_log(f"委托撤单失败，找不到委托：{vt_orderid}", algo)
            return

        req: CancelRequest = order.create_cancel_request()
        self.main_engine.cancel_order(req, order.gateway_name)

    def get_tick(self, algo: AlgoTemplate) -> Optional[TickData]:
        """查询行情"""
        tick: Optional[TickData] = self.main_engine.get_tick(algo.vt_symbol)

        if not tick:
            self.write_log(f"查询行情失败，找不到行情：{algo.vt_symbol}", algo)

        return tick

    def get_contract(self, algo: AlgoTemplate) -> Optional[ContractData]:
        """查询合约"""
        contract: Optional[ContractData] = self.main_engine.get_contract(algo.vt_symbol)

        if not contract:
            self.write_log(f"查询合约失败，找不到合约：{algo.vt_symbol}", algo)

        return contract

    def write_log(self, msg: str, algo: AlgoTemplate = None) -> None:
        """输出日志"""
        if algo:
            formatted_msg: str = f"[{algo.todo_id}] {msg}"  # 使用todo_id标识算法单
        else:
            frame = sys._getframe(1)
            func_name = frame.f_code.co_name
            class_name = self.__class__.__name__
            formatted_msg = f"[{class_name}.{func_name}] {msg}"

        log: LogData = LogData(msg=formatted_msg, gateway_name=APP_NAME)
        event: Event = Event(EVENT_ALGO_LOG, data=log)
        self.event_engine.put(event)

    def put_algo_event(self, algo: AlgoTemplate, data: dict) -> None:
        """推送更新"""
        # 移除运行结束的算法实例
        if (
            algo in self.algos.values()
            and algo.status in {AlgoStatus.STOPPED, AlgoStatus.FINISHED}
        ):
            self.algos.pop(algo.todo_id)

            for algos in self.symbol_algo_map.values():
                if algo in algos:
                    algos.remove(algo)

        event: Event = Event(EVENT_ALGO_UPDATE, data)
        self.event_engine.put(event)

    def get_holding(self, vt_symbol: str) -> float:
        """获取指定合约的持仓数量"""
        return self.position_manager.get_position(vt_symbol)

