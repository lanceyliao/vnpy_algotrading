from typing import Optional, TYPE_CHECKING
import requests
from datetime import datetime, timedelta

from vnpy.event import Event
from vnpy.trader.engine import BaseEngine
from vnpy.trader.object import TickData, OrderData, TradeData, ContractData, BarData
from vnpy.trader.constant import OrderType, Offset, Direction
from vnpy.trader.utility import virtual
from vnpy.trader.setting import SETTINGS
import sys
from .base import AlgoStatusEnum, APP_NAME
from prod.recorder_engine import OrderErrorData, EVENT_ORDER_ERROR_RECORD

if TYPE_CHECKING:
    from .engine import AlgoEngine


class AlgoTemplate:
    """算法模板"""

    _count: int = 0                 # 实例计数

    display_name: str = ""          # 显示名称
    default_setting: dict = {}      # 默认参数
    variables: list = []            # 变量名称

    def __init__(
        self,
        algo_engine: "AlgoEngine",
        algo_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        setting: dict,
        todo_id: int = 0,  # 关联的Todo ID
    ) -> None:
        """构造函数"""
        self.algo_engine: BaseEngine = algo_engine
        self.algo_name: str = algo_name

        self.vt_symbol: str = vt_symbol
        self.direction: Direction = direction
        self.offset: Offset = offset
        self.price: float = price
        self.volume: float = volume
        self.todo_id: int = todo_id

        self.status: AlgoStatusEnum = AlgoStatusEnum.PAUSED
        self.traded: float = 0
        self.traded_price: float = 0
        
        # 黑洞订单管理
        self.black_hole_orders: dict[str, OrderData] = {}  # vt_orderid: order，黑洞订单集合
        self.black_hole_volume: float = 0  # 存储黑洞成交量

        # 添加告警缓存
        self._alert_cache = {}  # 用于存储最近的告警 {title: timestamp}
        self._alert_cache_timeout = 300  # 缓存超时时间(秒)

        self.active_orders: dict[str, OrderData] = {}  # vt_orderid:order


    def update_tick(self, tick: TickData) -> None:
        """行情数据更新"""
        if self.status == AlgoStatusEnum.RUNNING:
            self.on_tick(tick)

    def update_bar(self, bar: BarData) -> None:
        """K线数据更新"""
        if self.status == AlgoStatusEnum.RUNNING:
            self.on_bar(bar)

    def update_order(self, order: OrderData) -> None:
        """委托数据更新"""
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
        elif order.vt_orderid in self.active_orders:
            self.active_orders.pop(order.vt_orderid)

        self.on_order(order)

    def update_trade(self, trade: TradeData) -> None:
        """成交数据更新"""
        # 判断成交方向是否与算法方向一致
        is_reverse = (
            (self.direction == Direction.LONG and trade.direction == Direction.SHORT) or
            (self.direction == Direction.SHORT and trade.direction == Direction.LONG)
        )
        
        # 计算成交量，反向成交需要减去
        volume_change = -trade.volume if is_reverse else trade.volume
        
        # 更新成交均价
        if self.traded == 0:
            # 第一笔成交，直接使用成交价
            self.traded_price = trade.price
        else:
            # 计算成本和总量
            old_cost = self.traded_price * self.traded
            new_cost = trade.price * volume_change
            
            # 如果是反向成交，使用净持仓计算均价
            new_volume = self.traded + volume_change
            if new_volume != 0:  # 防止除以0
                self.traded_price = (old_cost + new_cost) / abs(new_volume)
            
        # 更新成交量
        self.traded += volume_change

        self.on_trade(trade)

    def update_timer(self) -> None:
        """每秒定时更新"""
        if self.status == AlgoStatusEnum.RUNNING:
            self.on_timer()

    @virtual
    def on_tick(self, tick: TickData) -> None:
        """行情回调"""
        pass

    @virtual
    def on_bar(self, bar: BarData) -> None:
        """K线回调"""
        pass

    @virtual
    def on_order(self, order: OrderData) -> None:
        """委托回调"""
        pass

    @virtual
    def on_trade(self, trade: TradeData) -> None:
        """成交回调"""
        pass

    @virtual
    def on_timer(self) -> None:
        """定时回调"""
        pass

    def start(self) -> None:
        """启动"""
        self.status = AlgoStatusEnum.RUNNING
        self.put_event()

        self.write_log()

    def stop(self) -> None:
        """停止"""
        self.status = AlgoStatusEnum.STOPPED
        self.cancel_all()
        self.put_event()

        self.write_log()

    def finish(self) -> None:
        """结束"""
        self.status = AlgoStatusEnum.FINISHED
        self.cancel_all()
        self.put_event()

        self.write_log()

    def pause(self) -> None:
        """暂停"""
        self.status = AlgoStatusEnum.PAUSED
        self.put_event()

        self.write_log()

    def resume(self) -> None:
        """恢复"""
        self.status = AlgoStatusEnum.RUNNING
        self.put_event()

        self.write_log()

    def buy(
        self,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ) -> None:
        """买入"""
        if self.status != AlgoStatusEnum.RUNNING:
            return

        return self.algo_engine.send_order(
            self,
            Direction.LONG,
            price,
            volume,
            order_type,
            offset
        )

    def sell(
        self,
        price: float,
        volume: float,
        order_type: OrderType = OrderType.LIMIT,
        offset: Offset = Offset.NONE
    ) -> None:
        """卖出"""
        if self.status != AlgoStatusEnum.RUNNING:
            return

        return self.algo_engine.send_order(
            self,
            Direction.SHORT,
            price,
            volume,
            order_type,
            offset
        )

    def cancel_order(self, vt_orderid: str) -> None:
        """撤销委托"""
        self.algo_engine.cancel_order(self, vt_orderid)

    def cancel_all(self) -> None:
        """全撤委托"""
        if not self.active_orders:
            return

        for vt_orderid in self.active_orders.keys():
            self.cancel_order(vt_orderid)

    def get_tick(self) -> Optional[TickData]:
        """查询行情"""
        return self.algo_engine.get_tick(self)

    def get_contract(self) -> Optional[ContractData]:
        """查询合约"""
        return self.algo_engine.get_contract(self)

    def get_parameters(self) -> dict:
        """获取算法参数"""
        strategy_parameters: dict = {}
        for name in self.default_setting.keys():
            strategy_parameters[name] = getattr(self, name)
        return strategy_parameters

    def get_variables(self) -> dict:
        """获取算法变量"""
        strategy_variables: dict = {}
        for name in self.variables:
            strategy_variables[name] = getattr(self, name)
        return strategy_variables

    def get_data(self) -> dict:
        """获取算法信息"""
        algo_data: dict = {
            "algo_name": self.algo_name,
            "vt_symbol": self.vt_symbol,
            "direction": self.direction,
            "offset": self.offset,
            "price": self.price,
            "volume": self.volume,
            "status": self.status,
            "traded": self.traded,
            "left": self.volume - self.traded - self.black_hole_volume,
            "traded_price": self.traded_price,
            "black_hole_volume": self.black_hole_volume,
            "parameters": self.get_parameters(),
            "variables": self.get_variables(),
            "todo_id": self.todo_id
        }
        return algo_data

    def write_log(self, msg: str="") -> None:
        """输出日志"""
        if not msg:
            msg = AlgoStatusEnum.to_str(self.status)
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] [todo_id:{self.todo_id}] {msg}"
        self.algo_engine.write_log(formatted_msg, need_format=False)

    def put_event(self) -> None:
        """推送更新"""
        data: dict = self.get_data()
        self.algo_engine.put_algo_event(self, data)

    def get_holding(self) -> float:
        """获取当前持仓"""
        return self.algo_engine.get_holding(self.vt_symbol)
    
    def query_leverage(self) -> None:
        """查询杠杆"""
        contract: ContractData = self.get_contract()
        if contract:
            gateway_name = contract.gateway_name
            gateway = self.algo_engine.main_engine.get_gateway(gateway_name)
            if gateway:
                gateway.rest_api.query_symbol_config()
    
    def get_leverage(self) -> int:
        """获取杠杆倍数"""
        contract: ContractData = self.get_contract()
        if contract and contract.extra:
            gateway_name = contract.gateway_name
            if "leverage" in contract.extra and gateway_name in contract.extra["leverage"]:
                current_leverage = int(contract.extra["leverage"][gateway_name])
                return current_leverage
        return 0
    
    def query_leverage_bracket(self) -> None:
        """查询杠杆梯度"""
        contract: ContractData = self.get_contract()
        if contract:
            gateway_name = contract.gateway_name
            gateway = self.algo_engine.main_engine.get_gateway(gateway_name)
            if gateway:
                gateway.rest_api.query_leverage_bracket()

    def get_leverage_bracket(self) -> list[int]:
        """获取杠杆梯度"""
        contract: ContractData = self.get_contract()
        if contract and contract.extra and "leverage_brackets" in contract.extra:
            leverage_brackets = contract.extra["leverage_brackets"]
            return leverage_brackets
        return []
    
    def query_lower_leverages(self, current_leverage: int, leverage_brackets: list) -> list[int]:
        """
        获取所有可用的更低杠杆倍数列表
        
        参数:
            current_leverage: 当前杠杆倍数
            leverage_brackets: 杠杆梯度信息列表
        """
        # 获取所有可用的杠杆倍数
        leverages = [b.get("initialLeverage", 0) for b in leverage_brackets if b.get("initialLeverage", 0) > 0]
        
        # 筛选并排序更低的杠杆倍数
        target_leverages = sorted([lev for lev in leverages if lev < current_leverage], reverse=True)
        
        self.write_log(f"可选的更低杠杆倍数（从大到小）：{target_leverages}")
        return target_leverages

    def send_alert(self, title: str = "", msg: str = "", need_phone: bool = False, need_db: bool = False) -> None:
        """
        发送告警并记录到数据库，相同标题的告警会在缓存期内被去重

        Args:
            title: 告警标题
            msg: 告警信息
            need_phone: 是否需要电话告警
        """
        current_time = datetime.now()

        # 检查是否存在相同标题的近期告警
        if title in self._alert_cache:
            last_alert_time = self._alert_cache[title]
            if (current_time - last_alert_time).total_seconds() < self._alert_cache_timeout:
                return

        # 更新告警缓存
        self._alert_cache[title] = current_time

        # 如果相同标题的告警在缓存期内，不发送告警记录数据库
        self.algo_engine.send_alert(self, title=title, msg=msg, need_phone=need_phone)

        if not need_db:
            # 创建告警记录
            alert_error = OrderErrorData(
                symbol=self.vt_symbol,
                exchange=self.get_contract().exchange if self.get_contract() else None,
                error_code=0,  # 使用0表示告警消息
                error_msg=f"{title}: {msg}",
                orderid="",  # 告警不关联订单
                todo_id=str(self.todo_id),
                create_date=current_time,
                gateway_name=APP_NAME
            )

            # 推送告警事件
            self.algo_engine.event_engine.put(Event(EVENT_ORDER_ERROR_RECORD, alert_error))