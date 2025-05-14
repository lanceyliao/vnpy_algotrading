from datetime import datetime
from vnpy.trader.constant import Direction, Offset, Status, OrderType
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine

from ..template import AlgoTemplate


class VolumeFollowSyncAlgo(AlgoTemplate):
    """跟量算法"""

    display_name: str = "VolumeFollowSync 跟量同步"

    default_setting: dict = {
        "price_add_percent": 2.0,  # 超价比例
        "max_order_wait": 1.0      # 最大订单等待时间（秒）
    }

    variables: list = [
        "order_price",
        "last_tick_volume"
    ]

    def __init__(
        self,
        algo_engine: BaseEngine,
        algo_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        setting: dict,
        todo_id: int = 0
    ) -> None:
        """构造函数"""
        super().__init__(algo_engine, algo_name, vt_symbol, direction, offset, price, volume, setting, todo_id)

        # 参数
        self.price_add_percent: float = setting.get("price_add_percent", self.default_setting["price_add_percent"])
        self.max_order_wait: float = setting.get("max_order_wait", self.default_setting["max_order_wait"])

        # 变量
        self.order_price: float = 0
        self.last_tick_volume: float = 0
        self.is_first_tick: bool = True  # 标记是否是第一个tick
        self.order_time_map: dict[str, datetime] = {}  # vt_orderid: order_time，记录订单的发出时间
        
        # 订单量跟踪
        self.order_volumes: dict[str, float] = {}  # vt_orderid: 已发出但未收到回报的订单量

        self.put_event()

    def get_total_pending(self) -> float:
        """
        计算总挂单量：
        1. 已发出但未收到回报的订单：使用发出时的volume
        2. 已收到回报的活跃订单：使用order.volume - order.traded
        """
        total = 0
        
        # 计算已收到回报的活跃订单的待成交量
        for order in self.active_orders.values():
            total += order.volume - order.traded
            
        # 加上已发出但未收到回报的订单量
        for volume in self.order_volumes.values():
            total += volume

        return total

    def on_tick(self, tick: TickData) -> None:
        """Tick行情回调"""
        # 处理第一个tick
        if self.is_first_tick:
            self.last_tick_volume = tick.volume
            self.is_first_tick = False
            return

        # 检查现有订单
        if self.active_orders:
            # 检查是否有需要撤销的订单
            for vt_orderid, order in list(self.active_orders.items()):
                # 获取订单发出时间
                order_time = self.order_time_map.get(vt_orderid)
                if not order_time:
                    continue
                
                # 检查订单是否超时
                time_delta = (tick.datetime - order_time).total_seconds()
                if time_delta > self.max_order_wait:
                    self.write_log(f"订单 {vt_orderid} 超过最大等待时间 {time_delta:.1f}秒，执行撤单")
                    self.cancel_order(vt_orderid)
                    continue
                
                # 检查价格是否已经不合理
                if self.direction == Direction.LONG:
                    if order.price < tick.ask_price_1:
                        self.write_log(f"订单 {vt_orderid} 买入价格{order.price}低于阈值{tick.ask_price_1}，执行撤单")
                        self.cancel_order(vt_orderid)
                else:
                    if order.price > tick.bid_price_1:
                        self.write_log(f"订单 {vt_orderid} 卖出价格{order.price}超过阈值{tick.bid_price_1}，执行撤单")
                        self.cancel_order(vt_orderid)

        # 记录当前tick的成交量
        current_volume: float = tick.volume - self.last_tick_volume
        self.last_tick_volume = tick.volume

        # 如果没有最新成交量，则返回
        if current_volume <= 0:
            return

        # 计算最大委托数量：成交量的1/5
        max_order_volume: float = current_volume / 5

        # 计算剩余可发送的订单量
        volume_left: float = self.get_remaining_volume()
        if volume_left <= 0:
            return

        order_volume: float = min(max_order_volume, volume_left)

        # 检查委托数量是否为0
        if order_volume <= 0:
            return

        # 根据方向计算委托价格
        if self.direction == Direction.LONG:
            # 买入超价：在卖一价上加价
            self.order_price = tick.ask_price_1 * (1 + self.price_add_percent / 100)

            # 检查涨停价
            if tick.limit_up:
                self.order_price = min(self.order_price, tick.limit_up)

            vt_orderid = self.buy(
                self.order_price,
                order_volume,
                offset=self.offset
            )
        else:
            # 卖出超价：在买一价下降价
            self.order_price = tick.bid_price_1 * (1 - self.price_add_percent / 100)

            # 检查跌停价
            if tick.limit_down:
                self.order_price = max(self.order_price, tick.limit_down)

            vt_orderid = self.sell(
                self.order_price,
                order_volume,
                offset=self.offset
            )

        # 记录订单信息
        if vt_orderid:
            self.order_volumes[vt_orderid] = order_volume
            self.order_time_map[vt_orderid] = tick.datetime

        self.put_event()

    def on_order(self, order: OrderData) -> None:
        """委托回调"""
        # 收到订单回报后，移除已发出订单记录
        if order.vt_orderid in self.order_volumes:
            self.order_volumes.pop(order.vt_orderid)
            
        # 如果订单已完成，清理记录
        if not order.is_active():
            if order.vt_orderid in self.order_time_map:
                self.order_time_map.pop(order.vt_orderid)
            self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """成交回调"""
        if self.traded >= self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}")
            self.finish()
        else:
            self.put_event()

    def get_remaining_volume(self) -> float:
        """
        获取剩余可发送的订单量
        需要考虑：已成交量(self.traded) + 未成交挂单量(total_pending)
        """
        remaining = self.volume - self.traded - self.get_total_pending()
        return max(0, remaining) 