from datetime import datetime
from vnpy.trader.constant import Direction, Offset, Status
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine

from ..template import AlgoTemplate


class VolumeFollowAlgo(AlgoTemplate):
    """跟量算法"""

    display_name: str = "VolumeFollow 跟量"

    default_setting: dict = {
        "price_add_percent": 2.0,  # 超价比例
        "max_order_wait": 1.0  # 最大订单等待时间（秒）
    }

    variables: list = [
        "order_price",
        "last_tick_volume",
        "total_ordered",  # 已发出的订单总量
        "total_pending"  # 当前挂单总量
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
        self.total_ordered: float = 0  # 已发出的订单总量
        self.total_pending: float = 0  # 当前挂单总量
        self.order_volumes: dict[str, float] = {}  # vt_orderid: 实际发出量
        self.order_traded: dict[str, float] = {}  # vt_orderid: 已成交量

        self.put_event()

    def get_remaining_volume(self) -> float:
        """
        获取剩余可发送的订单量
        需要考虑：已成交量(self.traded) + 未成交挂单量(self.total_pending)
        """
        remaining = self.volume - self.traded - self.total_pending
        return max(0, remaining)

    def update_order_status(self, vt_orderid: str, order_volume: float, traded: float) -> None:
        """
        更新订单状态和相关数量
        """
        # 首次收到订单回报时更新总发单量
        if vt_orderid not in self.order_volumes:
            # 计算订单量差异
            original_volume = self.order_volumes.get(vt_orderid, order_volume)
            volume_diff = original_volume - order_volume
            if volume_diff > 0:
                self.total_ordered -= volume_diff
                self.write_log(f"订单量被修改: {vt_orderid}, "
                               f"原始数量: {original_volume}, "
                               f"实际数量: {order_volume}, "
                               f"更新后总发单量: {self.total_ordered}")

        # 计算之前的挂单量
        old_pending = self.order_volumes.get(vt_orderid, 0) - self.order_traded.get(vt_orderid, 0)

        # 更新订单信息
        self.order_volumes[vt_orderid] = order_volume
        self.order_traded[vt_orderid] = traded

        # 计算新的挂单量
        new_pending = order_volume - traded

        # 更新总挂单量
        self.total_pending = self.total_pending - old_pending + new_pending

        self.write_log(f"订单状态更新: {vt_orderid}, "
                       f"订单量: {order_volume}, "
                       f"已成交: {traded}, "
                       f"挂单中: {new_pending}, "
                       f"总挂单量: {self.total_pending}, "
                       f"总发单量: {self.total_ordered}, "
                       f"总成交量: {self.traded}")

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

        self.write_log(
            f"剩余可发送订单量: {volume_left}, "
            f"当前挂单总量: {self.total_pending}, "
            f"已发送总量: {self.total_ordered}, "
            f"已成交总量: {self.traded}, "
            f"跟量拆单上限: {max_order_volume}"
        )

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
            self.order_traded[vt_orderid] = 0
            self.total_ordered += order_volume
            self.total_pending += order_volume
            self.order_time_map[vt_orderid] = tick.datetime
            self.write_log(f"发出新订单: {vt_orderid}, "
                           f"数量: {order_volume}, "
                           f"累计发送量: {self.total_ordered}, "
                           f"当前挂单量: {self.total_pending}")

        self.put_event()

    def on_order(self, order: OrderData) -> None:
        """委托回调"""
        # 更新订单状态和数量
        self.update_order_status(order.vt_orderid, order.volume, order.traded)

        # 如果订单已完成，清理记录
        if not order.is_active():
            if order.vt_orderid in self.order_volumes:
                self.order_volumes.pop(order.vt_orderid)
            if order.vt_orderid in self.order_traded:
                self.order_traded.pop(order.vt_orderid)
            if order.vt_orderid in self.order_time_map:
                self.order_time_map.pop(order.vt_orderid)
            self.put_event()

        # 更新订单状态
        super().on_order(order)

    def on_trade(self, trade: TradeData) -> None:
        """成交回调"""
        # 更新订单成交状态
        old_traded = self.order_traded.get(trade.vt_orderid, 0)
        new_traded = old_traded + trade.volume
        self.order_traded[trade.vt_orderid] = new_traded

        # 更新挂单量
        order_volume = self.order_volumes.get(trade.vt_orderid, 0)
        self.total_pending = max(0, self.total_pending - trade.volume)

        self.write_log(f"收到成交回报: {trade.vt_orderid}, "
                       f"成交数量: {trade.volume}, "
                       f"订单总量: {order_volume}, "
                       f"订单已成交: {new_traded}, "
                       f"总挂单量: {self.total_pending}, "
                       f"总成交量: {self.traded}")

        if self.traded >= self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}")
            self.finish()
        else:
            self.put_event() 