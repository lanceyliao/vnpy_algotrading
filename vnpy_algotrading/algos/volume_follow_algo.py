from vnpy.trader.constant import Direction, Offset
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine

from ..template import AlgoTemplate


class VolumeFollowAlgo(AlgoTemplate):
    """跟量算法"""

    display_name: str = "VolumeFollow 跟量"

    default_setting: dict = {
        "price_add_percent": 2.0  # 超价比例
    }

    variables: list = [
        "vt_orderid",
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

        # 变量
        self.vt_orderid: str = ""
        self.order_price: float = 0
        self.last_tick_volume: float = 0

        self.put_event()

    def on_tick(self, tick: TickData) -> None:
        """Tick行情回调"""
        # 如果有活动委托，先撤单
        if self.vt_orderid:
            self.cancel_all()
            return

        # 记录当前tick的成交量
        current_volume: float = tick.volume - self.last_tick_volume
        self.last_tick_volume = tick.volume

        # 如果没有最新成交量，则返回
        if current_volume <= 0:
            return

        # 计算最大委托数量：成交量的1/5
        max_order_volume: float = current_volume / 5

        # 计算剩余需要交易的数量
        volume_left: float = self.volume - self.traded
        order_volume: float = min(max_order_volume, volume_left)

        # 检查委托数量是否为0
        if order_volume <= 0:
            self.write_log(f"上一tick成交量为0，不生成委托")
            return

        self.write_log(f"剩余需要交易的数量: {volume_left}, 跟量拆单上限: {max_order_volume}")

        # 根据方向计算委托价格
        if self.direction == Direction.LONG:
            # 买入超价：在卖一价上加价
            self.order_price = tick.ask_price_1 * (1 + self.price_add_percent / 100)

            # 检查涨停价
            if tick.limit_up:
                self.order_price = min(self.order_price, tick.limit_up)

            self.vt_orderid = self.buy(
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

            self.vt_orderid = self.sell(
                self.order_price,
                order_volume,
                offset=self.offset
            )

        self.put_event()

    def on_order(self, order: OrderData) -> None:
        """委托回调"""
        if not order.is_active():
            self.vt_orderid = ""
            self.order_price = 0
            self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """成交回调"""
        if self.traded >= self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}")
            self.finish()
        else:
            self.put_event() 