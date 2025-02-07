from datetime import datetime
from vnpy.trader.constant import Direction, Offset, Status, OrderType
from vnpy.trader.object import TradeData, OrderData, TickData
from vnpy.trader.engine import BaseEngine
from vnpy.trader.utility import round_to
from ..template import AlgoTemplate


class VolumeFollowAlgo(AlgoTemplate):
    """跟量算法"""

    display_name: str = "VolumeFollow 跟量"

    default_setting: dict = {
        "price_add_percent": 2.0,  # 超价比例
    }

    variables: list = [
        "tick_volume"
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
        self.is_first_tick: bool = True  # 标记是否是第一个tick
        self.tick: TickData = None  # 最新tick数据
        self.tick_volume: float = 0  # 当前tick的成交量
        
        # 订单管理
        self.cancel_orderids: set = set()  # 等待撤单委托号集合
        self.pending_orderids: set = set()  # 已发出但未收到回报的订单号集合
        self.order_cancel_time: dict = {}  # 记录pending订单的撤单时间

        self.put_event()

    def on_tick(self, tick: TickData) -> None:
        """Tick行情回调"""
        # 处理第一个tick
        if self.is_first_tick:
            self.is_first_tick = False
            return

        # 计算当前tick的成交量
        self.tick_volume = tick.volume - (self.tick.volume if self.tick else tick.volume)
        self.tick = tick

        # 如果没有最新成交量，则返回
        if self.tick_volume <= 0:
            return

        # 检查是否有需要清理的超时撤单订单
        self.check_cancelled_timeout_orders()

        # 执行委托
        self.trade()

    def check_cancelled_timeout_orders(self) -> None:
        """检查超时的撤单订单"""
        for vt_orderid in list(self.pending_orderids):
            if vt_orderid not in self.order_cancel_time:
                continue
                
            cancel_time = self.order_cancel_time[vt_orderid]
            if (datetime.now() - cancel_time).total_seconds() > 10:
                self.write_log(f"订单 {vt_orderid} 撤单超过10秒未收到回报，判定为丢失订单")
                self.pending_orderids.remove(vt_orderid)
                self.order_cancel_time.pop(vt_orderid)

    def trade(self) -> None:
        """执行委托"""
        if not self.check_order_finished():
            self.cancel_old_order()
        else:
            self.send_new_order()

    def check_order_finished(self) -> bool:
        """检查委托是否全部结束"""
        # 检查是否有活跃订单或未收到回报的订单
        if self.active_orders or self.pending_orderids:
            return False
        return True

    def cancel_old_order(self) -> None:
        """撤销旧委托"""
        # 撤销活跃订单
        for vt_orderid in self.active_orders:
            if vt_orderid not in self.cancel_orderids:
                self.cancel_order(vt_orderid)
                self.cancel_orderids.add(vt_orderid)

        # 撤销未收到回报的订单
        for vt_orderid in self.pending_orderids:
            if vt_orderid not in self.cancel_orderids:
                self.cancel_order(vt_orderid)
                self.cancel_orderids.add(vt_orderid)
                self.order_cancel_time[vt_orderid] = datetime.now()  # 只记录pending订单的撤单时间

    def send_new_order(self) -> None:
        """发送新委托"""
        # 获取合约信息
        contract = self.get_contract()
        if not contract:
            return
        
        # 计算最大委托数量：成交量的1/5
        max_order_volume: float = self.tick_volume / 5

        # 计算剩余可发送的订单量
        volume_left: float = self.volume - self.traded
        if volume_left <= 0:
            return

        # 计算委托数量
        order_volume: float = min(max_order_volume, volume_left)
        if order_volume <= 0:
            return
            
        # 根据方向计算委托价格
        if self.direction == Direction.LONG:
            # 买入超价：在卖一价上加价
            order_price = self.tick.ask_price_1 * (1 + self.price_add_percent / 100)
            # 检查涨停价
            if self.tick.limit_up:
                order_price = min(order_price, self.tick.limit_up)
        else:
            # 卖出超价：在买一价下降价
            order_price = self.tick.bid_price_1 * (1 - self.price_add_percent / 100)
            # 检查跌停价
            if self.tick.limit_down:
                order_price = max(order_price, self.tick.limit_down)
            
        order_volume = round_to(order_volume, contract.min_volume)
        order_price = round_to(order_price, contract.pricetick)
        min_notional: float = contract.extra.get("min_notional", 0)
        if min_notional > 0:
            # 检查当前订单的名义价值
            current_notional = order_price * order_volume
            if current_notional < min_notional:
                self.write_log(f"当前订单量{order_volume}名义价值 {current_notional} 小于最小名义价值 {min_notional}")
                return
                
            # 检查剩余未成交部分是否满足最小名义价值
            remaining_volume = round_to(volume_left - order_volume, contract.min_volume)
            if remaining_volume > 0:
                remaining_notional = order_price * remaining_volume * 0.95
                if remaining_notional < min_notional:
                    self.write_log(f"剩余订单量{remaining_volume}名义价值 {remaining_notional} 小于最小名义价值 {min_notional}")
                    return

        # 发送订单
        if self.direction == Direction.LONG:
            vt_orderid = self.buy(order_price, order_volume, offset=self.offset)
        else:
            vt_orderid = self.sell(order_price, order_volume, offset=self.offset)

        # 记录已发出但未收到回报的订单号
        if vt_orderid:
            self.pending_orderids.add(vt_orderid)
            self.write_log(f"发送新订单 {vt_orderid}，方向：{self.direction}，价格：{order_price}，数量：{order_volume}")

    def on_order(self, order: OrderData) -> None:
        """委托回调"""
        self.write_log(f"收到订单回报 {order.vt_orderid}，状态：{order.status}，已成交：{order.traded}，剩余：{order.volume - order.traded}")
        
        # 收到订单回报后，移除已发出订单记录
        if order.vt_orderid in self.pending_orderids:
            self.pending_orderids.remove(order.vt_orderid)
            if order.vt_orderid in self.order_cancel_time:
                self.order_cancel_time.pop(order.vt_orderid)
                self.write_log(f"订单 {order.vt_orderid} 已从撤单超时监控中移除")
            
        if not order.is_active():
            if order.vt_orderid in self.cancel_orderids:
                self.cancel_orderids.remove(order.vt_orderid)
                self.write_log(f"订单 {order.vt_orderid} 已从撤单集合中移除")
            self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        """成交回调"""
        self.write_log(f"收到成交回报 {trade.vt_orderid}，价格：{trade.price}，数量：{trade.volume}，方向：{trade.direction}")
        
        if self.traded >= self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}，算法执行完成")
            self.finish()
        else:
            # 获取合约信息以处理精度
            contract = self.get_contract()
            if not contract:
                self.write_log(f"error! 找不到合约信息: {trade.vt_symbol}")
                return
                
            # 使用round_to处理精度后再比较
            if round_to(self.traded - self.volume, contract.min_volume) >= 0:
                self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}，算法执行完成")
                self.finish()
            else:
                self.put_event() 