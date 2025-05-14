from datetime import datetime, timedelta
from vnpy.trader.constant import Direction, Offset, Status, OrderType
from vnpy.trader.object import TradeData, OrderData, TickData, BarData
from vnpy.trader.engine import BaseEngine
from vnpy.trader.utility import round_to
from ..template import AlgoTemplate


class VolumeFollowAlgo(AlgoTemplate):
    """跟量算法"""

    display_name: str = "VolumeFollow 跟量"

    default_setting: dict = {
        "price_add_percent": 2.0,  # 超价比例
        "min_notional_multiplier": 2.0,  # 最小名义价值倍数
        "volume_ratio": 0.2,  # 跟量比例
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
        self.min_notional_multiplier: float = setting.get("min_notional_multiplier", self.default_setting["min_notional_multiplier"])
        self.volume_ratio: float = setting.get("volume_ratio", self.default_setting["volume_ratio"])

        # 变量
        self.tick: TickData = None  # 最新tick数据
        self.tick_volume: float = 0  # 当前tick的成交量
        
        # 分钟级数据
        self.current_minute: datetime = None  # 当前分钟时间
        self.minute_first_volume: float = 0  # 当前分钟第一个ticker的成交量
        self.minute_first_ticker_time: datetime = None  # 当前分钟第一个ticker的时间
        self.minute_last_volume: float = 0  # 当前分钟最后一个ticker的成交量
        self.last_bar_volume: float = 0  # 上一分钟K线的成交量
        
        # 拆单相关变量
        self.last_ticker_time: datetime = None  # 上一次volume发生变化的时间（ticker推送）
        self.last_volume_split_time: datetime = None  # 上一次分配volume的时间
        self.split_volume: float = 0  # 每次分配的volume
        self.split_count: int = 0  # 需要分配的总次数
        self.allocated_count: int = 0  # 已分配的次数
        
        # 订单管理
        self.cancel_orderids: set = set()  # 等待撤单委托号集合
        self.pending_orderids: set = set()  # 已发出但未收到回报的订单号集合
        self.order_cancel_time: dict = {}  # 记录pending订单的撤单时间

        self.put_event()

    def on_timer(self) -> None:
        """定时回调"""
        pass

    def on_tick(self, tick: TickData) -> None:
        """Tick行情回调"""
        # 处理第一个tick
        if self.tick is None:
            self.tick = tick
            self.last_ticker_time = tick.datetime
            self.current_minute = tick.datetime.replace(second=0, microsecond=0)
            self.minute_first_volume = tick.volume
            self.minute_first_ticker_time = tick.datetime
            self.minute_last_volume = tick.volume
            return

        new_minute = tick.datetime.replace(second=0, microsecond=0)

        # 计算实际volume变化
        real_volume_change = tick.volume - self.tick.volume

        # 如果是新的一分钟
        if new_minute > self.current_minute:
            # 检查是否只差一分钟
            minute_diff = int((new_minute - self.current_minute).total_seconds() / 60)
            
            # 新的一分钟，且只差1分钟
            if (minute_diff == 1 
                and self.last_bar_volume > 0 
                and self.minute_first_ticker_time 
                and self.minute_first_ticker_time.minute == self.current_minute.minute  # 确保是上一分钟的数据
                and self.minute_first_ticker_time.second <= 5
            ):
                # 只有当上一分钟的第一个ticker是在前5秒内收到时，才使用bar数据修正volume_change
                minute_volume = self.minute_last_volume - self.minute_first_volume
                real_volume_change = self.last_bar_volume - minute_volume
            else:
                real_volume_change = 0
                
            # 更新分钟相关数据
            self.minute_first_volume = tick.volume
            self.minute_first_ticker_time = tick.datetime
            self.current_minute = new_minute
        else:
            # 同一分钟内
            real_volume_change = tick.volume - self.tick.volume
            
        # 更新最后一个tick的volume
        self.minute_last_volume = tick.volume
        
        # 根据是否有实际volume变化来处理
        if real_volume_change > 0:
            self.write_log(f"检测到volume变化：{real_volume_change}")
            # 获取合约信息
            contract = self.get_contract()
            if not contract:
                self.write_log("获取合约信息失败")
                return
                
            # 获取最小名义价值
            min_notional = contract.extra.get("min_notional", 0)
            if not min_notional:
                self.write_log("获取最小名义价值失败")
                return

            # 计算ticker之间的时间差
            ticker_time_diff = tick.datetime - self.last_ticker_time
            self.last_ticker_time = tick.datetime
            
            # 重置计数器
            self.allocated_count = 0

            # 计算需要分配的次数
            time_based_count = max(1, int(ticker_time_diff.total_seconds() / 0.5))  # 修改：确保至少1次
            
            # 计算基于最小名义价值的最大拆分次数
            notional_value = real_volume_change * tick.last_price
            min_split_notional = min_notional * self.min_notional_multiplier / self.volume_ratio
            max_splits = max(1, int(notional_value / min_split_notional))  # 修改：确保至少1次
            
            # 取两者的较小值作为实际拆分次数
            self.split_count = min(time_based_count, max_splits)
            
            # 计算每次分配的volume
            self.split_volume = real_volume_change / self.split_count
            
            # 分配当前ticker的volume
            self.tick_volume = self.split_volume
            self.write_log(f"当前ticker实际volume：{real_volume_change}，时间拆分{time_based_count}次，价值拆分{max_splits}次，实际拆分{self.split_count}次，每次分配volume：{self.split_volume}")
            self.allocated_count += 1
            self.last_volume_split_time = tick.localtime
            
        elif real_volume_change == 0:
            # depth推送，计算距离上次分配的时间差
            if self.last_volume_split_time and self.allocated_count < self.split_count:
                time_since_last_split = tick.localtime - self.last_volume_split_time
                
                # 如果距离上次分配超过0.5秒，分配一次volume
                if time_since_last_split.total_seconds() >= 0.5:
                    self.tick_volume = self.split_volume
                    self.allocated_count += 1
                    self.last_volume_split_time = tick.localtime
                    self.write_log(f"距上次分配{time_since_last_split.total_seconds():.3f}秒，分配volume：{self.split_volume}，已分配{self.allocated_count}/{self.split_count}次")
                else:
                    self.tick_volume = 0
            else:
                self.tick_volume = 0
        else:
            self.tick_volume = 0
                
        self.tick = tick

        # 如果没有最新成交量，则返回
        if self.tick_volume <= 0:
            return

        # 检查是否有需要清理的超时撤单订单
        self.check_cancelled_timeout_orders()

        # 执行委托
        self.trade()

    def on_bar(self, bar: BarData) -> None:
        """K线数据更新"""
        self.last_bar_volume = bar.volume

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
        
        # 计算最大委托数量：使用volume_ratio参数
        max_order_volume: float = self.tick_volume * self.volume_ratio
        max_order_volume = round_to(max_order_volume, contract.min_volume)

        # 计算剩余可发送的订单量
        volume_left: float = self.volume - self.traded
        volume_left = round_to(volume_left, contract.min_volume)
        
        order_volume_abs: float = min(max_order_volume, abs(volume_left))
        if order_volume_abs == 0:
            return
            
        # 根据方向计算委托价格
        if self.direction == Direction.LONG and volume_left > 0 or self.direction == Direction.SHORT and volume_left < 0:
            # 买入超价：在卖一价上加价
            order_price = self.tick.last_price * (1 + self.price_add_percent / 100)
            # 检查涨停价
            if self.tick.limit_up:
                order_price = min(order_price, self.tick.limit_up)
        elif self.direction == Direction.SHORT and volume_left > 0 or self.direction == Direction.LONG and volume_left < 0:
            # 卖出超价：在买一价下降价
            order_price = self.tick.last_price * (1 - self.price_add_percent / 100)
            # 检查跌停价
            if self.tick.limit_down:
                order_price = max(order_price, self.tick.limit_down)
        else:
            return            
        order_price = round_to(order_price, contract.pricetick)

        min_notional: float = contract.extra.get("min_notional", 0)
        if min_notional == 0:
            return

        vt_orderid = None

        if volume_left > 0:
            # 检查当前订单的名义价值
            current_notional = order_price * order_volume_abs
            if current_notional >= min_notional:
                # 检查剩余未成交部分是否满足最小名义价值
                remaining_volume = round_to(volume_left - order_volume_abs, contract.min_volume)
                if min_notional * self.min_notional_multiplier > order_price * remaining_volume > 0:
                    return

                # 发送普通订单
                if self.direction == Direction.LONG:
                    vt_orderid = self.buy(order_price, order_volume_abs)
                else:
                    vt_orderid = self.sell(order_price, order_volume_abs)

                # 记录已发出但未收到回报的订单号
                if vt_orderid:
                    self.write_log(f"发送新订单 {vt_orderid}，方向：{self.direction}，价格：{order_price}，数量：{order_volume_abs}")
            
            else:
                # 获取当前持仓
                current_pos = self.get_holding()                
                
                if (current_pos < 0 if self.direction == Direction.LONG else current_pos > 0) and abs(current_pos) >= volume_left:
                    # 情况1：持仓与volume_left方向相反且持仓绝对值大于等于volume_left：直接平仓
                    if self.direction == Direction.LONG:
                        vt_orderid = self.buy(order_price, order_volume_abs, offset=Offset.CLOSE)
                    else:
                        vt_orderid = self.sell(order_price, order_volume_abs, offset=Offset.CLOSE)
                        
                    if vt_orderid:
                        self.write_log(f"发送平仓订单 {vt_orderid}，方向：{self.direction}，价格：{order_price}，数量：{order_volume_abs}")
                elif volume_left * order_price <= min_notional:
                    # 情况2：剩余量的名义价值<=最小名义价值要求时，发送最小的大于等于min_notional的订单，持仓调成反的，下次进入send_new_order就能用平仓单反向平掉多发的量
                    min_volume = min_notional / order_price
                    min_volume = round_to(min_volume, contract.min_volume)
                    if min_volume * order_price < min_notional:  # 确保四舍五入后的volume对应的金额仍然大于等于min_notional
                        min_volume = round_to(min_volume + contract.min_volume, contract.min_volume)
                    
                    if self.direction == Direction.LONG:
                        vt_orderid = self.buy(order_price, min_volume)
                    else:
                        vt_orderid = self.sell(order_price, min_volume)
                        
                    if vt_orderid:
                        self.write_log(f"发送最小名义价值订单 {vt_orderid}，方向：{self.direction}，价格：{order_price}，数量：{min_volume}")
                else:
                    # 其他情况不发送订单
                    pass
                    
        elif volume_left < 0:
            # 如果volume_left < 0，说明是之前发送>=min_notional的订单导致的超发：发送反向平仓单
            current_notional = order_price * order_volume_abs
            reverse_direction = "空头" if self.direction == Direction.LONG else "多头"
            if current_notional >= min_notional:
                # 检查剩余未成交部分是否满足最小名义价值
                remaining_volume = round_to(volume_left - order_volume_abs, contract.min_volume)
                if min_notional * self.min_notional_multiplier > order_price * remaining_volume > 0:
                    return
                
                # 发送普通订单
                if self.direction == Direction.LONG:
                    vt_orderid = self.sell(order_price, order_volume_abs)
                else:
                    vt_orderid = self.buy(order_price, order_volume_abs)

                if vt_orderid:
                    self.write_log(f"发送反向平仓订单 {vt_orderid}，方向：{reverse_direction}，价格：{order_price}，数量：{order_volume_abs}")

            else:
                # 获取当前持仓
                current_pos = self.get_holding()
                if (current_pos < 0 if self.direction == Direction.SHORT else current_pos > 0) and abs(current_pos) >= abs(volume_left):
                    # 情况1：持仓与volume_left方向相反且持仓绝对值大于等于abs(volume_left)：直接平仓
                    if self.direction == Direction.SHORT:
                        vt_orderid = self.buy(order_price, order_volume_abs, offset=Offset.CLOSE)
                    else:
                        vt_orderid = self.sell(order_price, order_volume_abs, offset=Offset.CLOSE)
                        
                    if vt_orderid:
                        self.write_log(f"发送反向平仓订单 {vt_orderid}，方向：{reverse_direction}，价格：{order_price}，数量：{order_volume_abs}")
                elif abs(volume_left) * order_price <= min_notional:
                    # 情况2：剩余量的名义价值<=最小名义价值要求时，发送最小的大于等于min_notional的订单，持仓调成反的，下次进入send_new_order就能用平仓单反向平掉多发的量
                    min_volume = min_notional / order_price
                    min_volume = round_to(min_volume, contract.min_volume)
                    if min_volume * order_price < min_notional:  # 确保四舍五入后的volume对应的金额仍然大于等于min_notional
                        min_volume = round_to(min_volume + contract.min_volume, contract.min_volume)

                    if self.direction == Direction.SHORT:
                        vt_orderid = self.buy(order_price, min_volume)
                    else:
                        vt_orderid = self.sell(order_price, min_volume)

                    if vt_orderid:
                        self.write_log(f"发送反向平仓订单 {vt_orderid}，方向：{reverse_direction}，价格：{order_price}，数量：{min_volume}")
                else:
                    # 其他情况不发送订单
                    pass

        if vt_orderid:
            self.pending_orderids.add(vt_orderid)

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
        
        if self.traded == self.volume:
            self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}，算法执行完成")
            self.finish()
        else:
            # 获取合约信息以处理精度
            contract = self.get_contract()
            if not contract:
                self.write_log(f"error! 找不到合约信息: {trade.vt_symbol}")
                return
                
            # 使用round_to处理精度后再比较
            if round_to(self.traded - self.volume, contract.min_volume) == 0:
                self.write_log(f"已交易数量：{self.traded}，总数量：{self.volume}，算法执行完成")
                self.finish()
            else:
                self.put_event() 