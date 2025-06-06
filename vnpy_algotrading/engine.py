import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, Type, Set

from vnpy.event import EventEngine, Event
from vnpy.trader.constant import Direction, Offset, OrderType, Exchange
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.event import (EVENT_TICK, EVENT_TIMER, EVENT_ORDER, EVENT_TRADE, EVENT_CONTRACT)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (SubscribeRequest, OrderRequest, LogData, ContractData, OrderData, TickData, TradeData,
                                CancelRequest, BarData)
from vnpy.trader.utility import round_to, load_json

from prod.barGen_redis_engine import EVENT_BAR
from .base import (EVENT_ALGO_LOG, EVENT_ALGO_UPDATE, APP_NAME, AlgoStatusEnum, AlgoTemplateEnum, is_active,
                   is_finished)
from .converter import PositionManager
from .database import (Todo, AlgoOrder, init_database)
from .template import AlgoTemplate

# 全局映射关系
DIRECTION_MAP = {
    "多": Direction.LONG,
    "空": Direction.SHORT
}

OFFSET_MAP = {
    "开": Offset.OPEN,
    "平": Offset.CLOSE,
    "平今": Offset.CLOSETODAY,
    "平昨": Offset.CLOSEYESTERDAY
}

class AlgoEngine(BaseEngine):
    """算法引擎"""

    # 配置文件名
    setting_filename: str = "algo_trading_setting.json"

    # 测试配置
    TEST_SYMBOLS = ["ETHUSDT.BINANCE"]  # 测试用的交易对
    TEST_NOTIONAL_RANGE = (1, 100)     # 测试订单的名义价值范围
    TEST_PRICE_DIVIDER = 3000           # 用于计算测试订单数量的除数
    TIMER_INTERVAL = 30                 # 测试订单生成间隔(秒)
    ALERT_INTERVAL = 3600               # 告警间隔(秒)

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """构造函数"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.algo_templates: dict[str, Type[AlgoTemplate]] = {}
        self.algos: dict[int, AlgoTemplate] = {}  # todo_id: algo
        self.symbol_algo_map: dict[str, set[AlgoTemplate]] = defaultdict(set)
        self.orderid_algo_map: dict[str, AlgoTemplate] = {}
        self.position_manager: PositionManager = PositionManager(event_engine, self)

        # 调度相关的属性
        self.processed_todos: Set[int] = set()  # 已处理的Todo ID集合
        self.is_stopping: bool = False  # 是否正在停止
        self.pending_orders: set = set()  # 等待撤单回报的订单集合
        self.resume_completed: bool = False  # 标记恢复过程是否完成

        # 合约就绪状态和待启动队列
        self.contract_ready: dict[str, bool] = {}
        self.start_requests: dict[str, set[int]] = defaultdict(set)  # vt_symbol: set[todo_id]

        # 配置参数
        self.allow_multiple_algos: bool = True  # 是否允许同一交易对运行多个算法

        # 测试相关属性
        self.timer_count: int = 0  # 计时器计数
        self.test_enabled: bool = False  # 是否启用测试订单生成

        # 告警相关属性
        self.last_phone_alert_time: datetime = None  # 上次电话告警时间

        # 加载配置
        self.algo_settings = load_json(self.setting_filename)
        self.write_log(f"算法交易引擎配置加载完成：\n{self.algo_settings}")

        # 加载算法模板
        self.load_algo_template()
        # 注册事件
        self.register_event()

    def start(self, test_enabled: bool = False, allow_multiple_algos: bool = True) -> None:
        """
        启动引擎
        
        参数:
            test_enabled: bool
                是否启用测试订单生成，默认为False
            allow_multiple_algos: bool
                是否允许同一交易对运行多个算法，默认为True
        """
        # 设置测试标志
        self.test_enabled = test_enabled
        self.allow_multiple_algos = allow_multiple_algos

        # 初始化数据库
        init_database()
        # 初始化引擎
        self.init_engine()
        self.write_log("算法交易引擎启动完成")

    def init_engine(self) -> None:
        """初始化引擎"""
        # 恢复未完成的算法单
        self.resume_algo_orders()

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
        from .algos.volume_follow_sync_algo import VolumeFollowSyncAlgo

        self.add_algo_template(TwapAlgo)
        self.add_algo_template(IcebergAlgo)
        self.add_algo_template(SniperAlgo)
        self.add_algo_template(StopAlgo)
        self.add_algo_template(BestLimitAlgo)
        self.add_algo_template(VolumeFollowAlgo)
        self.add_algo_template(VolumeFollowSyncAlgo)

    def add_algo_template(self, template: AlgoTemplate) -> None:
        """添加算法类"""
        self.algo_templates[template.__name__] = template

    def get_algo_template(self) -> dict:
        """获取算法类"""
        return self.algo_templates

    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_BAR, self.process_bar_event)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_ALGO_UPDATE, self.process_algo_event)

    def process_tick_event(self, event: Event) -> None:
        """处理行情事件"""
        tick: TickData = event.data
        algos: set[AlgoTemplate] = self.symbol_algo_map[tick.vt_symbol]

        for algo in algos:
            algo.update_tick(tick)

    def process_bar_event(self, event: Event) -> None:
        """处理K线事件"""
        bar: BarData = event.data
        algos: set[AlgoTemplate] = self.symbol_algo_map[bar.vt_symbol]

        for algo in algos:
            algo.update_bar(bar)

    def process_timer_event(self, event: Event) -> None:
        """处理定时事件"""
        # 生成列表避免字典改变
        algos: list[AlgoTemplate] = list(self.algos.values())
        for algo in algos:
            algo.update_timer()

        # 处理待执行订单
        self.process_todo_orders()

        # 如果启用了测试，每隔TIMER_INTERVAL秒生成一个新订单
        if self.test_enabled:
            if self.timer_count == 0:
                self.generate_test_order()
            self.timer_count += 1
            if self.timer_count >= self.TIMER_INTERVAL:
                self.timer_count = 0

    def process_trade_event(self, event: Event) -> None:
        """处理成交事件"""
        trade: TradeData = event.data

        algo: Optional[AlgoTemplate] = self.orderid_algo_map.get(trade.vt_orderid, None)

        if algo and is_active(algo.status):
            algo.update_trade(trade)

    def process_order_event(self, event: Event) -> None:
        """处理委托事件"""
        order: OrderData = event.data

        # 处理算法交易订单
        algo: Optional[AlgoTemplate] = self.orderid_algo_map.get(order.vt_orderid, None)
        if algo and is_active(algo.status):
            algo.update_order(order)

        # 处理关闭引擎时的撤单回报
        if order.vt_orderid in self.pending_orders:
            if not order.is_active():
                self.pending_orders.remove(order.vt_orderid)

    def process_algo_event(self, event: Event) -> None:
        """处理算法事件"""
        data = event.data
        todo_id = data.get("todo_id")
        if not todo_id:
            return

        # 更新算法单状态
        self.update_algo_status(
            todo_id=todo_id,
            status=data["status"],
            traded=data["traded"],
            traded_price=data["traded_price"],
            black_hole_volume=data["black_hole_volume"]
        )

    def update_algo_status(self, todo_id: int, status: int, traded: float = 0, traded_price: float = 0, black_hole_volume: float = 0) -> None:
        """更新算法单状态"""
        # 更新算法单状态
        AlgoOrder.update(
            traded=traded,
            traded_price=traded_price,
            black_hole_volume=black_hole_volume,
            status=status,
            update_time=datetime.now()
        ).where(AlgoOrder.todo_id == todo_id).execute()

        # 如果算法完成，更新Todo状态并处理新订单
        if is_finished(status):
            # 检查是否完全成交
            algo_order = AlgoOrder.get(AlgoOrder.todo_id == todo_id)
            # completed_status = 11 if algo_order.traded == algo_order.volume else 5
            if algo_order.traded == algo_order.volume and algo_order.black_hole_volume == 0:
                completed_status = 11
            elif algo_order.traded + algo_order.black_hole_volume == algo_order.volume:
                completed_status = 5
            elif status == AlgoStatusEnum.STOPPED:
                completed_status = 12
            else:
                completed_status = 5

            Todo.update(
                completed=completed_status
            ).where(Todo.id == todo_id).execute()

            # 从已处理集合中移除，允许重新处理
            if todo_id in self.processed_todos:
                self.processed_todos.remove(todo_id)

            self.write_log(str(algo_order))

    def resume_algo_orders(self) -> None:
        """恢复数据库中活动的算法单"""
        # 获取可设置的恢复时限(小时),默认1小时
        resume_hours = 1

        # 计算截止时间
        cutoff_time = datetime.now() - timedelta(hours=resume_hours)

        # 查询数据库中需要恢复的算法单
        running_algos = AlgoOrder.select().where(
            (AlgoOrder.status.in_([
                AlgoStatusEnum.RUNNING,
                AlgoStatusEnum.PAUSED
            ])) &
            (AlgoOrder.start_time >= cutoff_time)  # 只恢复创建时间在截止时间之后的订单
        )

        all_result = True
        for algo_order in running_algos:
            todo = Todo.get_or_none(Todo.id == algo_order.todo_id)
            if not todo:
                self.write_log(f"找不到对应的Todo记录: {algo_order.todo_id}")
                continue

            # 计算剩余数量
            volume_left = algo_order.volume - algo_order.traded - algo_order.black_hole_volume
            if volume_left == 0:
                self.update_algo_status(algo_order.todo_id,
                AlgoStatusEnum.FINISHED,
                algo_order.traded,
                algo_order.traded_price,
                algo_order.black_hole_volume)
                continue

            # 添加到已处理集合
            self.processed_todos.add(algo_order.todo_id)

            # 使用新的启动方法
            result = self.start_algo_with_retry(algo_order.todo_id)

            if result == -1:  # 启动失败
                self.update_algo_status(algo_order.todo_id,
                                        AlgoStatusEnum.PAUSED,
                                        algo_order.traded,
                                        algo_order.traded_price,
                                        algo_order.black_hole_volume)
                all_result = False
                continue

            self.write_log(str(algo_order))

        self.resume_completed = True
        self.write_log("恢复算法单完成")

        if not all_result:
            self.query_contract()

    def query_contract(self) -> None:
        """查询合约"""
        try:
            self.main_engine.get_gateway("BINANCE_LINEAR").rest_api.query_contract()
        except Exception as e:
            try:
                gateway: BaseGateway = list(self.main_engine.gateways.values())[0]
                gateway.rest_api.query_contract()
            except Exception as e:
                self.write_log(f"查询合约失败：{e}")

    def process_contract_event(self, event: Event) -> None:
        """处理合约事件"""
        contract = event.data
        vt_symbol = contract.vt_symbol

        # 标记合约就绪
        if not self.contract_ready.get(vt_symbol, False):
            self.contract_ready[vt_symbol] = True
            # 如果是测试，且是被测试合约，记录日志
            if self.test_enabled and vt_symbol in self.TEST_SYMBOLS:
                self.write_log(f"合约就绪: {vt_symbol}, 处理该合约的所有待启动算法")

            # 处理该合约的所有待启动算法
            todo_ids = self.start_requests[vt_symbol]
            for todo_id in list(todo_ids):  # 使用list创建副本进行遍历
                result = self.start_algo_with_retry(todo_id)
                if result == -1:
                    self.query_contract()
                    break

    def start_algo_with_retry(
        self,
        todo_id: int
    ) -> int:
        """带重试的算法启动"""
        # 从数据库读取算法单信息
        algo_order = AlgoOrder.get_or_none(AlgoOrder.todo_id == todo_id)
        if not algo_order:
            self.write_log(f"启动失败: 找不到对应的算法单记录(todo_id={todo_id})")
            return -1

        # 检查是否允许启动新算法
        if not self.allow_multiple_algos and self.symbol_algo_map[algo_order.vt_symbol]:
            self.write_log(f"启动失败: {algo_order.vt_symbol} 已有运行中的算法单")
            return -1

        self.start_requests[algo_order.vt_symbol].add(todo_id)

        # 如果合约已就绪，立即尝试启动
        if self.contract_ready.get(algo_order.vt_symbol, False):
            # 转换方向和开平
            direction = DIRECTION_MAP.get(algo_order.direction, Direction.NET)
            offset = OFFSET_MAP.get(algo_order.offset, Offset.NONE)

            # 获取算法参数
            template_name = AlgoTemplateEnum.to_str(algo_order.template)
            setting = self.algo_settings[template_name]

            # 启动算法
            result = self.start_algo(
                template_name=template_name,
                vt_symbol=algo_order.vt_symbol,
                direction=direction,
                offset=offset,
                price=algo_order.price,
                volume=algo_order.volume,
                setting=setting,
                todo_id=todo_id,
                traded=algo_order.traded,
                traded_price=algo_order.traded_price,
                black_hole_volume=algo_order.black_hole_volume
            )

            if result == -1:
                # 启动失败，保持在集合中并关闭合约就绪状态
                self.contract_ready[algo_order.vt_symbol] = False
                self.write_log(f"合约未就绪：{algo_order.vt_symbol}，算法启动失败")
                return -1

            # 启动成功，从集合中移除
            self.start_requests[algo_order.vt_symbol].remove(todo_id)
            return result
        else:
            self.write_log(f"合约未就绪，保持在等待集合中：{algo_order.vt_symbol}")
            return -1

    def process_todo_orders(self) -> None:
        """处理待执行订单"""
        # 等待恢复过程完成
        if not self.resume_completed:
            return

        # 查询新创建的订单
        todos = Todo.select().where(
            (Todo.completed == 10) &  # 新创建的任务
            (Todo.id.not_in(self.processed_todos))  # 排除已处理的订单
        )

        all_result = True
        for todo in todos:
            # 标记为已处理
            self.processed_todos.add(todo.id)

            # 检查是否已存在相同todo_id的算法单
            existing_algo = AlgoOrder.get_or_none(AlgoOrder.todo_id == todo.id)
            if existing_algo:
                self.write_log(f"error 存在运行中未恢复的算法单，跳过: todo_id={todo.id}")
                continue

            # 转换方向和开平
            direction = DIRECTION_MAP.get(todo.direction, Direction.NET)
            offset = OFFSET_MAP.get(todo.offset, Offset.NONE)

            if direction == Direction.NET:
                self.write_log(f"无效的方向: {todo.direction}, todo_id: {todo.id}")
                continue

            # 创建算法单记录
            algo_order = AlgoOrder(
                todo_id=todo.id,
                vt_symbol=todo.vt_symbol,
                direction=todo.direction,
                offset=todo.offset,
                price=todo.price,
                volume=todo.real_volume,
                traded=0,
                traded_price=0,
                black_hole_volume=0,
                status=AlgoStatusEnum.RUNNING,
                # template=AlgoTemplateEnum.VolumeFollowAlgo, # 默认使用跟量算法
                template=AlgoTemplateEnum.VolumeFollowSyncAlgo,
                start_time=datetime.now(),
                update_time=datetime.now()
            )
            algo_order.save()

            # 使用新的启动方法
            result = self.start_algo_with_retry(todo_id=todo.id)

            if result == -1:  # 启动失败
                # 更新Todo和AlgoOrder状态
                Todo.update(completed=5).where(Todo.id == todo.id).execute()
                AlgoOrder.update(
                    status=AlgoStatusEnum.PAUSED,
                    update_time=datetime.now()
                ).where(AlgoOrder.todo_id == todo.id).execute()
                all_result = False
                continue

            # 获取最新的算法单信息
            algo_order = AlgoOrder.get(AlgoOrder.todo_id == todo.id)
            self.write_log(str(algo_order))

        if not all_result:
            self.query_contract()

    def stop_all(self) -> None:
        """停止全部算法"""
        if self.is_stopping:
            return

        self.is_stopping = True

        # 暂停所有算法单并撤销所有活动委托
        for algo in self.algos.values():
            self.pending_orders.update(algo.active_orders.keys())
            algo.pause()
            algo.cancel_all()

        # 等待所有撤单回报
        wait_count = 0
        while self.pending_orders and wait_count < 10:  # 最多等待10秒
            time.sleep(1)
            wait_count += 1
            self.write_log(f"等待撤单回报，剩余：{len(self.pending_orders)}")

        # 更新数据库中算法单状态
        self.update_all_algo_status()

        self.write_log("算法交易引擎安全停止完成")

    def update_all_algo_status(self) -> None:
        """更新所有算法单状态为暂停"""
        AlgoOrder.update(
            status=AlgoStatusEnum.PAUSED,
            update_time=datetime.now()
        ).where(
            AlgoOrder.status == AlgoStatusEnum.RUNNING
        ).execute()

    def start_algo(
        self,
        template_name: str,
        vt_symbol: str,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        setting: dict,
        todo_id: int = 0,
        traded: float = 0,
        traded_price: float = 0,
        black_hole_volume: float = 0
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

        # 设置已成交信息
        algo.traded = traded
        algo.traded_price = traded_price
        algo.black_hole_volume = black_hole_volume

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

    def subscribe(self, symbol: str, exchange: Exchange, gateway_name: str) -> None:
        """订阅行情"""
        req: SubscribeRequest = SubscribeRequest(
            symbol=symbol,
            exchange=exchange
        )
        self.main_engine.subscribe(req, gateway_name)

    def unsubscribe(self, symbol: str, exchange: Exchange, gateway_name: str) -> None:
        """退订行情"""
        req: SubscribeRequest = SubscribeRequest(
            symbol=symbol,
            exchange=exchange
        )
        if not hasattr(self.main_engine, "unsubscribe"):
            gateway: BaseGateway = self.main_engine.get_gateway(gateway_name)
            if gateway:
                gateway.unsubscribe(req)
        else:
            self.main_engine.unsubscribe(req, gateway_name)

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

        if not volume:
            self.write_log(f"委托数量为0，不生成委托")
            return ""
        if not price:
            self.write_log(f"委托价格为0，不生成委托")
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

    def write_log(self, msg: str, algo: Optional[AlgoTemplate] = None, need_format: bool = True) -> None:
        """输出日志"""
        if need_format:
            func_name = sys._getframe(1).f_code.co_name
            class_name = algo.__class__.__name__ if algo else self.__class__.__name__
            formatted_msg = f"[{class_name}.{func_name}] {msg}"
        else:
            formatted_msg: str = msg

        log: LogData = LogData(msg=formatted_msg, gateway_name=APP_NAME)
        event: Event = Event(EVENT_ALGO_LOG, data=log)
        self.event_engine.put(event)

    def put_algo_event(self, algo: AlgoTemplate, data: dict) -> None:
        """推送更新"""
        # 移除运行结束的算法实例
        if (
            algo in self.algos.values()
            and is_finished(algo.status)
        ):
            self.algos.pop(algo.todo_id)

            # 从symbol_algo_map中移除算法实例
            for vt_symbol, algos in self.symbol_algo_map.items():
                if algo in algos:
                    algos.remove(algo)
                    # 如果该交易对没有其他算法在运行,则退订行情
                    if not algos:
                        contract = self.main_engine.get_contract(vt_symbol)
                        if contract:
                            self.unsubscribe(contract.symbol, contract.exchange, contract.gateway_name)
                            self.write_log(f"退订行情: {vt_symbol}")

        event: Event = Event(EVENT_ALGO_UPDATE, data)
        self.event_engine.put(event)

    def get_holding(self, vt_symbol: str) -> float:
        """获取指定合约的持仓数量"""
        return self.position_manager.get_position(vt_symbol)

    def generate_test_order(self) -> None:
        """生成测试订单"""
        # 随机选择交易对
        vt_symbol = random.choice(self.TEST_SYMBOLS)

        # 随机选择方向
        direction = random.choice(["多", "空"])

        # 获取当前市场价格
        tick = self.main_engine.get_tick(vt_symbol)
        if not tick or not tick.ask_price_1 or not tick.bid_price_1:
            return

        # 随机生成数量
        notional = random.uniform(*self.TEST_NOTIONAL_RANGE)
        volume = notional / self.TEST_PRICE_DIVIDER

        # 获取合约信息并处理精度
        contract = self.main_engine.get_contract(vt_symbol)
        if not contract:
            return
        volume = round_to(volume, contract.min_volume)
        if volume == 0:
            return

        # 根据方向设置价格
        price = tick.ask_price_1 if direction == "多" else tick.bid_price_1

        # 创建新的Todo记录
        todo = Todo(
            content=f"{vt_symbol}_test",
            vt_symbol=vt_symbol,
            direction=direction,
            offset="开",  # 固定为开仓
            price=price,
            signal_volume=volume,
            real_volume=volume,
            level=1,
            ref=0,
            user="test",
            completed=10,  # 新创建状态
            datetime=datetime.now(),
            create_date=datetime.now(),
            create_by="system",
            remarks="测试订单",
            orderid="",
            kuo1="",
            kuo2=""
        )
        todo.save()

        self.write_log(
            f"生成测试订单[todo_id:{todo.id}]: {vt_symbol}, 方向: {direction}, "
            f"价格: {price}, 数量: {volume}, "
        )