""""""
import re
import sys, traceback
from threading import Thread
from queue import Queue, Empty
from typing import Any, Dict, List, Optional
from collections import defaultdict
from datetime import datetime, time
from zoneinfo import ZoneInfo
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import BarData, BaseData
from vnpy.trader.event import EVENT_TIMER#, EVENT_BAR_AGG
from vnpy.trader.database import get_database, convert_tz
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.utility import save_json, load_json, virtual
from peewee import CharField, DateTimeField, DoubleField, Model, IntegerField
from dataclasses import dataclass
from peewee import chunked

APP_NAME = "DataRecorder"
EVENT_RECORDER_LOG = "eRecorderLog"
from .event import EVENT_BAR_RECORD, EVENT_BAR_AGG, EVENT_ORDER_ERROR_RECORD

# 数据库时区
DB_TZ = ZoneInfo("Asia/Shanghai")

# 获取数据库实例
database = get_database()

# 定义聚合K线数据表
class AggregatedBarData(Model):
    """聚合K线数据表"""
    symbol = CharField()
    exchange = CharField()
    interval = CharField()
    datetime = DateTimeField()
    volume = DoubleField()
    turnover = DoubleField()
    open_interest = DoubleField()
    open_price = DoubleField()
    high_price = DoubleField()
    low_price = DoubleField()
    close_price = DoubleField()

    class Meta:
        database = database.db
        indexes = (
            (('symbol', 'exchange', 'interval', 'datetime'), True),
        )

# 定义聚合K线汇总数据表
class AggregatedBarOverview(Model):
    """聚合K线汇总数据表"""
    symbol = CharField()
    exchange = CharField()
    interval = CharField()
    count = IntegerField()
    start = DateTimeField()
    end = DateTimeField()

    class Meta:
        database = database.db
        indexes = (
            (('symbol', 'exchange', 'interval'), True),
        )

class OrderError(Model):
    symbol = CharField()
    exchange = CharField()
    orderid = CharField()
    create_date = DateTimeField(default=datetime.now())
    error_code = IntegerField()
    error_msg = CharField()
    username = CharField(null=True)
    todo_id = CharField(null=True)
    fix_date = DateTimeField(null=True)
    remarks = CharField(null=True)
    ext1 = CharField(null=True)
    ext2 = CharField(null=True)

    class Meta:
        database = database.db
        indexes = ((('symbol', 'exchange', 'orderid', 'create_date', 'error_code'), True),)


@dataclass
class OrderErrorData(BaseData):
    symbol: str
    exchange: Exchange
    error_code: int
    error_msg: str
    orderid: str = None
    create_date: datetime = datetime.now()
    username: str = None
    todo_id: str = None
    fix_date: datetime = None
    remarks: str = None
    ext1: str = None
    ext2: str = None

# 确保表存在
database.db.create_tables([AggregatedBarData, AggregatedBarOverview, OrderError], safe=True)


class RecorderEngine(BaseEngine):
    """"""
    setting_filename = "data_recorder_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.queue: Queue = Queue()
        self.thread: Thread = Thread(target=self.run)
        self.active: bool = False

        self.timer_count: int = 0
        self.timer_interval: int = 60

        # 添加数据库检查计数器
        self.db_check_count: int = 0
        self.db_check_interval: int = 20  # 20次timer_interval,即20分钟

        self.bars: Dict[str, List[BarData]] = defaultdict(list)
        self.agg_bars: Dict[str, List[BarData]] = defaultdict(list)
        self.order_errors: List[OrderErrorData] = []
        self.database = database

        # self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_BAR_RECORD, self.process_bar_event)
        self.event_engine.register(EVENT_BAR_AGG, self.process_bar_agg_event)
        self.event_engine.register(EVENT_ORDER_ERROR_RECORD, self.process_order_error_record_event)

    def process_order_error_record_event(self, event: Event) -> None:
        """处理委托错误事件"""
        order_error: OrderErrorData = event.data
        self.order_errors.append(order_error)

    def process_bar_event(self, event: Event) -> None:
        self.record_bar(event.data)

    def process_bar_agg_event(self, event: Event) -> None:
        """处理聚合K线事件"""
        bar = event.data
        self.agg_bars[f"{bar.vt_symbol}_{bar.interval.value}"].append(bar)

    def process_timer_event(self, event: Event) -> None:
        """"""
        self.timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        self.timer_count = 0

        # 增加数据库连接检查计数
        self.db_check_count += 1
        if self.db_check_count >= self.db_check_interval:
            self.db_check_count = 0
            # 每20分钟检查一次数据库连接
            self.check_database_connection()

        # 处理普通K线
        self.write_log(f"record_bar engine info: {self.bars.keys()}")
        for bars in self.bars.values():
            self.queue.put(("bar", bars))
        self.bars.clear()

        # 处理聚合K线
        self.write_log(f"record_bar engine info: {self.agg_bars.keys()}")
        for bars in self.agg_bars.values():
            self.queue.put(("agg_bar", bars))
        self.agg_bars.clear()

        for error in self.order_errors:
            self.queue.put(("order_error", error))
        self.order_errors.clear()

    def run(self) -> None:
        """"""
        while self.active:
            try:
                task: Any = self.queue.get(timeout=1)
                task_type, data = task

                if task_type == "bar":
                    self.write_log(f"record_bar engine info: {data}")
                    self.database.save_bar_data(data)
                elif task_type == "agg_bar":
                    # 读取主键参数
                    bar: BarData = data[0]
                    symbol: str = bar.symbol
                    exchange: Exchange = bar.exchange
                    interval: Interval = bar.interval

                    # 批量保存聚合K线数据
                    agg_bars = []
                    for bar in data:
                        # 调整时区
                        bar_datetime = convert_tz(bar.datetime)

                        # 使用__dict__转换数据
                        d = bar.__dict__
                        d["exchange"] = d["exchange"].value
                        d["interval"] = d["interval"].value
                        d["datetime"] = bar_datetime

                        # 移除不需要的字段
                        d.pop("gateway_name", None)
                        d.pop("vt_symbol", None)
                        d.pop("open_interest_value", None)

                        agg_bars.append(d)

                    # 批量保存数据
                    with self.database.db.atomic():
                        for c in chunked(agg_bars, 50):  # 每50条数据一批
                            AggregatedBarData.insert_many(c).on_conflict_replace().execute()

                    # 更新汇总数据
                    overview: AggregatedBarOverview = AggregatedBarOverview.get_or_none(
                        AggregatedBarOverview.symbol == symbol,
                        AggregatedBarOverview.exchange == exchange.value,
                        AggregatedBarOverview.interval == interval.value
                        )

                    if not overview:
                        overview = AggregatedBarOverview()
                        overview.symbol = symbol
                        overview.exchange = exchange.value
                        overview.interval = interval.value
                        overview.start = data[0].datetime
                        overview.end = data[-1].datetime
                        overview.count = len(data)
                    else:
                        overview.start = min(data[0].datetime, overview.start)
                        overview.end = max(data[-1].datetime, overview.end)

                        s = AggregatedBarData.select().where(
                            (AggregatedBarData.symbol == symbol)
                            & (AggregatedBarData.exchange == exchange.value)
                            & (AggregatedBarData.interval == interval.value)
                        )
                        overview.count = s.count()

                    overview.save()
                elif task_type == "order_error":
                    error = data
                    # 使用__dict__转换数据
                    d = error.__dict__
                    d["exchange"] = d["exchange"].value

                    # 移除不需要的字段
                    d.pop("gateway_name", None)  # 因为BaseData中有这个字段
                    d.pop("vt_symbol", None)     # 如果有的话

                    OrderError.insert(d).on_conflict_replace().execute()

            except Empty:
                continue
            except Exception:
                self.active = False
                msg = f"record_engine 触发异常已停止\n{traceback.format_exc()}"
                self.write_log(msg)

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def close(self) -> None:
        """"""
        self.active = False
        if self.thread.is_alive():
            self.thread.join()

    def write_log(self, msg: str) -> None:
        """"""
        self.main_engine.write_log(msg)

    def put_event(self) -> None:
        """"""
        pass

    def load_setting(self) -> None:
        """"""
        pass

    @virtual
    def record_bar(self, bar: BarData) -> None:
        """"""
        self.bars[bar.vt_symbol].append(bar)

    def check_database_connection(self) -> None:
        """检查数据库连接并在必要时重连"""
        try:
            # 尝试执行一个简单的查询来测试连接
            self.database.get_bar_overview()
        except Exception as e:
            self.write_log(f"数据库连接已断开，正在尝试重连: {str(e)}")
            try:
                # 关闭现有连接
                self.database.db.close()
                # 重新连接
                self.database.db.connect()
                self.write_log("数据库重连成功")
            except Exception as e:
                self.write_log(f"数据库重连失败: {str(e)}")


class RecorderEngineCtp(RecorderEngine):
    def record_bar(self, bar: BarData) -> None:
        """"""
        bar.symbol = re.sub(r"""(\D*)(\d+)(.*)""", r"\g<1>888\g<3>", bar.symbol)
        super().record_bar(bar)
