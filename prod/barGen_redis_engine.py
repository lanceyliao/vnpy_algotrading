""""""
import traceback
from copy import copy
from threading import Thread
from queue import Queue, Empty
from time import sleep
from typing import Callable, Dict, Optional
from datetime import datetime,time
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.object import (
    SubscribeRequest,
    TickData,
    BarData,
    ContractData
)
# import redis
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_TIMER
from vnpy.trader.utility import load_json, save_json 
from vnpy.trader.object import BarData, TickData, Interval
from vnpy.trader.constant import (
    Exchange
)
from vnpy.trader.setting import SETTINGS
APP_NAME = "BarGenEngine"
from .event import EVENT_BAR, EVENT_MINIBAR, EVENT_BAR_RECORD
from vnpy.trader.database import DB_TZ
# from vnpy.usertools.db_status_manager import Status
from peewee import fn
from collections import deque

# 定时重载 setting.json
class BarGenEngine(BaseEngine):
    """"""
    # setting_filename = "barGen_redis_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.queue = Queue()
        self.thread = Thread(target=self.run)
        self.active = False

        # self.bar_recordings = []
        self.bar_recordings = set()
        # self.bar_generators = {}
        
        self.bars: Dict[str, BarData] = {}
        self.last_ticks: Dict[str, TickData] = {}
        #self.last_dt: datetime = None
        self.last_dts: Dict[str, datetime] = {}

        self.tick_time: Dict[str, str] = {} # cache tick time for debug info
        # 批量订阅相关
        self.pending_symbols = set()      # 待订阅集合
        self.batch_size = 66              # 每批订阅数量
        self.subscribe_interval = 0.3  # 每批订阅的间隔时间(秒)
        self.last_subscribe_time = None  # 上次订阅的时间

        self.timer_count = 0
        self.timer_interval = 60
        # create connection
        # self.r = redis.Redis(host=SETTINGS["redis.host"], port=SETTINGS["redis.port"], password=SETTINGS["redis.password"])

        # self.load_setting()

        self.register_event()
        # self.start()  # need test vigar 1216 !
        self.update_subscriptions()
        self.put_event()

    def run(self):
        """"""
        while self.active:
            try:
                if datetime.now().minute%30==0 and datetime.now().second==0:
                    pass
            except Exception:
                msg = f"barGen run 触发异常已停止\n{traceback.format_exc()}"
                self.write_log(f"barGen error: {msg}")

    def close(self):
        """"""
        self.active = False
        if self.thread.is_alive():
            self.thread.join()

    def start(self):
        """"""
        self.active = True
        self.thread.start()

    def subscribe_recording(self, vt_symbol: str):
        """"""
        try:
            contract = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return
            # self.write_log(f"prepare to send subscribe req：{vt_symbol}")
            self.subscribe(contract)
        except Exception:
            msg = f"barGen error 触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg)
            return
        # self.write_log(f"添加K线记录成功：{vt_symbol}")

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_BAR, self.process_bar_event)

    # def update_tick(self, tick: TickData):
    #     """"""
    #     if tick.vt_symbol in self.bar_recordings:
    #         bg = self.get_bar_generator(tick.vt_symbol)
    #         bg.update_tick(copy(tick))
    
    # no need to filter datetime for binance
    def future_tick_filter(self, tick):
        ret = False
        return ret
    
    # def update_redis(self, tick: TickData):
    #     data = {
    #         "price": tick.last_price,
    #         "limit_up": tick.limit_up,
    #         "limit_down": tick.limit_down,
    #         "volume": tick.volume,
    #         "open_interest": tick.open_interest,
    #         "datetime": tick.datetime.strftime('%Y%m%d%H%M%S.%f')
    #     }
    #     vt_symbol = f"{tick.symbol}.{tick.exchange.value}"
    #     for key, value in data.items():
    #         self.r.hset(vt_symbol, key, value)
        
    
    def update_tick(self, tick: TickData):
        if not tick.last_price:
            return
        
        # self.update_redis(tick)
        
        # Update tick cache
        self.last_ticks[tick.vt_symbol] = tick
        self.last_dts[tick.vt_symbol] = tick.datetime

    def process_timer_event(self, event: Event):
        """"""
        # 每5秒执行一次订阅检查
        if self.timer_count % 5 == 0:
            self.update_subscriptions()

            # 批量订阅所有pending symbols
            if self.pending_symbols:
                current_batch = []
                
                for vt_symbol in list(self.pending_symbols):  # 转换为list以便移除元素
                    try:
                        contract = self.main_engine.get_contract(vt_symbol)
                        if not contract:
                            self.write_log(f"找不到合约：{vt_symbol}")
                            continue
                        
                        req = SubscribeRequest(
                            symbol=contract.symbol,
                            exchange=contract.exchange
                        )
                        current_batch.append(req)
                        
                        # 当达到批次大小时，发送订阅请求
                        if len(current_batch) >= self.batch_size:
                            gateway_name = contract.gateway_name
                            gateway = self.main_engine.get_gateway(gateway_name)
                            gateway.subscribe_reqs(current_batch)
                            
                            # 从pending中移除已订阅的symbols
                            for req in current_batch:
                                vt_symbol = f"{req.symbol}.{req.exchange.value}"
                                self.pending_symbols.remove(vt_symbol)
                            
                            current_batch = []  # 清空当前批次
                            sleep(self.subscribe_interval)  # 等待一段时间再发送下一批
                    
                    except Exception:
                        msg = f"创建订阅请求异常: {vt_symbol}\n{traceback.format_exc()}"
                        self.write_log(msg)
                        continue

                # 处理剩余的symbols
                if current_batch:
                    gateway_name = contract.gateway_name
                    gateway = self.main_engine.get_gateway(gateway_name)
                    gateway.subscribe_reqs(current_batch)
                    
                    for req in current_batch:
                        vt_symbol = f"{req.symbol}.{req.exchange.value}"
                        self.pending_symbols.remove(vt_symbol)

        self.timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        
        tick_time_str = self.tick_time_info()
        # self.write_log(f"process_timer_event: {tick_time_str}")
        self.timer_count = 0
                    
    def process_tick_event(self, event: Event):
        """"""
        # update cache
        tick = event.data
        time_str = tick.datetime.strftime('%Y%m%d%H%M%S')
        self.tick_time[tick.symbol] = time_str
        self.update_tick(tick)
        
    def tick_time_info(self):
        ret = "{"
        for k,v in self.tick_time.items():
            info = f"{k} {v}\n"
            ret = ret + info
        ret = ret+"}"
        return ret

    def process_contract_event(self, event: Event):
        """"""
        contract = event.data
        vt_symbol = contract.vt_symbol

        if vt_symbol in self.bar_recordings:
            self.pending_symbols.add(vt_symbol)  # 使用add而不是append

    def write_log(self, msg: str):
        """"""
        self.main_engine.write_log(msg)

    def put_event(self):
        """"""
        pass

    # 处理bar,供其它应用处理
    def record_bar(self, bar: BarData):
        """"""
        try:
            time_str = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            self.write_log(f" ======1======record bar memory: {time_str}: {bar.vt_symbol} {bar.datetime} o:{bar.open_price} h:{bar.high_price} l:{bar.low_price} c:{bar.close_price}")
            # self.write_log(f" ======2========put to event_engine: {bar}")
            # event = Event(EVENT_BAR, bar)
            event2 = Event(EVENT_BAR_RECORD, bar)
            # self.event_engine.put(event)
            self.event_engine.put(event2)
        except Exception:
            msg = f"record_bar 触发异常已停止\n{traceback.format_exc()}"
            self.write_log(msg)

    def subscribe(self, contract: ContractData):
        """"""
        req = SubscribeRequest(
            symbol=contract.symbol,
            exchange=contract.exchange
        )
        # self.write_log(f"send subscribe req {contract.symbol}")
        self.main_engine.subscribe(req, contract.gateway_name)


    def update_subscriptions(self):
        """从status表获取vt_symbol，更新bar_recordings集合"""
        # statuses = Status.select(fn.SUBSTRING_INDEX(Status.content, '_', 1).distinct().alias('vt_symbol'))
        # for status in statuses:
        #     if status.vt_symbol not in self.bar_recordings:
        #         self.subscribe_recording(status.vt_symbol)
        #         self.bar_recordings.add(status.vt_symbol)
        # statuses = "1000BONKUSDT.BINANCE","1000FLOKIUSDT.BINANCE","1000LUNCUSDT.BINANCE","1000PEPEUSDT.BINANCE","1000RATSUSDT.BINANCE","1000SATSUSDT.BINANCE","1000SHIBUSDT.BINANCE","1INCHUSDT.BINANCE","AAVEUSDT.BINANCE","ACEUSDT.BINANCE","AEVOUSDT.BINANCE","AIUSDT.BINANCE","ALGOUSDT.BINANCE","ALICEUSDT.BINANCE","ALTUSDT.BINANCE","AMBUSDT.BINANCE","ANKRUSDT.BINANCE","APEUSDT.BINANCE","API3USDT.BINANCE","APTUSDT.BINANCE","ARBUSDT.BINANCE","ARKMUSDT.BINANCE","ARKUSDT.BINANCE","ARPAUSDT.BINANCE","ARUSDT.BINANCE","ASTRUSDT.BINANCE","ATOMUSDT.BINANCE","AUCTIONUSDT.BINANCE","AVAXUSDT.BINANCE","AXLUSDT.BINANCE","AXSUSDT.BINANCE","BAKEUSDT.BINANCE","BALUSDT.BINANCE","BATUSDT.BINANCE","BCHUSDT.BINANCE","BEAMXUSDT.BINANCE","BELUSDT.BINANCE","BIGTIMEUSDT.BINANCE","BLURUSDT.BINANCE","BLZUSDT.BINANCE","BNXUSDT.BINANCE","BOMEUSDT.BINANCE","BONDUSDT.BINANCE","BSVUSDT.BINANCE","C98USDT.BINANCE","CELOUSDT.BINANCE","CFXUSDT.BINANCE","CHRUSDT.BINANCE","CHZUSDT.BINANCE","CKBUSDT.BINANCE","COTIUSDT.BINANCE","CRVUSDT.BINANCE","CYBERUSDT.BINANCE","DARUSDT.BINANCE","DENTUSDT.BINANCE","DOGEUSDT.BINANCE","DUSKUSDT.BINANCE","DYDXUSDT.BINANCE","DYMUSDT.BINANCE","EDUUSDT.BINANCE","EGLDUSDT.BINANCE","ENJUSDT.BINANCE","ENSUSDT.BINANCE","ETHFIUSDT.BINANCE","ETHUSDT.BINANCE","FETUSDT.BINANCE","FILUSDT.BINANCE","FLMUSDT.BINANCE","FTMUSDT.BINANCE","FXSUSDT.BINANCE","GALAUSDT.BINANCE","GASUSDT.BINANCE","GLMUSDT.BINANCE","GMTUSDT.BINANCE","GMXUSDT.BINANCE","GRTUSDT.BINANCE","GTCUSDT.BINANCE","HBARUSDT.BINANCE","HIFIUSDT.BINANCE","HIGHUSDT.BINANCE","HOOKUSDT.BINANCE","HOTUSDT.BINANCE","ICPUSDT.BINANCE","IDUSDT.BINANCE","ILVUSDT.BINANCE","IMXUSDT.BINANCE","INJUSDT.BINANCE","IOTAUSDT.BINANCE","IOTXUSDT.BINANCE","JASMYUSDT.BINANCE","JTOUSDT.BINANCE","JUPUSDT.BINANCE","KASUSDT.BINANCE","KAVAUSDT.BINANCE","KEYUSDT.BINANCE","KNCUSDT.BINANCE","KSMUSDT.BINANCE","LDOUSDT.BINANCE","LEVERUSDT.BINANCE","LINAUSDT.BINANCE","LINKUSDT.BINANCE","LITUSDT.BINANCE","LOOMUSDT.BINANCE","LPTUSDT.BINANCE","LQTYUSDT.BINANCE","LRCUSDT.BINANCE","LUNA2USDT.BINANCE","MAGICUSDT.BINANCE","MANAUSDT.BINANCE","MANTAUSDT.BINANCE","MASKUSDT.BINANCE","MAVIAUSDT.BINANCE","MEMEUSDT.BINANCE","MINAUSDT.BINANCE","MKRUSDT.BINANCE","MTLUSDT.BINANCE","MYROUSDT.BINANCE","NEARUSDT.BINANCE","NEOUSDT.BINANCE","NFPUSDT.BINANCE","NMRUSDT.BINANCE","OMGUSDT.BINANCE","OMUSDT.BINANCE","ONDOUSDT.BINANCE","ONEUSDT.BINANCE","ONTUSDT.BINANCE","OPUSDT.BINANCE","ORDIUSDT.BINANCE","PENDLEUSDT.BINANCE","PEOPLEUSDT.BINANCE","PHBUSDT.BINANCE","PIXELUSDT.BINANCE","POLYXUSDT.BINANCE","PORTALUSDT.BINANCE","POWRUSDT.BINANCE","PYTHUSDT.BINANCE","RDNTUSDT.BINANCE","REEFUSDT.BINANCE","RONINUSDT.BINANCE","ROSEUSDT.BINANCE","RSRUSDT.BINANCE","RUNEUSDT.BINANCE","RVNUSDT.BINANCE","SANDUSDT.BINANCE","SEIUSDT.BINANCE","SFPUSDT.BINANCE","SKLUSDT.BINANCE","SNXUSDT.BINANCE","SOLUSDT.BINANCE","SSVUSDT.BINANCE","STGUSDT.BINANCE","STMXUSDT.BINANCE","STORJUSDT.BINANCE","STRKUSDT.BINANCE","STXUSDT.BINANCE","SUIUSDT.BINANCE","SUPERUSDT.BINANCE","SUSHIUSDT.BINANCE","SXPUSDT.BINANCE","THETAUSDT.BINANCE","TIAUSDT.BINANCE","TLMUSDT.BINANCE","TOKENUSDT.BINANCE","TONUSDT.BINANCE","TRBUSDT.BINANCE","TRUUSDT.BINANCE","TRXUSDT.BINANCE","UMAUSDT.BINANCE","UNIUSDT.BINANCE","VANRYUSDT.BINANCE","WIFUSDT.BINANCE","WLDUSDT.BINANCE","WOOUSDT.BINANCE","XAIUSDT.BINANCE","XMRUSDT.BINANCE","XRPUSDT.BINANCE","YGGUSDT.BINANCE","ZECUSDT.BINANCE","ZENUSDT.BINANCE","ZETAUSDT.BINANCE","ZILUSDT.BINANCE","ZRXUSDT.BINANCE"
        status_testnet = "BTCUSDT.BINANCE", "ETHUSDT.BINANCE", "IPUSDT.BINANCE" , "OMUSDT.BINANCE"
        # status_testnet = "BTCUSDT.BINANCE", "XLMUSDT.BINANCE"
        # status_testnet = "ANKRUSDT.BINANCE",

        # for vt_symbol in statuses:
        for vt_symbol in status_testnet:
            if vt_symbol not in self.bar_recordings:
                self.bar_recordings.add(vt_symbol)

    def process_bar_event(self, event: Event):
        """处理bar事件"""
        bar = event.data
        self.record_bar(bar)
