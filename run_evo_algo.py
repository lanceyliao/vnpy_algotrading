from vnpy_evo.event import EventEngine
from vnpy_evo.trader.engine import MainEngine
from vnpy_evo.trader.utility import load_json
import time
import signal
import sys

from vnpy_algotrading.base import EVENT_ALGO_LOG
from prod.recorder_engine import RecorderEngine

# 全局变量
TEST_ENABLED = True

class AlgoApp:
    def __init__(self):
        self.event_engine = EventEngine(interval=0.5)
        self.main_engine = MainEngine(self.event_engine)

        self.gateway = None
        self.algo_engine = None
        self.init_engines()

        self._active = True  # 添加运行状态标记

        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 在Windows平台上signal模块可能不完全支持
        if sys.platform != 'win32':
            # 忽略SIGTSTP信号(Ctrl+Z)，避免进程被挂起
            signal.signal(signal.SIGTSTP, signal.SIG_IGN)

    def _signal_handler(self, signum, frame):
        """
        信号处理函数
        """
        sig_name = signal.Signals(signum).name
        self.main_engine.write_log(f"收到信号: {sig_name}，准备关闭应用...")
        self._active = False

    def init_engines(self):
        # 添加币安网关
        from prod.binance_linear_gateway import BinanceLinearGateway
        self.gateway = self.main_engine.add_gateway(BinanceLinearGateway)
        setting = load_json("connect_binance_testnet.json")
        self.main_engine.connect(setting, "BINANCE_LINEAR")
        self.main_engine.write_log("接口添加成功")

        # 添加K线生成引擎
        from prod.barGen_redis_engine import BarGenEngine
        bar_gen_engine = self.main_engine.add_engine(BarGenEngine)
        bar_gen_engine.start()
        self.main_engine.write_log("添加Bar生成引擎")
        # 添加K线记录引擎
        self.recorder_engine = self.main_engine.add_engine(RecorderEngine)

        # 记录算法引擎日志
        log_engine = self.main_engine.get_engine('log')
        self.event_engine.register(EVENT_ALGO_LOG, log_engine.process_log_event)
        
        # 添加算法交易引擎
        from vnpy_algotrading import AlgoTradingApp
        self.algo_engine = self.main_engine.add_app(AlgoTradingApp)
        self.algo_engine.start(TEST_ENABLED, allow_multiple_algos=True)  # 启动算法交易引擎，传入测试标志
        self.main_engine.write_log("算法交易引擎创建成功")

    def run(self):
        """运行应用"""
        self.main_engine.write_log("算法交易应用已启动")
        while self._active:
            time.sleep(1)
        self.main_engine.write_log("应用正在关闭...")
        self.close()

    def close(self):
        """关闭应用"""
        if self.algo_engine:
            self.algo_engine.close()

        if self.gateway:
            self.gateway.cancel_all()

        time.sleep(2)

        self.main_engine.close()

def main():
    """主函数"""

    app = AlgoApp()
    app.run()

if __name__ == "__main__":
    # 默认不启用测试订单生成
    main()