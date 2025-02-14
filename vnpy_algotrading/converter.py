from typing import Dict, TYPE_CHECKING
import sys
from vnpy.trader.object import PositionData, TradeData
from vnpy.trader.constant import Direction
from vnpy.event import Event, EventEngine
from vnpy.trader.event import EVENT_POSITION, EVENT_TRADE

if TYPE_CHECKING:
    from .engine import AlgoEngine

class PositionManager:
    """仓位管理器"""

    def __init__(self, event_engine: EventEngine, algo_engine: "AlgoEngine") -> None:
        """构造函数"""
        self.event_engine: EventEngine = event_engine
        self.algo_engine = algo_engine
        self.positions: Dict[str, float] = {}  # vt_symbol: volume
        
        self.register_event()
        
    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        
    def process_position_event(self, event: Event) -> None:
        """处理持仓事件"""
        position: PositionData = event.data
        
        # 由于Binance的持仓是NET模式，volume可正可负
        old_volume = self.positions.get(position.vt_symbol, 0.0)
        self.positions[position.vt_symbol] = position.volume
        
        self.algo_engine.write_log(
            f"持仓更新: {position.vt_symbol}, 数量: {old_volume} -> {position.volume}"
        )
        
    def get_position(self, vt_symbol: str) -> float:
        """获取指定合约的持仓数量"""
        return self.positions.get(vt_symbol, 0.0)

    def write_log(self, msg: str) -> None:
        """输出日志"""
        func_name = sys._getframe(1).f_code.co_name
        class_name = self.__class__.__name__
        formatted_msg = f"[{class_name}.{func_name}] {msg}"
        self.algo_engine.write_log(formatted_msg, need_format=False)
        