from typing import Dict
from vnpy.trader.object import PositionData, TradeData
from vnpy.trader.constant import Direction
from vnpy.event import Event, EventEngine
from vnpy.trader.event import EVENT_POSITION, EVENT_TRADE


class PositionManager:
    """仓位管理器"""

    def __init__(self, event_engine: EventEngine) -> None:
        """构造函数"""
        self.event_engine: EventEngine = event_engine
        self.positions: Dict[str, float] = {}  # vt_symbol: volume
        
        self.register_event()
        
    def register_event(self) -> None:
        """注册事件监听"""
        self.event_engine.register(EVENT_POSITION, self.process_position_event)
        self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        
    def process_position_event(self, event: Event) -> None:
        """处理持仓事件"""
        position: PositionData = event.data
        
        # 由于Binance的持仓是NET模式，volume可正可负
        self.positions[position.vt_symbol] = position.volume
        
    def process_trade_event(self, event: Event) -> None:
        """处理成交事件"""
        trade: TradeData = event.data
        
        # 如果没有该合约的持仓记录，初始化为0
        if trade.vt_symbol not in self.positions:
            self.positions[trade.vt_symbol] = 0.0
            
        # 根据成交方向更新持仓
        volume_change = trade.volume
        if trade.direction == Direction.SHORT:
            volume_change = -volume_change
            
        self.positions[trade.vt_symbol] += volume_change
        
    def get_position(self, vt_symbol: str) -> float:
        """获取指定合约的持仓数量"""
        return self.positions.get(vt_symbol, 0.0) 