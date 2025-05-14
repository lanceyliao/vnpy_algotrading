
```mermaid
flowchart TB
    %% 样式定义
    classDef subgraphStyle fill:#f9f9f9,stroke:#666,stroke-width:2px,rx:5
    classDef dataFlow fill:#e8f4ff,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    classDef storeStyle fill:#ffe8e8,stroke:#666,stroke-width:2px,rx:10
    classDef alertStyle fill:#ffece8,stroke:#666,stroke-width:2px,rx:5
    classDef calcStyle fill:#f0ffe8,stroke:#666,stroke-width:2px,rx:5
    
    subgraph 全局数据存储["全局数据管理 (VolumeAlgo类属性)"]
        S1["成交量统计<br>minute_first_volume: float = 0<br>minute_last_volume: float = 0<br>unallocated_volume: float = 0<br>volume_speed: float = 0<br>traded_volume: float = 0"]:::storeStyle
        S2["订单管理字典<br>active_orders: dict = {}<br>pending_orders: dict = {}<br>black_hole_orders: dict = {}<br>order_cancel_time: dict = {}"]:::storeStyle
        S3["时间记录<br>last_tick_time: datetime = None<br>order_time_map: dict = {}<br>current_minute: datetime = None<br>last_volume_time: datetime = None"]:::storeStyle
        S4["状态控制<br>status: Enum = RUNNING<br>can_send_order: bool = True<br>error_counts: dict = {}<br>volume_target: float = 0"]:::storeStyle
    end

    subgraph 计算参数管理["算法参数配置"]
        C1["成交量参数"]:::calcStyle --> C2["跟量比例: float = 0.2<br>最小名义价值倍数: float = 2.0"]
        C3["时间参数"]:::calcStyle --> C4["定时器间隔: 0.5秒<br>订单等待时间: 2.0秒<br>撤单超时时间: 2.5秒<br>行情超时时间: 5.0秒"]
        C5["价格参数"]:::calcStyle --> C6["超价比例: float = 2.0<br>涨跌停检查: tick_size"]
    end

    subgraph 行情事件驱动["行情事件驱动 (on_tick)"]
        A1[TICK数据] --> A2["数据校验<br>1.检查datetime有效<br>2.检查price>0<br>3.检查volume>=0"]
        A2 --> A3["首次行情<br>1.记录first_volume<br>2.记录last_volume<br>3.设置current_minute"] & A4["更新时间<br>1.去除时区信息<br>2.更新last_tick_time"]
        A4 --> A5{"分钟切换检查<br>tick.minute>current"}
        A5 -->|新分钟| A6["K线修正<br>1.diff==1检查<br>2.首tick<5秒检查<br>3.用K线量校正tick量"]
        A5 -->|同分钟| A7["计算变化量<br>diff=new-last"]
        A6 & A7 --> A8["累计未分配<br>1.检查diff>0<br>2.记录变化时间<br>3.累加未分配量"]
        A8 --> A9["计算速度<br>间隔>0.5秒时<br>speed=volume/interval"]
    end

    subgraph 定时器事件驱动["定时器事件驱动 (on_timer)"]
        B1[0.5秒定时器] --> B2{"行情检查<br>now-last_tick>5s"}
        B2 -->|超时| B3["发送告警并停止"]
        B2 -->|正常| B4["参数计算<br>1.min_value=base*mult/ratio<br>2.min_vol=value/price<br>3.suggest=speed*0.5"]
        B4 --> B5["确定发单量<br>取建议量与最小量较大值"]
        B5 --> B6["订单管理"]
        B6 --> B7["检查撤单"] & B8["执行发单"]
        B7 --> B9["超时撤单<br>1.检查>2.5秒<br>2.加入黑洞<br>3.发送告警"] & B10["价格撤单<br>1.检查买卖价<br>2.记录撤单时"]
        B8 --> B11["智能发单<br>1.计算实际量<br>2.应用超价率<br>3.合并剩余量"]
    end

    subgraph 订单管理["订单状态管理"]
        O1["订单回报"] --> O2["更新字典<br>1.移出pending<br>2.更新active<br>3.清理time_map"]
        O2 --> O3{"检查状态"}
        O3 -->|"完成"| O4["清理资源"]
        O3 -->|"拒绝"| O5["错误处理"]
        O5 --> O6["1008无限重试"] & O7["2027最多5次<br>发送告警"] & O8["其他最多5次"]
    end

    subgraph 成交管理["成交处理"]
        F1["成交回报"] --> F2["解除冷却<br>重置错误计数"]
        F2 --> F3["更新统计<br>1.累加成交量<br>2.计算均价"]
        F3 --> F4["检查完成<br>traded+黑洞=目标"]
    end

    %% 关键数据流
    A9 --"更新速度"--> S1
    A8 --"更新未分配量"--> S1
    A4 --"更新时间"--> S3
    B11 --"更新订单"--> S2
    B9 --"更新黑洞"--> S2
    O2 --"更新状态"--> S4
    F3 --"更新成交"--> S1

    %% 检查流
    S1 --"读取速度"--> B4
    S2 --"检查订单"--> B7
    S3 --"检查超时"--> B2
    S4 --"检查状态"--> B8

    %% 参数应用
    C2 --"应用参数"--> B4
    C4 --"应用时间"--> B9
    C6 --"应用价格"--> B11

    %% 应用样式
    class 全局数据存储,计算参数管理,行情事件驱动,定时器事件驱动,订单管理,成交管理 subgraphStyle
