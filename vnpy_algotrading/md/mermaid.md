
```mermaid
flowchart TB
    %% 子图样式定义
    classDef subgraphStyle fill:#f9f9f9,stroke:#666,stroke-width:2px,rx:5
    classDef dataFlow fill:#e8f4ff,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    classDef storeStyle fill:#ffe8e8,stroke:#666,stroke-width:2px,rx:10
    
    subgraph 行情事件驱动["行情事件驱动 (on_tick执行频率:实时)"]
        A1[TICK行情数据] --> A2["TICK基础检查<br>1.检查时间戳<br>2.检查price/volume"]
        A2 --> A3["首次行情初始化<br>1.tick.datetime记录时间戳<br>2.tick时间去秒记录分钟开始<br>3.tick.volume记录首末笔成交量<br>4.minute_first_ticker_time记录"] & A4["更新时间戳<br>1.去除时区信息<br>2.last_tick_time=now"]
        A4 --> A5{"分钟切换检查<br>new_minute > current_minute"}
        A5 -->|新分钟| A6["K线数据修正<br>1.分钟差=1且有成交<br>2.首tick在前5秒<br>3.用K线校正成交量<br>4.更新首笔统计"]
        A5 -->|同分钟| A7["计算成交量变化<br>1.当前volume - last_volume<br>2.更新minute_last_volume"]
        A6 & A7 --> A8["累计未分配量<br>1.检查变化量>0<br>2.unallocated_volume累加<br>3.记录本次变化时间"]
        A8 --> A9["计算实时速度<br>1.当前与上次间隔>0.5秒<br>2.速度=未分配量/时间差<br>3.更新volume_speed"]
    end

    subgraph 定时器事件驱动["定时器事件驱动 (on_timer间隔:0.5秒)"]
        B1["定时器触发"] --> B2["行情超时检查<br>1.now - last_tick_time<br>2.>5秒发送告警<br>3.停止算法"]
        B2 --> B3["基础数据检查<br>1.价格!=0<br>2.合约信息存在<br>3.最小名义价值>0"]
        B3 --> B4["计算发单参数<br>1.最小值=原值*倍数/比例<br>2.最小量=最小值/价格<br>3.建议量=速度*0.5秒"]
        B4 --> B5["确定发单量<br>1.未分配<=最小则全发<br>2.否则取较大值<br>3.不超未分配量"]
        B5 --> B6["启动订单管理"]
        B6 --> B7["撤单处理"] & B8["智能发单"]
        B7 --> B9["撤单超时处理<br>1.撤单>2.5秒无回报<br>2.加入黑洞字典<br>3.发送电话告警"] & B10["价格不合理撤单<br>1.买入价<卖一价<br>2.卖出价>买一价<br>3.记录撤单时间"]
        B8 --> B11["发送新订单<br>1.volume>0且未冷却<br>2.量=本次*跟量比例<br>3.价格含超价比例<br>4.智能合并剩余小单"]
    end

    subgraph 订单回报事件驱动["订单回报事件驱动 (on_order执行频率:实时)"]
        C1["状态变化接收"] --> C2["更新订单记录<br>1.pending_orders删除<br>2.active_orders更新<br>3.time_map清理"]
        C2 --> C3["检查订单完成<br>1.是否不再活跃<br>2.检查总目标量"]
        C3 --> C4["处理订单拒绝<br>1.解析拒单原因<br>2.获取错误代码<br>3.创建数据库记录"]
        C4 --> C5["过载-1008<br>无限重试"] & C6["超限-2027<br>1.最多5次<br>2.电话告警<br>3.数据库记录"] & C7["其他错误<br>1.最多5次<br>2.仅存数据库"]
    end

    subgraph 成交回报事件驱动["成交回报事件驱动 (on_trade执行频率:实时)"]
        D1["接收成交数据"] --> D2["处理冷却状态<br>1.can_send=True<br>2.error_counts=5"]
        D2 --> D3["更新成交统计<br>1.记录订单/价/量<br>2.累加traded值<br>3.计算成交均价"]
        D3 --> D4["检查算法完成<br>1.无活跃订单<br>2.无待回报单<br>3.总量已满足"]
    end

    subgraph 智能发单处理["智能发单处理模块 (函数:trade)"]
        E1["基础参数检查<br>1.volume_left>0<br>2.can_send=True<br>3.volume>min"] --> E2["计算委托价<br>1.买入=price*(1+2%)<br>2.卖出=price*(1-2%)<br>3.限制涨跌停"]
        E2 --> E3{"名义价值检查<br>price\*volume>=min\*2"}
        E3 -->|满足| E4["开仓委托<br>1.检查剩余价值<br>2.小单则合并<br>3.发送限价单"]
        E3 -->|不满足| E5["平仓委托<br>1.同价格委托<br>2.reduce=True<br>3.发送平仓单"]
    end

    subgraph 状态完成检查["状态完成检查模块 (函数:check_finished)"]
        F1["开始状态检查"] --> F2{"活动订单检查<br>active_orders={}"}
        F2 --> F3{"待回报单检查<br>pending={}"}
        F3 --> F4{"成交量检查<br>traded+黑洞=目标"}
        F4 --> F5["执行完成处理<br>1.状态=FINISHED<br>2.清理资源<br>3.发送事件"]
    end

    %% 核心数据存储
    S1["成交量追踪<br>volume_speed<br>unallocated_volume"]:::storeStyle
    S2["订单字典<br>active_orders<br>pending_orders<br>black_hole_orders"]:::storeStyle
    S3["时间记录<br>last_tick_time<br>order_time_map"]:::storeStyle
    S4["状态管理<br>status=RUNNING<br>can_send=True"]:::storeStyle

    %% 数据流转关系
    A9 --"成交速度"--> S1
    A8 --"未分配量"--> S1
    B11 --"发单触发"--> E1
    C2 --"订单状态"--> S2
    B9 --"超时订单"--> S2
    D2 --"更新状态"--> S4
    A4 --"时间更新"--> S3
    B10 --"撤单时间"--> S3
    
    %% 核心数据流
    S1 --"速度数据"--> B4
    S2 --"订单记录"--> F2
    S2 --"订单记录"--> F3
    S4 --"状态检查"--> F4