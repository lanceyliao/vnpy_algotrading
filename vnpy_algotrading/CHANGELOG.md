# 算法交易引擎更新日志

## 新增VolumeFollowSyncAlgo算法
1. 订单管理机制升级
   - 支持多个订单并发执行和跟踪
   - 引入pending_orders字典替代pending_orderids集合，支持更完整的订单信息追踪
   - 新增order_time_map记录订单发出时间
   - 优化order_cancel_time处理撤单超时逻辑

2. 订单完成检查机制
   - 新增check_all_finished方法，全面检查算法执行完成条件
   - 综合检查活跃订单、未收到回报订单、撤单超时和预期交易量
   - 在on_trade、on_order和on_timer中统一使用完成检查逻辑

3. 订单超时和价格管理
   - 新增max_order_wait参数控制订单最大等待时间
   - 增强check_and_cancel_orders逻辑，统一处理活跃订单和pending订单
   - 优化价格合理性检查，支持实时撤销不合理价格的订单

4. 成交量计算优化
   - 改进get_total_pending方法，考虑订单方向计算待成交量
   - 优化get_remaining_volume逻辑，正确处理正负方向的订单量