"""
用户算法下单分发脚本
用于多组用户算法下单的任务分发，按照remarks中定义的组号进行分组，并在同一时间只处理每组的第一个订单
"""

import threading
import time
import random
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import json
import redis
from loguru import logger as _logger

from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    IntegerField,
    Model as ModelBase,
    fn
)

from vnpy.trader.setting import SETTINGS

from .database import Todo, AlgoOrder, db

# 定义全局logger
# 在模块级别配置日志，避免多个模块重复配置
# 其他模块可以使用 from vnpy_algotrading.dispatch_algo import logger 来复用这个配置
logger = _logger.bind(name="dispatch")

# 配置日志
logger.remove()  # 移除默认处理程序
logger.add(sys.stderr, level="TRACE", format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")  # 添加控制台输出
logger.add("dispatch.log", rotation="10 MB", retention="10 days", level="TRACE", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}")  # 添加文件输出

# 定义用户信息表
class UserInfo(ModelBase):
    """用户信息表"""
    id = AutoField()
    username = CharField()   # 用户名
    zhname = CharField()     # 中文名
    money = IntegerField()   # 资金
    remarks = CharField()    # 备注（包含分组信息）
    create_date = DateTimeField()  # 创建时间
    
    class Meta:
        database = db
        indexes = (
            (("username", "zhname"), True),  # 唯一索引
        )


class UserGroupManager:
    """用户分组管理类，负责用户分组相关的查询和管理"""
    
    def __init__(self) -> None:
        """初始化用户分组管理器"""
        # 确保表存在
        db.create_tables([UserInfo, AlgoOrder, Todo], safe=True)
        # 用户分组缓存
        self._user_groups: Dict[int, List[str]] = {}  # 组号 -> 用户列表
        self._user_to_group: Dict[str, int] = {}      # 用户名 -> 组号
        self._user_to_sequence: Dict[str, int] = {}   # 用户名 -> 序号
        # 加载用户分组信息
        self._load_user_groups()
    
    def _load_user_groups(self) -> None:
        """加载用户分组信息"""
        # 清空缓存
        self._user_groups = defaultdict(list)
        self._user_to_group = {}
        self._user_to_sequence = {}
        
        # 查询所有用户信息
        users = UserInfo.select()
        for user in users:
            # 解析remarks中的分组信息（3位数字）
            try:
                if user.remarks and len(user.remarks) >= 3:
                    # 尝试从remarks中提取3位数字
                    group_info = ""
                    for char in user.remarks:
                        if char.isdigit():
                            group_info += char
                            if len(group_info) == 3:
                                break
                    
                    if len(group_info) == 3:
                        group = int(group_info[0])  # 第一位表示组号
                        sequence = int(group_info[1:3])  # 后两位表示序号
                        
                        # 更新缓存
                        self._user_groups[group].append(user.username)
                        self._user_to_group[user.username] = group
                        self._user_to_sequence[user.username] = sequence
            except Exception as e:
                logger.error(f"解析用户 {user.username} 的分组信息失败: {e}")
    
    def get_user_group(self, username: str) -> int:
        """获取用户所属的组号"""
        return self._user_to_group.get(username, 0)
    
    def get_user_sequence(self, username: str) -> int:
        """获取用户在组内的序号"""
        return self._user_to_sequence.get(username, 0)
    
    def get_group_users(self, group: int) -> List[str]:
        """获取指定组的所有用户"""
        return self._user_groups.get(group, [])
    
    def get_groups_count(self) -> int:
        """获取组数"""
        return len(self._user_groups)
    
    def get_next_user(self, group: int, current_user: str) -> Optional[str]:
        """获取指定组中当前用户的下一个用户"""
        users = self._user_groups.get(group, [])
        if not users or current_user not in users:
            return None
        
        # 按照序号排序
        sorted_users = sorted(users, key=lambda u: self._user_to_sequence.get(u, 999))
        
        # 找到当前用户的索引
        try:
            current_index = sorted_users.index(current_user)
            # 返回下一个用户，如果是最后一个则返回None
            if current_index < len(sorted_users) - 1:
                return sorted_users[current_index + 1]
            return None
        except ValueError:
            return None
    
    def get_first_user_in_group(self, group: int) -> Optional[str]:
        """获取指定组中序号最小的用户"""
        users = self._user_groups.get(group, [])
        if not users:
            return None
        
        # 按照序号排序，返回序号最小的用户
        return sorted(users, key=lambda u: self._user_to_sequence.get(u, 999))[0]
    
    def reload_groups(self) -> None:
        """重新加载用户分组信息"""
        self._load_user_groups()
    
    def init_test_users(self) -> None:
        """初始化测试用户数据"""
        # 检查是否已有用户数据
        if UserInfo.select().count() > 0:
            logger.info("已存在用户数据，跳过测试用户初始化")
            return
        
        # 创建测试用户
        test_users = [
            # 第一组用户
            {"username": "zhuser1", "zhname": "zhuser1", "money": 10000, "remarks": "102", "create_date": datetime.now()},
            {"username": "Liub", "zhname": "Liub", "money": 10000, "remarks": "103", "create_date": datetime.now()},
            {"username": "Zhangl", "zhname": "Zhangl", "money": 10000, "remarks": "105", "create_date": datetime.now()},
            {"username": "zhmain", "zhname": "zhmain", "money": 10000, "remarks": "101", "create_date": datetime.now()},
            {"username": "Xiey", "zhname": "Xiey", "money": 10000, "remarks": "106", "create_date": datetime.now()},
            {"username": "Wangxy", "zhname": "Wangxy", "money": 10000, "remarks": "107", "create_date": datetime.now()},
            {"username": "Liuy", "zhname": "Liuy", "money": 10000, "remarks": "109", "create_date": datetime.now()},
            {"username": "Zouyw", "zhname": "Zouyw", "money": 10000, "remarks": "110", "create_date": datetime.now()},
            {"username": "Chen", "zhname": "Chen", "money": 10000, "remarks": "111", "create_date": datetime.now()},
            
            # 第二组用户
            {"username": "Gaoy", "zhname": "Gaoy", "money": 20000, "remarks": "203", "create_date": datetime.now()},
            {"username": "Zhangqf", "zhname": "Zhangqf", "money": 20000, "remarks": "208", "create_date": datetime.now()},
        ]
        
        # 批量插入用户数据
        for user_data in test_users:
            UserInfo.create(**user_data)
        
        logger.info(f"已创建 {len(test_users)} 个测试用户")
        
        # 重新加载用户分组信息
        self.reload_groups()


class OrderDispatcher:
    """订单分发器，负责按照分组规则分发待处理订单"""
    
    # Todo表的状态常量
    STATUS_CREATED = 10     # 任务创建
    STATUS_COMPLETED = 11   # 任务完成
    STATUS_COMPLETED2 = 2  # 任务完成2
    STATUS_COMPLETED3 = 12  # 任务完成3
    STATUS_ERROR = 5        # 任务异常完成
    STATUS_DISPATCHED = 20  # 已分发给redis（新增状态）
    
    # 测试配置
    TEST_SYMBOLS = ["ETHUSDT.BINANCE", "BTCUSDT.BINANCE"]  # 测试用的交易对
    TEST_PRICE_RANGE = (10, 5000)                          # 测试订单的价格范围
    TEST_VOLUME_RANGE = (0.1, 1.0)                         # 测试订单的数量范围
    TEST_ORDER_INTERVAL = 20                              # 测试订单生成间隔(秒)
    TEST_ORDER_COMPLETE_INTERVAL = 10                      # 测试订单完成间隔(秒)
    
    def __init__(self) -> None:
        """构造函数"""
        # 用户分组管理器
        self.user_manager = UserGroupManager()
        
        # Redis客户端
        self.redis_client = None
        
        # 记录每个组正在处理的订单ID
        # 复合键(组号, ref) -> todo_id，允许每组处理多个不同ref的订单
        self._processing_orders: Dict[Tuple[int, int], int] = {}  # (组号, ref) -> todo_id
        
        # 记录订单的分发时间，用于超时检测
        self._dispatch_times: Dict[int, datetime] = {}  # todo_id -> 分发时间

        # 超时时间配置
        self.timeout_minutes = 10  # 超时时间（分钟）
        
        # 测试模式相关属性
        self.test_enabled = False
        self._last_test_order_time = datetime.now()
        self._last_test_complete_time = datetime.now()
        self._test_ref_counter = 100  # 测试订单的ref起始值
        
        # 运行标志
        self._running = False
        
        # 分发线程
        self._dispatch_thread = None
        
        # 初始化Redis连接
        self.init_redis()
    
    def init_redis(self) -> None:
        """初始化Redis连接"""
        try:
            # 从全局设置中获取Redis配置
            redis_host = SETTINGS.get("redis.host", "localhost")
            redis_port = SETTINGS.get("redis.port", 6379)
            redis_password = SETTINGS.get("redis.password", None)
            redis_db = 0
            
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True  # 自动将响应解码为字符串
            )
            logger.info(f"Redis连接初始化成功: {redis_host}:{redis_port}/{redis_db}")
        except Exception as e:
            logger.error(f"Redis连接初始化失败: {e}")
    
    def start(self, test_enabled=False) -> None:
        """
        启动分发器
        
        参数:
            test_enabled: bool
                是否启用测试模式，默认为False
        """
        # 设置测试模式
        self.test_enabled = test_enabled
        
        # 如果启用测试模式，初始化测试用户
        if self.test_enabled:
            self.user_manager.init_test_users()
            logger.info("测试模式已启用，将自动生成测试订单")
        
        self._running = True
        logger.info("订单分发器启动成功")
        
        # 创建并启动分发线程
        self._dispatch_thread = threading.Thread(target=self._dispatch_loop, name="DispatchThread")
        self._dispatch_thread.daemon = True  # 设置为守护线程，主线程结束时自动结束
        self._dispatch_thread.start()
        logger.info("分发线程已启动")
    
    def stop(self) -> None:
        """停止分发器"""
        if not self._running:
            return
            
        self._running = False
        logger.info("订单分发器正在停止...")
        
        # 等待分发线程结束
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=5)  # 最多等待5秒
            if self._dispatch_thread.is_alive():
                logger.warning("分发线程未能在5秒内停止")
        
        logger.info("订单分发器已停止")
    
    def _dispatch_loop(self) -> None:
        """主分发循环"""
        last_print_time = datetime.now()
        
        while self._running:
            try:
                # 检查Redis连接
                if not self.redis_client:
                    logger.warning("Redis连接不可用，尝试重新连接...")
                    self.init_redis()
                    time.sleep(1)
                    continue
                
                # 检查超时订单
                self._check_timeout_orders()
                
                # 检查已完成的订单，为其所在的组发送下一个订单
                self._check_completed_orders()
                
                # 检查待处理的订单，为每个组的每个不同ref发送订单
                self._dispatch_pending_orders()
                
                # 每分钟打印一次等待分发的任务
                now = datetime.now()
                if (now - last_print_time).total_seconds() >= 60:
                    self._print_pending_tasks()
                    last_print_time = now
                
                # 测试模式：生成测试订单和模拟完成订单
                if self.test_enabled:
                    # 生成测试订单
                    if (now - self._last_test_order_time).total_seconds() >= self.TEST_ORDER_INTERVAL:
                        self._generate_test_orders()
                        self._last_test_order_time = now
                    
                    # 模拟完成订单
                    if (now - self._last_test_complete_time).total_seconds() >= self.TEST_ORDER_COMPLETE_INTERVAL:
                        self._simulate_order_completion()
                        self._last_test_complete_time = now
                
                # 短暂休眠，避免CPU占用过高
                time.sleep(0.1)
                
            except Exception as e:
                logger.exception(f"分发循环发生错误: {e}")
                time.sleep(1)  # 出错后暂停一秒
    
    def _generate_test_orders(self) -> None:
        """生成测试订单"""
        try:
            # 获取当前最大的ref值
            max_ref = Todo.select(fn.MAX(Todo.ref)).scalar() or 100
            
            # 生成新的ref（父任务ID）
            self._test_ref_counter = max(self._test_ref_counter, max_ref) + 1
            ref = self._test_ref_counter
            
            # 随机选择交易对
            vt_symbol = random.choice(self.TEST_SYMBOLS)
            
            # 随机选择方向
            direction = random.choice(["多", "空"])
            
            # 随机生成价格和数量
            price = random.uniform(*self.TEST_PRICE_RANGE)
            volume = random.uniform(*self.TEST_VOLUME_RANGE)
            
            # 为每个组的用户创建订单
            created_count = 0
            current_time = datetime.now()
            
            for group in range(1, self.user_manager.get_groups_count() + 1):
                users = self.user_manager.get_group_users(group)
                if not users:
                    continue
                
                # 按序号排序用户
                sorted_users = sorted(users, key=lambda u: self.user_manager.get_user_sequence(u))
                
                # 为每个用户创建订单
                for i, user in enumerate(sorted_users):
                    # 为每个用户添加一个微小的时间差，避免唯一键冲突
                    order_time = current_time + timedelta(seconds=i*0.1)
                    # 为每个用户添加一个随机后缀，避免content字段冲突
                    random_suffix = f"{random.randint(1000, 9999)}"
                    
                    try:
                        todo = Todo(
                            content=f"{vt_symbol}_test_{random_suffix}",
                            vt_symbol=vt_symbol,
                            direction=direction,
                            offset="开",  # 固定为开仓
                            price=price,
                            signal_volume=volume,
                            real_volume=volume,
                            level=1,
                            ref=ref,  # 使用同一个ref
                            user=user,
                            completed=self.STATUS_CREATED,  # 新创建状态
                            datetime=order_time,
                            create_date=order_time,
                            create_by="system",
                            remarks=f"测试订单-组{group}-{random_suffix}",
                            orderid="",
                            kuo1="",
                            kuo2=""
                        )
                        todo.save()
                        created_count += 1
                    except Exception as e:
                        logger.error(f"为用户 {user} 创建测试订单时发生错误: {e}")
            
            logger.info(f"已生成 {created_count} 个测试订单，ref={ref}, 交易对={vt_symbol}, 方向={direction}, 价格={price:.2f}, 数量={volume:.4f}")
        except Exception as e:
            logger.exception(f"生成测试订单时发生错误: {e}")
    
    def _simulate_order_completion(self) -> None:
        """模拟订单完成"""
        try:
            # 查找状态为已分发的订单
            dispatched_orders = Todo.select().where(
                Todo.completed == self.STATUS_DISPATCHED
            ).limit(2)  # 每次最多模拟完成2个订单
            
            completed_count = 0
            for order in dispatched_orders:
                # 随机决定是否完成成功 (80%成功率)
                status = self.STATUS_COMPLETED if random.random() > 0.2 else self.STATUS_ERROR
                
                # 更新订单状态
                Todo.update(
                    completed=status
                ).where(Todo.id == order.id).execute()
                
                logger.info(f"模拟完成订单[{order.id}], 用户:{order.user}, ref:{order.ref}, 状态:{status}")
                completed_count += 1
                
            if completed_count == 0 and random.random() > 0.7:
                # 如果没有已分发的订单，尝试分发一些待处理的订单
                self._dispatch_pending_orders()
                
        except Exception as e:
            logger.exception(f"模拟订单完成时发生错误: {e}")
    
    def _find_and_dispatch_next_order(self, ref: int, username: str) -> None:
        """查找并分发指定ref和用户的下一个待处理订单"""
        next_todo = Todo.get_or_none(
            (Todo.ref == ref) & 
            (Todo.user == username) & 
            (Todo.completed == self.STATUS_CREATED)
        )
        
        if next_todo:
            self._dispatch_order(next_todo)
    
    def _dispatch_order(self, todo: Todo) -> None:
        """分发指定的订单到Redis"""
        try:
            # 准备要发送到Redis的订单数据
            order_data = {
                "id": todo.id,
                "content": todo.content,
                "vt_symbol": todo.vt_symbol,
                "direction": todo.direction,
                "offset": todo.offset,
                "price": float(todo.price),
                "signal_volume": float(todo.signal_volume),
                "real_volume": float(todo.real_volume),
                "level": todo.level,
                "ref": todo.ref,
                "user": todo.user,
                "datetime": todo.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "remarks": todo.remarks,
                "orderid": todo.orderid
            }
            
            # 转换为JSON字符串并发送到Redis
            redis_key = f"order:{todo.id}"
            self.redis_client.set(redis_key, json.dumps(order_data))
            
            # 获取用户所属的组
            group = self.user_manager.get_user_group(todo.user)
            logger.info(f"订单[{todo.id}]已分发到Redis，用户:{todo.user}，组:{group}，ref:{todo.ref}")
            
            # 更新订单状态为已分发
            Todo.update(
                completed=self.STATUS_DISPATCHED
            ).where(Todo.id == todo.id).execute()
            
            # 记录分发时间和正在处理的订单
            self._processing_orders[(group, todo.ref)] = todo.id
            self._dispatch_times[todo.id] = datetime.now()
        except Exception as e:
            logger.exception(f"分发订单[{todo.id}]时发生错误: {e}")
    
    def _print_pending_tasks(self) -> None:
        """打印各组等待分发的任务"""
        try:
            # 获取所有待处理的订单
            pending_tasks = Todo.select().where(
                Todo.completed == self.STATUS_CREATED
            )
            
            # 按组统计待处理订单
            group_tasks = defaultdict(list)
            for task in pending_tasks:
                user = task.user
                group = self.user_manager.get_user_group(user)
                if group > 0:  # 只统计有效组
                    group_tasks[group].append(task)
            
            # 打印统计结果
            # logger.info("="*50)
            logger.info(f"各组等待分发的任务统计 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            # logger.info("="*50)
            
            if not group_tasks:
                logger.info("当前没有等待分发的任务")
            else:
                for group, tasks in sorted(group_tasks.items()):
                    # 按ref分组统计
                    ref_counts = defaultdict(int)
                    for task in tasks:
                        ref_counts[task.ref] += 1
                    
                    # 打印该组的统计信息
                    ref_info = ", ".join([f"ref={ref}:{count}个" for ref, count in ref_counts.items()])
                    logger.info(f"组 {group}: 共 {len(tasks)} 个待处理任务 ({ref_info})")
                    
                    # 打印详细信息
                    for task in tasks:
                        logger.info(f"  - 任务ID: {task.id}, 用户: {task.user}, ref: {task.ref}, 内容: {task.content}, 标的: {task.vt_symbol}")
            
            logger.info("="*50)
        except Exception as e:
            logger.exception(f"打印待处理任务时发生错误: {e}")
    
    def _check_timeout_orders(self) -> None:
        """检查超时订单"""
        now = datetime.now()
        timeout_limit = now - timedelta(minutes=self.timeout_minutes)
        
        # 检查每个正在处理的订单是否超时
        for todo_id, dispatch_time in list(self._dispatch_times.items()):
            if dispatch_time < timeout_limit:
                # 超过10分钟未完成，记录错误信息
                todo = Todo.get_or_none(Todo.id == todo_id)
                if todo:
                    error_msg = f"订单[{todo_id}]超过{self.timeout_minutes}分钟未完成，用户:{todo.user}"
                    logger.warning(f"超时警告: {error_msg}")
                
                # 从超时记录中移除
                del self._dispatch_times[todo_id]
    
    def _check_completed_orders(self) -> None:
        """检查已完成的订单，为其所在的组发送下一个订单"""
        # 查找每个组当前正在处理的订单
        for (group, ref), todo_id in list(self._processing_orders.items()):
            # 检查订单是否已完成（状态为11或5）
            todo = Todo.get_or_none(Todo.id == todo_id)
            if not todo:
                # 订单不存在，从记录中移除
                del self._processing_orders[(group, ref)]
                continue
            
            # 如果订单已完成（完成或异常完成）
            if todo.completed in {self.STATUS_COMPLETED, self.STATUS_ERROR, self.STATUS_COMPLETED2, self.STATUS_COMPLETED3}:
                logger.info(f"组 {group} 的ref {ref} 订单 {todo_id} 已完成(状态:{todo.completed})，准备发送下一个订单")
                
                # 从处理中和超时记录移除
                del self._processing_orders[(group, ref)]
                if todo_id in self._dispatch_times:
                    del self._dispatch_times[todo_id]
                
                # 查找同一ref的下一个用户的订单
                next_user = self.user_manager.get_next_user(group, todo.user)
                if next_user:
                    self._find_and_dispatch_next_order(todo.ref, next_user)
    
    def reload_user_groups(self) -> None:
        """重新加载用户分组信息"""
        self.user_manager.reload_groups()
        logger.info("已重新加载用户分组信息")


    def _dispatch_pending_orders(self) -> None:
        """检查待处理的订单，为每个组的每个不同ref发送订单"""
        # 获取不同ref的待处理订单
        refs = Todo.select(Todo.ref).where(
            Todo.completed == self.STATUS_CREATED
        ).group_by(Todo.ref).order_by(Todo.ref)
        
        for ref_obj in refs:
            ref = ref_obj.ref
            group_count =  self.user_manager.get_groups_count()

            # 对于每个ref，检查各个组是否有待处理的订单
            for group in range(1, group_count+1):
                # 如果该组已经在处理该ref的订单，则跳过
                if (group, ref) in self._processing_orders:
                    continue
                
                # 获取该组的第一个用户
                first_user = self.user_manager.get_first_user_in_group(group)
                if not first_user:
                    continue
                
                # 查找该用户在该ref下的待处理订单
                todo = Todo.get_or_none(
                    (Todo.ref == ref) & 
                    (Todo.user == first_user) & 
                    (Todo.completed == self.STATUS_CREATED)
                )
                
                if todo:
                    self._dispatch_order(todo)
                else:
                    # 如果first_user的任务找不到，说明目前是"查漏"状态
                    # 按顺序循环获取下一个用户，直到找到有待处理订单的用户
                    current_user = first_user
                    while current_user:
                        # 查找该用户在该ref下的待处理订单
                        todo = Todo.get_or_none(
                            (Todo.ref == ref) &
                            (Todo.user == current_user) &
                            (Todo.completed == self.STATUS_CREATED)
                        )
                        if todo:
                            self._dispatch_order(todo)
                            break  # 找到一个待处理的订单后退出循环

                        # 获取下一个用户
                        current_user = self.user_manager.get_next_user(group, current_user)

                    # 如果所有用户都没有待处理订单，记录日志
                    if not current_user:
                        logger.debug(f"组 {group} 中没有用户有ref {ref}的待处理订单")

# 创建全局分发器实例
_dispatcher = None

def get_dispatcher() -> OrderDispatcher:
    """获取全局分发器实例"""
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = OrderDispatcher()
    return _dispatcher
