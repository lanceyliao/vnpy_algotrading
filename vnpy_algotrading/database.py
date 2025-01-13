from datetime import datetime
from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    FloatField,
    IntegerField,
    Model as ModelBase
)
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import load_json
SETTINGS.update(load_json("vt_setting_local.json"))
from vnpy_mysql.mysql_database import db

# 算法单状态码
ALGO_STATUS = {
    "运行": 1,
    "暂停": 2,
    "停止": 3,
    "结束": 4
}

# 算法模板代码
ALGO_TEMPLATE = {
    "VolumeFollowAlgo": 1,
    "TwapAlgo": 2,
    "IcebergAlgo": 3,
    "SniperAlgo": 4,
    "StopAlgo": 5,
    "BestLimitAlgo": 6
}

# 反向映射
STATUS_ALGO = {v: k for k, v in ALGO_STATUS.items()}
TEMPLATE_ALGO = {v: k for k, v in ALGO_TEMPLATE.items()}


class Todo(ModelBase):
    """
    Index is id 
    """
    id = AutoField()
    content = CharField()  # f"{vt_symbol}_{strategy}"
    vt_symbol = CharField()
    direction = CharField()
    offset = CharField()
    price = FloatField()
    signal_volume = FloatField()
    real_volume = FloatField()
    level = IntegerField()
    ref = IntegerField()  # 子任务对应的父任务id
    user = CharField()
    completed = IntegerField()  # 1任务创建 2任务完成 5任务异常完成
    datetime = DateTimeField()
    create_date = DateTimeField()
    create_by = CharField()
    remarks = CharField()
    orderid = CharField()
    kuo1 = CharField()
    kuo2 = CharField()
    
    class Meta:
        database = db
        indexes = ((("content", "vt_symbol", "datetime"), True),)


class AlgoOrder(ModelBase):
    """算法单状态表"""
    id = AutoField()
    todo_id = IntegerField()  # 关联的Todo表ID
    vt_symbol = CharField()
    direction = CharField()
    offset = CharField()
    price = FloatField()
    volume = FloatField()  # 总量
    traded = FloatField()  # 已成交
    traded_price = FloatField()  # 成交均价
    status = IntegerField()  # 状态码: 1=RUNNING, 2=PAUSED, 3=STOPPED, 4=FINISHED
    template = IntegerField()  # 算法模板代码: 1=VolumeFollowAlgo, 2=TwapAlgo, ...
    start_time = DateTimeField()
    update_time = DateTimeField()
    
    class Meta:
        database = db
        indexes = (
            (("todo_id",), False),  # 唯一索引
            (("vt_symbol", "status"), False),
        )


def init_database() -> None:
    """初始化数据库"""
    db.create_tables([Todo, AlgoOrder], safe=True)
