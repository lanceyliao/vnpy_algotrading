from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    FloatField,
    IntegerField,
    Model as ModelBase
)
from vnpy_mysql.mysql_database import db
from .base import AlgoStatusEnum, AlgoTemplateEnum

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
    completed = IntegerField()  # 10任务创建 11任务完成 5任务异常完成
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
    status = IntegerField()  # 状态码: ALGO_STATUS.RUNNING, ALGO_STATUS.PAUSED, ...
    template = IntegerField()  # 算法模板代码: ALGO_TEMPLATE.VolumeFollowAlgo, ALGO_TEMPLATE.TwapAlgo, ...
    start_time = DateTimeField()
    update_time = DateTimeField()
    
    def __str__(self) -> str:
        """格式化输出算法单信息"""        
        return (
            f"算法单[todo_id:{self.todo_id}]: {self.vt_symbol}, "
            f"方向: {self.direction}, 开平: {self.offset}, "
            f"价格: {self.price}, 数量: {self.volume}, "
            f"已成交: {self.traded}, 成交均价: {self.traded_price}, "
            f"状态: {AlgoStatusEnum.to_str(self.status)}, "
            f"算法: {self.template}"
        )
    
    class Meta:
        database = db
        indexes = (
            (("todo_id",), False),  # 唯一索引
            (("vt_symbol", "status"), False),
        )


def init_database() -> None:
    """初始化数据库"""
    db.create_tables([Todo, AlgoOrder], safe=True)
