EVENT_ALGO_LOG = "eAlgoLog"
EVENT_ALGO_UPDATE = "eAlgoUpdate"


APP_NAME = "AlgoTrading"

class EnumId:
    def __init__(self, *args):
        self.idx2name = {}
        for (idx, name) in enumerate(args, 1):
            setattr(self, name, idx)
            self.idx2name[idx] = name

    def to_str(self, idx):
        return self.idx2name.get(idx, "NOTFOUND")


AlgoStatusEnum = EnumId(
    "RUNNING",   # 运行
    "PAUSED",    # 暂停
    "STOPPED",   # 停止
    "FINISHED"   # 结束
)
AlgoTemplateEnum = EnumId(
    "VolumeFollowAlgo",
    "VolumeFollowSyncAlgo",
    "TwapAlgo",
    "IcebergAlgo", 
    "SniperAlgo",
    "StopAlgo",
    "BestLimitAlgo"
)


def is_active(status: int) -> bool:
    """判断状态是否处于活动状态"""
    return status in {AlgoStatusEnum.RUNNING, AlgoStatusEnum.PAUSED}


def is_finished(status: int) -> bool:
    """判断状态是否已完成"""
    return status in {AlgoStatusEnum.STOPPED, AlgoStatusEnum.FINISHED}
