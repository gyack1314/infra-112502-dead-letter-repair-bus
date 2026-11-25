"""领域异常。"""


class RepairBusError(ValueError):
    """输入契约或当前状态不允许该操作。"""


class BlockedOperation(RepairBusError):
    """依赖/回执/更正状态尚未满足。"""

    def __init__(self, message: str, blockers: list[dict] | None = None):
        super().__init__(message)
        self.blockers = blockers or []
