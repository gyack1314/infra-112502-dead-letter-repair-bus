"""JSON 快照持久化。

演练/运营入口每次处理一条输入后原子落盘，因此进程重启后可以从同一状态继续，
未消解的外部回执查询不会只存在于内存。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .model import new_state


class StateStore:
    def __init__(self, path: str | os.PathLike[str] | None):
        self.path = Path(path) if path else None

    def load(self) -> dict[str, Any]:
        if self.path is None:
            return new_state()
        if not self.path.exists():
            state = new_state()
            self.save(state)
            return state
        with self.path.open("r", encoding="utf-8") as stream:
            state = json.load(stream)
        if not isinstance(state, dict) or state.get("version") != 1:
            raise ValueError(f"状态文件版本不受支持：{self.path}")
        return state

    def save(self, state: dict[str, Any]) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        with tmp.open("w", encoding="utf-8") as stream:
            json.dump(state, stream, ensure_ascii=False, indent=2, sort_keys=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, self.path)
