#!/usr/bin/env python3
"""项目根目录运行入口，等价于 python3 -m repair_bus。"""

from repair_bus.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
