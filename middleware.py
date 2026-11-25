#!/usr/bin/env python3
"""兼容常见入口命名；实际实现位于 repair_bus 包。"""

from repair_bus.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
