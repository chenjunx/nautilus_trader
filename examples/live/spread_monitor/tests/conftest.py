import sys
from pathlib import Path


# 让 `import spread_monitor` 在 pytest 收集这个目录时也能解析——等价于直接
# `python examples/live/cross_venue_spread_monitor.py` 时脚本目录被自动加入 sys.path。
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
