"""裂纹事件流契约排程 API。

三方角色：
- producer  采集端（发布端，滚动升级时 old/mixed/new 三态）
- qc        质检订阅端
- report    日报订阅端
"""

__all__ = ["models", "compat", "planner", "main"]
