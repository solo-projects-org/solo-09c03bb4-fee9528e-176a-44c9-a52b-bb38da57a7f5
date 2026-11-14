"""轨检裂纹事件平台：契约排程 API + 事件分发 API。

三方角色：
- producer  采集端（发布端，滚动升级时 old/mixed/new 三态）
- qc        质检订阅端 / 质检接收端
- report    日报订阅端 / 日报接收端

模块：
- models/compat/planner：契约兼容性判定与滚动升级排程
- dispatch_models/store/dispatcher/dispatch_api：事件分发链路
- receiver：本地接收端演示服务（同一镜像以 RECEIVER_NAME 区分实例）
"""

__all__ = [
    "models",
    "compat",
    "planner",
    "dispatch_models",
    "store",
    "dispatcher",
    "dispatch_api",
    "receiver",
    "main",
]
