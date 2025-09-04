"""
主蓝图包
"""

from flask import Blueprint

# 创建主蓝图
bp = Blueprint('main', __name__)

# 导入路由模块
from . import routes

# 导出蓝图
__all__ = ['bp']