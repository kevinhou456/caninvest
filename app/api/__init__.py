"""
API蓝图包
"""

from flask import Blueprint

# 创建API蓝图
bp = Blueprint('api', __name__)

# 导入所有路由模块
from . import families, members, accounts, stocks, categories, imports, reports, scheduler, validation

# 导出蓝图
__all__ = ['bp']