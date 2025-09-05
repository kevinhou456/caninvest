"""
股票分类管理API - 临时禁用
StockCategory and StockCategoryI18n models have been deleted during system redesign
"""

from flask import request, jsonify
from flask_babel import _
from app import db
# StockCategory and StockCategoryI18n models have been deleted - temporarily disabling functionality
# from app.models.stock import StockCategory, StockCategoryI18n
from . import bp

@bp.route('/stock-categories', methods=['GET'])
def get_stock_categories():
    """获取所有股票分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({
        'error': 'Stock categories temporarily disabled during system redesign',
        'categories': []
    })

@bp.route('/stock-categories', methods=['POST'])
def create_stock_category():
    """创建股票分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Category creation temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/<int:category_id>', methods=['GET'])
def get_stock_category(category_id):
    """获取特定分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Category lookup temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/<int:category_id>', methods=['PUT'])
def update_stock_category(category_id):
    """更新股票分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Category updates temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/<int:category_id>', methods=['DELETE'])
def delete_stock_category(category_id):
    """删除股票分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Category deletion temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/batch', methods=['POST'])
def batch_create_categories():
    """批量创建分类 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Batch category operations temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/templates', methods=['GET'])
def get_category_templates():
    """获取分类模板 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Category templates temporarily disabled during system redesign')}), 503

@bp.route('/stock-categories/apply-template', methods=['POST'])
def apply_category_template():
    """应用分类模板 - 临时禁用"""
    # Temporarily disabled - StockCategory model has been deleted
    return jsonify({'error': _('Template application temporarily disabled during system redesign')}), 503