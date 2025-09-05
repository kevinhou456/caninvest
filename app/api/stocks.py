"""
股票管理API - 临时禁用
Stock and StockCategory models have been deleted during system redesign
"""

from flask import request, jsonify
from flask_babel import _
from app import db
# Stock and StockCategory models have been deleted - temporarily disabling functionality
# from app.models.stock import Stock, StockCategory
from app.models.stocks_cache import StocksCache
from app.models.price_cache import StockPriceCache
from . import bp

@bp.route('/stocks/search', methods=['GET'])
def search_stocks():
    """搜索股票 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({
        'error': 'Stock search temporarily disabled during system redesign',
        'stocks': []
    })

@bp.route('/stocks/<symbol>', methods=['GET'])
def get_stock(symbol):
    """获取股票详情 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock details temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>/price', methods=['GET'])
def get_stock_price(symbol):
    """获取股票当前价格 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock price lookup temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>/history', methods=['GET'])
def get_stock_history(symbol):
    """获取股票历史价格 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock history temporarily disabled during system redesign')}), 503

@bp.route('/stocks', methods=['POST'])
def create_stock():
    """创建股票记录 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock creation temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>', methods=['PUT'])
def update_stock(symbol):
    """更新股票信息 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock updates temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>/categories', methods=['POST'])
def add_stock_category(symbol):
    """为股票添加分类 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock categorization temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>/categories', methods=['DELETE'])
def remove_stock_category(symbol):
    """移除股票分类 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Stock categorization temporarily disabled during system redesign')}), 503

@bp.route('/stocks/batch-categorize', methods=['POST'])
def batch_categorize_stocks():
    """批量分类股票 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Batch stock operations temporarily disabled during system redesign')}), 503

@bp.route('/stocks/<symbol>/category-suggestions', methods=['GET'])
def get_category_suggestions(symbol):
    """获取股票分类建议 - 临时禁用"""
    # Temporarily disabled - Stock model has been deleted
    return jsonify({'error': _('Category suggestions temporarily disabled during system redesign')}), 503