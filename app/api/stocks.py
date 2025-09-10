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
from app.models.transaction import Transaction
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

@bp.route('/stocks/<int:stock_id>', methods=['PUT'])
def update_stock(stock_id):
    """更新股票信息"""
    try:
        # 获取请求数据
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': _('No data provided')}), 400
        
        # 查找股票缓存记录
        stock = StocksCache.query.get(stock_id)
        if not stock:
            return jsonify({'success': False, 'error': _('Stock not found')}), 404
        
        # 获取新的股票代码和旧的股票代码
        old_symbol = stock.symbol
        new_symbol = data.get('symbol', '').upper().strip()
        
        if not new_symbol:
            return jsonify({'success': False, 'error': _('Stock symbol is required')}), 400
        
        # 只有当股票代码或货币发生变化时才检查重复
        new_currency = data.get('currency', stock.currency)
        existing_stock_transactions_updated = 0
        if new_symbol != old_symbol or new_currency != stock.currency:
            existing_stock = StocksCache.query.filter(
                StocksCache.symbol == new_symbol,
                StocksCache.currency == new_currency,
                StocksCache.id != stock_id
            ).first()
            
            if existing_stock:
                # 当发现重复的股票代码时，不返回错误，而是进行以下操作：
                # 1. 将现有重复股票的所有交易记录更新为当前正在编辑的股票代码
                existing_transactions = Transaction.query.filter_by(stock=existing_stock.symbol).all()
                for transaction in existing_transactions:
                    transaction.stock = old_symbol  # 暂时设置为旧代码，稍后会一起更新为新代码
                existing_stock_transactions_updated = len(existing_transactions)
                
                # 2. 删除重复的stock_cache记录
                print(f"删除重复的股票缓存记录: {existing_stock.symbol} ({existing_stock.currency})")
                print(f"将 {existing_stock_transactions_updated} 条交易记录从 {existing_stock.symbol} 更新为 {old_symbol}")
                db.session.delete(existing_stock)
                db.session.flush()  # 确保删除操作立即生效
        
        # 更新股票缓存记录
        stock.symbol = new_symbol
        stock.name = data.get('name', stock.name)
        stock.exchange = data.get('exchange', stock.exchange)
        stock.currency = new_currency
        
        # 如果股票代码发生变化，更新所有相关的交易记录
        updated_transactions_count = 0
        if old_symbol != new_symbol:
            transactions = Transaction.query.filter_by(stock=old_symbol).all()
            for transaction in transactions:
                transaction.stock = new_symbol
            updated_transactions_count = len(transactions)
            print(f"Updated {updated_transactions_count} transaction records from {old_symbol} to {new_symbol}")
        
        # 总的更新交易记录数量（包括原有的和从重复股票合并过来的）
        total_updated_transactions = updated_transactions_count + existing_stock_transactions_updated
        
        # 强制刷新股票价格和信息
        from app.services.stock_price_service import StockPriceService
        price_service = StockPriceService()
        
        # 如果名称或交易所为空，或者股票代码发生变化，强制从Yahoo Finance获取最新信息
        should_refresh_info = (not stock.name or not stock.exchange or old_symbol != new_symbol)
        
        if should_refresh_info:
            print(f"强制刷新{new_symbol}({stock.currency})的价格和信息...")
            # 强制更新价格，这会同时更新名称和交易所信息（如果当前为空）
            price_updated = price_service.update_stock_price(new_symbol, stock.currency)
            
            # 重新查询更新后的股票信息
            db.session.refresh(stock)
            
            if price_updated:
                print(f"成功刷新{new_symbol}的信息: 名称={stock.name}, 交易所={stock.exchange}, 价格={stock.current_price}")
            else:
                print(f"无法从Yahoo Finance获取{new_symbol}的信息")
        
        # 重置价格更新时间，强制下次访问时重新获取价格
        stock.price_updated_at = None
        
        db.session.commit()
        
        # 构建响应消息
        response_data = {
            'success': True,
            'message': _('Stock updated successfully'),
            'updated_transactions': total_updated_transactions,
            'refreshed_info': should_refresh_info,
            'stock_info': {
                'symbol': stock.symbol,
                'name': stock.name,
                'exchange': stock.exchange,
                'currency': stock.currency,
                'current_price': float(stock.current_price) if stock.current_price else None
            }
        }
        
        # 如果有合并重复股票的操作，添加额外信息
        if existing_stock_transactions_updated > 0:
            response_data['merged_duplicate'] = True
            response_data['merged_transactions'] = existing_stock_transactions_updated
            print(f"成功合并重复股票，更新了 {existing_stock_transactions_updated} 条来自重复股票的交易记录")
        
        return jsonify(response_data)
        
    except Exception as e:
        db.session.rollback()
        print(f"Error updating stock: {str(e)}")
        return jsonify({'success': False, 'error': _('Failed to update stock')}), 500

@bp.route('/stocks/<int:stock_id>', methods=['DELETE'])
def delete_stock(stock_id):
    """删除股票缓存记录"""
    try:
        # 查找股票缓存记录
        stock = StocksCache.query.get(stock_id)
        if not stock:
            return jsonify({'success': False, 'error': _('Stock not found')}), 404
        
        # 删除股票缓存记录（不影响交易记录）
        db.session.delete(stock)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('Stock cache record deleted successfully')
        })
        
    except Exception as e:
        db.session.rollback()
        print(f"Error deleting stock: {str(e)}")
        return jsonify({'success': False, 'error': _('Failed to delete stock')}), 500

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

@bp.route('/stocks/refresh-info', methods=['POST'])
def refresh_stock_info():
    """临时获取股票信息用于表单自动填充"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': _('No data provided')}), 400
        
        symbol = data.get('symbol', '').upper().strip()
        currency = data.get('currency', 'USD')
        
        if not symbol:
            return jsonify({'success': False, 'error': _('Stock symbol is required')}), 400
        
        # 使用股票价格服务获取信息
        from app.services.stock_price_service import StockPriceService
        price_service = StockPriceService()
        
        # 直接从Yahoo Finance获取信息
        stock_data = price_service.get_stock_price(symbol)
        
        if stock_data:
            return jsonify({
                'success': True,
                'stock_info': {
                    'symbol': symbol,
                    'name': stock_data.get('name', ''),
                    'exchange': stock_data.get('exchange', ''),
                    'currency': stock_data.get('currency', currency),
                    'current_price': stock_data.get('price', 0)
                }
            })
        else:
            return jsonify({
                'success': False,
                'error': _('Unable to get stock information from Yahoo Finance')
            })
        
    except Exception as e:
        print(f"Error refreshing stock info: {str(e)}")
        return jsonify({'success': False, 'error': _('Failed to get stock information')}), 500