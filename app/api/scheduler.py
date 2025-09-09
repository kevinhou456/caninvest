"""
调度器管理API
"""

from flask import jsonify, request
from app.api import bp
from app.scheduler import scheduler

@bp.route('/scheduler/status', methods=['GET'])
def get_scheduler_status():
    """获取调度器状态"""
    try:
        status = scheduler.get_job_status()
        return jsonify({
            'success': True,
            'data': status
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/scheduler/trigger-price-update', methods=['POST'])
def trigger_price_update():
    """手动触发价格更新"""
    try:
        # 获取请求参数
        data = request.get_json() or {}
        symbols = data.get('symbols', [])
        
        # 手动执行价格更新
        from app.services.stock_price_service import StockPriceService
        from app.models.stocks_cache import StocksCache
        
        price_service = StockPriceService()
        
        if symbols:
            # 更新指定股票 - 需要查找对应的currency信息
            symbol_currency_pairs = []
            for symbol in symbols:
                # 查找该symbol的所有可能的currency组合
                stocks = StocksCache.query.filter_by(symbol=symbol).all()
                if stocks:
                    for stock in stocks:
                        symbol_currency_pairs.append((stock.symbol, stock.currency))
                else:
                    # 如果没有找到，尝试用默认货币USD
                    symbol_currency_pairs.append((symbol, 'USD'))
            
            results = price_service.update_prices_for_symbols(symbol_currency_pairs)
        else:
            # 更新需要更新的股票
            stocks_needing_update = StocksCache.get_stocks_needing_update()
            if stocks_needing_update:
                symbol_currency_pairs = [(stock.symbol, stock.currency) for stock in stocks_needing_update[:20]]
                results = price_service.update_prices_for_symbols(symbol_currency_pairs)
            else:
                results = {
                    'updated': 0,
                    'skipped': 0,
                    'failed': 0,
                    'errors': ['No stocks need updates']
                }
        
        return jsonify({
            'success': True,
            'data': results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/scheduler/stocks-needing-update', methods=['GET'])
def get_stocks_needing_update():
    """获取需要更新价格的股票列表"""
    try:
        from app.models.stocks_cache import StocksCache
        stocks = StocksCache.get_stocks_needing_update()
        
        stocks_data = []
        for stock in stocks[:50]:  # 限制返回数量
            stocks_data.append({
                'symbol': stock.symbol,
                'name': stock.name,
                'current_price': float(stock.current_price) if stock.current_price else None,
                'price_updated_at': stock.price_updated_at.isoformat() if stock.price_updated_at else None,
                'needs_update': stock.needs_price_update(),
                'is_trading_hours': stock.is_trading_hours()
            })
        
        return jsonify({
            'success': True,
            'data': {
                'stocks': stocks_data,
                'total_count': len(stocks)
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/scheduler/api-usage', methods=['GET'])
def get_api_usage():
    """获取API使用统计"""
    try:
        from app.services.stock_price_service import StockPriceService
        price_service = StockPriceService()
        usage_stats = price_service.get_api_usage_stats()
        
        return jsonify({
            'success': True,
            'data': usage_stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500