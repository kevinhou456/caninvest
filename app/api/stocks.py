"""
股票管理API
"""

from flask import request, jsonify
from flask_babel import _
from app import db
from app.models.stock import Stock, StockCategory
from app.models.price_cache import StockPriceCache
from . import bp

@bp.route('/stocks/search', methods=['GET'])
def search_stocks():
    """搜索股票"""
    query = request.args.get('q', '').strip()
    limit = request.args.get('limit', 10, type=int)
    
    if not query:
        return jsonify({'stocks': []})
    
    stocks = Stock.search(query, limit=min(limit, 50))
    
    return jsonify({
        'stocks': [stock.to_dict(include_price=True) for stock in stocks]
    })

@bp.route('/stocks/<symbol>', methods=['GET'])
def get_stock(symbol):
    """获取股票详情"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    # 获取价格历史
    include_history = request.args.get('include_history', 'false').lower() == 'true'
    days = request.args.get('days', 30, type=int)
    
    result = stock.to_dict(include_price=True)
    
    if include_history:
        result['price_history'] = stock.get_price_history(days=days)
    
    return jsonify(result)

@bp.route('/stocks/<symbol>/price', methods=['GET'])
def get_stock_price(symbol):
    """获取股票当前价格"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    # 获取缓存价格
    cached_price = StockPriceCache.get_current_price(stock.id)
    
    if cached_price and not cached_price.is_expired:
        return jsonify({
            'symbol': stock.symbol,
            'price': float(cached_price.price),
            'price_change': float(cached_price.price_change) if cached_price.price_change else None,
            'price_change_percent': float(cached_price.price_change_percent) if cached_price.price_change_percent else None,
            'volume': cached_price.volume,
            'last_updated': cached_price.last_updated.isoformat(),
            'cache_status': 'fresh'
        })
    
    # 如果没有缓存或已过期，返回过期数据并标记
    if cached_price:
        return jsonify({
            'symbol': stock.symbol,
            'price': float(cached_price.price),
            'price_change': float(cached_price.price_change) if cached_price.price_change else None,
            'price_change_percent': float(cached_price.price_change_percent) if cached_price.price_change_percent else None,
            'volume': cached_price.volume,
            'last_updated': cached_price.last_updated.isoformat(),
            'cache_status': 'expired'
        })
    
    return jsonify({'error': _('No price data available')}), 404

@bp.route('/stocks/<symbol>/history', methods=['GET'])
def get_stock_history(symbol):
    """获取股票历史价格"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    days = request.args.get('days', 30, type=int)
    price_type = request.args.get('type', 'daily')
    
    history = StockPriceCache.get_historical_prices(stock.id, price_type, days)
    
    return jsonify({
        'symbol': stock.symbol,
        'price_type': price_type,
        'days': days,
        'history': [price.to_dict() for price in history]
    })

@bp.route('/stocks', methods=['POST'])
def create_stock():
    """创建股票记录"""
    data = request.get_json()
    
    if not data or not data.get('symbol'):
        return jsonify({'error': _('Stock symbol is required')}), 400
    
    symbol = data['symbol'].upper()
    
    # 检查是否已存在
    existing_stock = Stock.query.filter_by(symbol=symbol).first()
    if existing_stock:
        return jsonify({'error': _('Stock already exists')}), 400
    
    stock = Stock(
        symbol=symbol,
        name=data.get('name', symbol),
        exchange=data.get('exchange'),
        currency=data.get('currency', 'USD'),
        category_id=data.get('category_id'),
        sector=data.get('sector'),
        market_cap=data.get('market_cap')
    )
    
    db.session.add(stock)
    db.session.commit()
    
    return jsonify({
        'message': _('Stock created successfully'),
        'stock': stock.to_dict()
    }), 201

@bp.route('/stocks/<symbol>', methods=['PUT'])
def update_stock(symbol):
    """更新股票信息"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 更新字段
    if 'name' in data:
        stock.name = data['name']
    if 'exchange' in data:
        stock.exchange = data['exchange']
    if 'currency' in data:
        stock.currency = data['currency']
    if 'category_id' in data:
        if data['category_id']:
            category = StockCategory.query.get(data['category_id'])
            if not category:
                return jsonify({'error': _('Invalid category')}), 400
            stock.category_id = data['category_id']
        else:
            stock.category_id = None
    if 'sector' in data:
        stock.sector = data['sector']
    if 'market_cap' in data:
        stock.market_cap = data['market_cap']
    if 'is_active' in data:
        stock.is_active = bool(data['is_active'])
    
    db.session.commit()
    
    return jsonify({
        'message': _('Stock updated successfully'),
        'stock': stock.to_dict()
    })

@bp.route('/stocks/<symbol>/categories', methods=['POST'])
def add_stock_category(symbol):
    """为股票添加分类"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    data = request.get_json()
    category_id = data.get('category_id')
    
    if not category_id:
        return jsonify({'error': _('Category ID is required')}), 400
    
    category = StockCategory.query.get(category_id)
    if not category:
        return jsonify({'error': _('Category not found')}), 404
    
    stock.category_id = category_id
    db.session.commit()
    
    return jsonify({
        'message': _('Category added successfully'),
        'stock': stock.to_dict()
    })

@bp.route('/stocks/<symbol>/categories', methods=['DELETE'])
def remove_stock_category(symbol):
    """移除股票分类"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    stock.category_id = None
    db.session.commit()
    
    return jsonify({
        'message': _('Category removed successfully'),
        'stock': stock.to_dict()
    })

@bp.route('/stocks/batch-categorize', methods=['POST'])
def batch_categorize_stocks():
    """批量分类股票"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    stock_symbols = data.get('stock_symbols', [])
    category_id = data.get('category_id')
    action = data.get('action', 'add')  # add/remove
    
    if not stock_symbols:
        return jsonify({'error': _('Stock symbols required')}), 400
    
    if action == 'add' and not category_id:
        return jsonify({'error': _('Category ID required for add action')}), 400
    
    if action == 'add':
        category = StockCategory.query.get(category_id)
        if not category:
            return jsonify({'error': _('Category not found')}), 404
    
    results = []
    
    for symbol in stock_symbols:
        stock = Stock.query.filter_by(symbol=symbol.upper()).first()
        if not stock:
            results.append({
                'symbol': symbol,
                'success': False,
                'error': 'Stock not found'
            })
            continue
        
        try:
            if action == 'add':
                stock.category_id = category_id
            elif action == 'remove':
                stock.category_id = None
            
            results.append({
                'symbol': symbol,
                'success': True
            })
        except Exception as e:
            results.append({
                'symbol': symbol,
                'success': False,
                'error': str(e)
            })
    
    db.session.commit()
    
    success_count = sum(1 for r in results if r['success'])
    
    return jsonify({
        'message': f'{success_count} stocks processed successfully',
        'results': results,
        'total_processed': len(stock_symbols),
        'success_count': success_count
    })

@bp.route('/stocks/<symbol>/category-suggestions', methods=['GET'])
def get_category_suggestions(symbol):
    """获取股票分类建议"""
    stock = Stock.query.filter_by(symbol=symbol.upper()).first()
    
    if not stock:
        return jsonify({'error': _('Stock not found')}), 404
    
    suggestions = []
    
    # 基于股票名称和行业的智能建议
    name_lower = (stock.name or '').lower()
    sector_lower = (stock.sector or '').lower()
    
    # 规则基础建议
    keyword_rules = {
        'Technology': ['tech', 'software', 'computer', 'digital', 'data', 'internet', 'cloud'],
        'Banking': ['bank', 'financial', 'insurance', 'trust', 'credit'],
        'Healthcare': ['health', 'medical', 'pharma', 'bio', 'drug', 'hospital'],
        'Energy': ['oil', 'gas', 'energy', 'petroleum', 'renewable', 'solar'],
        'REITs': ['reit', 'real estate', 'property', 'trust'],
        'Index Funds': ['etf', 'fund', 'index', 'spdr', 'ishares', 'vanguard']
    }
    
    for category_name, keywords in keyword_rules.items():
        confidence = 0
        matched_keywords = []
        
        for keyword in keywords:
            if keyword in name_lower or keyword in sector_lower:
                confidence += 0.15
                matched_keywords.append(keyword)
        
        if confidence > 0:
            # 查找对应的分类
            category = StockCategory.query.filter(
                StockCategory.name.ilike(f'%{category_name}%')
            ).first()
            
            if category:
                suggestions.append({
                    'id': category.id,
                    'name': category.get_localized_name(),
                    'color': category.color,
                    'icon': category.icon,
                    'confidence': min(confidence, 1.0),
                    'matched_keywords': matched_keywords
                })
    
    # 按置信度排序
    suggestions.sort(key=lambda x: x['confidence'], reverse=True)
    
    return jsonify({
        'symbol': stock.symbol,
        'stock_name': stock.name,
        'sector': stock.sector,
        'suggestions': suggestions[:5]  # 返回前5个建议
    })