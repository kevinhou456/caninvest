"""
股票分类管理API
"""

from flask import request, jsonify
from flask_babel import _
from app import db
from app.models.stock import StockCategory, StockCategoryI18n
from . import bp

@bp.route('/stock-categories', methods=['GET'])
def get_stock_categories():
    """获取所有股票分类"""
    include_translations = request.args.get('include_translations', 'false').lower() == 'true'
    
    categories = StockCategory.query.filter_by(is_active=True).order_by(
        StockCategory.sort_order, StockCategory.name
    ).all()
    
    return jsonify({
        'categories': [cat.to_dict(include_translations=include_translations) for cat in categories]
    })

@bp.route('/stock-categories', methods=['POST'])
def create_stock_category():
    """创建股票分类"""
    data = request.get_json()
    
    if not data or not data.get('name'):
        return jsonify({'error': _('Category name is required')}), 400
    
    # 检查名称是否已存在
    existing = StockCategory.query.filter_by(name=data['name'], is_active=True).first()
    if existing:
        return jsonify({'error': _('Category name already exists')}), 400
    
    category = StockCategory(
        name=data['name'],
        description=data.get('description'),
        parent_id=data.get('parent_id'),
        color=data.get('color', '#4A90E2'),
        icon=data.get('icon', 'fas fa-tag'),
        is_system=False,
        sort_order=data.get('sort_order', 0),
        created_by=data.get('created_by')
    )
    
    db.session.add(category)
    db.session.flush()  # 获取分类ID
    
    # 添加翻译（如果提供）
    translations = data.get('translations', {})
    for lang_code, translation in translations.items():
        if lang_code in ['en', 'zh_CN'] and translation.get('name'):
            category_i18n = StockCategoryI18n(
                category_id=category.id,
                language_code=lang_code,
                name=translation['name'],
                description=translation.get('description')
            )
            db.session.add(category_i18n)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Category created successfully'),
        'category': category.to_dict(include_translations=True)
    }), 201

@bp.route('/stock-categories/<int:category_id>', methods=['GET'])
def get_stock_category(category_id):
    """获取特定分类"""
    category = StockCategory.query.get_or_404(category_id)
    
    result = category.to_dict(include_translations=True)
    result['stocks'] = [stock.to_dict() for stock in category.stocks.filter_by(is_active=True).all()]
    
    return jsonify(result)

@bp.route('/stock-categories/<int:category_id>', methods=['PUT'])
def update_stock_category(category_id):
    """更新股票分类"""
    category = StockCategory.query.get_or_404(category_id)
    data = request.get_json()
    
    if not data:
        return jsonify({'error': _('No data provided')}), 400
    
    # 更新基本信息
    if 'name' in data:
        # 检查名称冲突
        existing = StockCategory.query.filter(
            StockCategory.name == data['name'],
            StockCategory.id != category_id,
            StockCategory.is_active == True
        ).first()
        if existing:
            return jsonify({'error': _('Category name already exists')}), 400
        category.name = data['name']
    
    if 'description' in data:
        category.description = data['description']
    if 'parent_id' in data:
        category.parent_id = data['parent_id']
    if 'color' in data:
        category.color = data['color']
    if 'icon' in data:
        category.icon = data['icon']
    if 'sort_order' in data:
        category.sort_order = data['sort_order']
    if 'is_active' in data:
        category.is_active = data['is_active']
    
    # 更新翻译
    if 'translations' in data:
        for lang_code, translation in data['translations'].items():
            if lang_code not in ['en', 'zh_CN']:
                continue
                
            existing_translation = StockCategoryI18n.query.filter_by(
                category_id=category_id,
                language_code=lang_code
            ).first()
            
            if existing_translation:
                if translation.get('name'):
                    existing_translation.name = translation['name']
                    existing_translation.description = translation.get('description')
                else:
                    # 如果名称为空，删除翻译
                    db.session.delete(existing_translation)
            else:
                if translation.get('name'):
                    new_translation = StockCategoryI18n(
                        category_id=category_id,
                        language_code=lang_code,
                        name=translation['name'],
                        description=translation.get('description')
                    )
                    db.session.add(new_translation)
    
    db.session.commit()
    
    return jsonify({
        'message': _('Category updated successfully'),
        'category': category.to_dict(include_translations=True)
    })

@bp.route('/stock-categories/<int:category_id>', methods=['DELETE'])
def delete_stock_category(category_id):
    """删除股票分类"""
    category = StockCategory.query.get_or_404(category_id)
    
    # 检查是否有关联的股票
    if category.stocks.count() > 0:
        return jsonify({
            'error': _('Cannot delete category with associated stocks')
        }), 400
    
    # 检查是否有子分类
    if category.children.count() > 0:
        return jsonify({
            'error': _('Cannot delete category with subcategories')
        }), 400
    
    db.session.delete(category)
    db.session.commit()
    
    return jsonify({
        'message': _('Category deleted successfully')
    })

@bp.route('/stock-categories/batch', methods=['POST'])
def batch_create_categories():
    """批量创建分类"""
    data = request.get_json()
    
    if not data or 'categories' not in data:
        return jsonify({'error': _('Categories data required')}), 400
    
    categories_data = data['categories']
    if not isinstance(categories_data, list):
        return jsonify({'error': _('Categories must be a list')}), 400
    
    created_categories = []
    errors = []
    
    for i, cat_data in enumerate(categories_data):
        try:
            if not cat_data.get('name'):
                errors.append(f'Item {i}: Category name is required')
                continue
            
            # 检查名称冲突
            existing = StockCategory.query.filter_by(name=cat_data['name'], is_active=True).first()
            if existing:
                errors.append(f'Item {i}: Category "{cat_data["name"]}" already exists')
                continue
            
            category = StockCategory(
                name=cat_data['name'],
                description=cat_data.get('description'),
                color=cat_data.get('color', '#4A90E2'),
                icon=cat_data.get('icon', 'fas fa-tag'),
                is_system=False,
                sort_order=cat_data.get('sort_order', 0)
            )
            
            db.session.add(category)
            db.session.flush()  # 获取ID
            
            # 添加翻译
            translations = cat_data.get('translations', {})
            for lang_code, translation in translations.items():
                if lang_code in ['en', 'zh_CN'] and translation.get('name'):
                    category_i18n = StockCategoryI18n(
                        category_id=category.id,
                        language_code=lang_code,
                        name=translation['name'],
                        description=translation.get('description')
                    )
                    db.session.add(category_i18n)
            
            created_categories.append(category)
            
        except Exception as e:
            errors.append(f'Item {i}: {str(e)}')
    
    if created_categories:
        db.session.commit()
    
    return jsonify({
        'message': f'{len(created_categories)} categories created successfully',
        'created_count': len(created_categories),
        'created_categories': [cat.to_dict() for cat in created_categories],
        'errors': errors
    })

@bp.route('/stock-categories/templates', methods=['GET'])
def get_category_templates():
    """获取分类模板"""
    templates = {
        'basic_investment': {
            'name': _('Basic Investment Categories'),
            'categories': [
                {
                    'name': 'Large Cap',
                    'color': '#4A90E2',
                    'icon': 'fas fa-chart-line',
                    'translations': {
                        'en': {'name': 'Large Cap Stocks', 'description': 'Large capitalization stocks'},
                        'zh_CN': {'name': '大盘股', 'description': '大市值股票'}
                    }
                },
                {
                    'name': 'Small Cap',
                    'color': '#7ED321',
                    'icon': 'fas fa-seedling',
                    'translations': {
                        'en': {'name': 'Small Cap Stocks', 'description': 'Small capitalization stocks'},
                        'zh_CN': {'name': '小盘股', 'description': '小市值股票'}
                    }
                },
                {
                    'name': 'Value',
                    'color': '#F5A623',
                    'icon': 'fas fa-gem',
                    'translations': {
                        'en': {'name': 'Value Stocks', 'description': 'Undervalued stocks'},
                        'zh_CN': {'name': '价值股', 'description': '被低估的股票'}
                    }
                },
                {
                    'name': 'Growth',
                    'color': '#D0021B',
                    'icon': 'fas fa-rocket',
                    'translations': {
                        'en': {'name': 'Growth Stocks', 'description': 'High growth potential stocks'},
                        'zh_CN': {'name': '成长股', 'description': '高增长潜力股票'}
                    }
                }
            ]
        },
        'sector_based': {
            'name': _('Sector-Based Categories'),
            'categories': [
                {
                    'name': 'Technology',
                    'color': '#4A90E2',
                    'icon': 'fas fa-microchip',
                    'translations': {
                        'en': {'name': 'Technology', 'description': 'Technology sector stocks'},
                        'zh_CN': {'name': '科技股', 'description': '科技行业股票'}
                    }
                },
                {
                    'name': 'Banking',
                    'color': '#7ED321',
                    'icon': 'fas fa-university',
                    'translations': {
                        'en': {'name': 'Banking & Financial', 'description': 'Banking and financial services'},
                        'zh_CN': {'name': '银行金融', 'description': '银行和金融服务'}
                    }
                },
                {
                    'name': 'Healthcare',
                    'color': '#BD10E0',
                    'icon': 'fas fa-heartbeat',
                    'translations': {
                        'en': {'name': 'Healthcare', 'description': 'Healthcare and pharmaceutical'},
                        'zh_CN': {'name': '医疗保健', 'description': '医疗保健和制药'}
                    }
                },
                {
                    'name': 'Energy',
                    'color': '#D0021B',
                    'icon': 'fas fa-fire',
                    'translations': {
                        'en': {'name': 'Energy', 'description': 'Energy sector including oil and renewables'},
                        'zh_CN': {'name': '能源', 'description': '能源行业包括石油和可再生能源'}
                    }
                }
            ]
        }
    }
    
    return jsonify({'templates': templates})

@bp.route('/stock-categories/apply-template', methods=['POST'])
def apply_category_template():
    """应用分类模板"""
    data = request.get_json()
    
    if not data or 'template_name' not in data:
        return jsonify({'error': _('Template name required')}), 400
    
    template_name = data['template_name']
    
    # 获取模板数据
    templates_response = get_category_templates()
    templates_data = templates_response.get_json()['templates']
    
    if template_name not in templates_data:
        return jsonify({'error': _('Template not found')}), 404
    
    template = templates_data[template_name]
    created_categories = []
    errors = []
    
    for cat_data in template['categories']:
        try:
            # 检查名称冲突
            existing = StockCategory.query.filter_by(name=cat_data['name'], is_active=True).first()
            if existing:
                errors.append(f'Category "{cat_data["name"]}" already exists')
                continue
            
            category = StockCategory(
                name=cat_data['name'],
                color=cat_data['color'],
                icon=cat_data['icon'],
                is_system=True,
                sort_order=len(created_categories)
            )
            
            db.session.add(category)
            db.session.flush()
            
            # 添加翻译
            for lang_code, translation in cat_data['translations'].items():
                category_i18n = StockCategoryI18n(
                    category_id=category.id,
                    language_code=lang_code,
                    name=translation['name'],
                    description=translation['description']
                )
                db.session.add(category_i18n)
            
            created_categories.append(category)
            
        except Exception as e:
            errors.append(f'Failed to create "{cat_data["name"]}": {str(e)}')
    
    if created_categories:
        db.session.commit()
    
    return jsonify({
        'message': f'Template "{template["name"]}" applied successfully',
        'created_count': len(created_categories),
        'created_categories': [cat.to_dict() for cat in created_categories],
        'errors': errors
    })