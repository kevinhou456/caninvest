#!/usr/bin/env python3
"""
Daily Stats API - 日统计月历视图API接口

提供月历视图所需的日统计数据API，支持：
1. 月历数据获取（当前月/指定月）
2. 单日浮动盈亏详情
3. 缓存统计和管理
4. 高性能的批量数据获取
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime, date
from typing import Dict, List, Optional
import logging

from decimal import Decimal, InvalidOperation

from app.services.daily_stats_service import daily_stats_service
from app.services.daily_stats_cache_service import daily_stats_cache_service
from app.models.account import Account, AccountMember
from app.models.family import Family
from app.models.member import Member

logger = logging.getLogger(__name__)

def get_family_account_ids(family_id: int) -> List[int]:
    """获取指定家庭的所有账户ID列表"""
    accounts = Account.query.filter_by(family_id=family_id).all()
    return [account.id for account in accounts]

def get_member_account_ids(member_id: int) -> List[int]:
    """获取指定成员的所有账户ID列表"""
    member = Member.query.get(member_id)
    if not member:
        return []
    
    # 使用成员的get_accounts方法获取账户信息
    accounts = member.get_accounts()
    return [account['id'] for account in accounts if 'id' in account]


def get_member_ownership_map(member_id: int) -> Dict[int, Decimal]:
    """获取成员各账户的持股比例映射"""
    ownership_map: Dict[int, Decimal] = {}
    memberships = AccountMember.query.filter_by(member_id=member_id).all()

    for membership in memberships:
        try:
            percentage = Decimal(str(membership.ownership_percentage or 0))
            ownership_map[membership.account_id] = percentage / Decimal('100')
        except (InvalidOperation, TypeError):
            ownership_map[membership.account_id] = Decimal('0')

    return ownership_map

# 创建蓝图
daily_stats_bp = Blueprint('daily_stats', __name__, url_prefix='/api/v1/daily-stats')


@daily_stats_bp.route('/calendar', methods=['GET'])
def get_monthly_calendar():
    """
    获取月历数据
    
    Query Parameters:
    - year: 年份（可选，默认当前年）
    - month: 月份（可选，默认当前月）
    - account_ids: 账户ID列表（可选，默认用户所有账户）
    
    Returns:
    {
        "success": true,
        "data": {
            "year": 2024,
            "month": 1,
            "account_ids": [1, 2],
            "daily_stats": {
                "2024-01-01": {
                    "date": "2024-01-01",
                    "total_assets": 100000.0,
                    "daily_change": 1500.0,
                    "daily_return_pct": 1.52,
                    "is_trading_day": false,
                    "has_transactions": false
                }
            },
            "month_summary": {
                "start_assets": 98500.0,
                "end_assets": 102000.0,
                "total_change": 3500.0,
                "return_pct": 3.55,
                "trading_days_count": 21,
                "transaction_days_count": 5
            }
        }
    }
    """
    try:
        # 解析参数
        year = request.args.get('year', type=int, default=date.today().year)
        month = request.args.get('month', type=int, default=date.today().month)
        
        # 获取账户ID列表
        account_ids_param = request.args.get('account_ids')
        member_id_param = request.args.get('member_id', type=int)
        
        ownership_map = None

        if account_ids_param:
            try:
                account_ids = [int(id.strip()) for id in account_ids_param.split(',') if id.strip()]
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '账户ID格式无效',
                    'message': 'account_ids参数必须是逗号分隔的数字列表'
                }), 400
        elif member_id_param:
            # 使用指定成员的账户
            account_ids = get_member_account_ids(member_id_param)
            ownership_map = get_member_ownership_map(member_id_param)
            if not account_ids:
                return jsonify({
                    'success': False,
                    'error': '没有找到成员或成员没有账户',
                    'message': f'成员ID {member_id_param} 不存在或没有关联账户'
                }), 400
        else:
            # 使用第一个家庭的所有账户
            family = Family.query.first()
            if not family:
                return jsonify({
                    'success': False,
                    'error': '没有找到家庭数据',
                    'message': '请先创建家庭数据'
                }), 400
            account_ids = get_family_account_ids(family.id)
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': '没有找到可用的账户',
                'message': '请确保用户拥有至少一个账户'
            }), 400
        
        # 验证日期参数
        if not (1 <= month <= 12):
            return jsonify({
                'success': False,
                'error': '月份参数无效',
                'message': '月份必须在1-12之间'
            }), 400
        
        if year < 2000 or year > 2100:
            return jsonify({
                'success': False,
                'error': '年份参数无效',
                'message': '年份必须在2000-2100之间'
            }), 400
        
        logger.info(f"获取月历数据: {year}-{month}, 账户: {account_ids}")
        
        # 获取月历数据
        calendar_data = daily_stats_service.get_monthly_calendar_data(
            account_ids, year, month, ownership_map
        )
        
        return jsonify({
            'success': True,
            'data': calendar_data.to_dict(),
            'meta': {
                'request_time': datetime.utcnow().isoformat(),
                'cache_info': daily_stats_cache_service.get_cache_statistics()
            }
        })
        
    except Exception as e:
        logger.error(f"获取月历数据失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '获取月历数据失败',
            'message': str(e)
        }), 500


@daily_stats_bp.route('/calendar/current', methods=['GET'])
def get_current_month_calendar():
    """
    获取当前月份的月历数据（快捷接口）
    
    Query Parameters:
    - account_ids: 账户ID列表（可选，默认用户所有账户）
    
    Returns:
        与 /calendar 相同的格式
    """
    try:
        # 获取账户ID列表
        account_ids_param = request.args.get('account_ids')
        member_id_param = request.args.get('member_id', type=int)
        
        if account_ids_param:
            try:
                account_ids = [int(id.strip()) for id in account_ids_param.split(',') if id.strip()]
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '账户ID格式无效'
                }), 400
        elif member_id_param:
            # 使用指定成员的账户
            account_ids = get_member_account_ids(member_id_param)
            ownership_map = get_member_ownership_map(member_id_param)
            if not account_ids:
                return jsonify({
                    'success': False,
                    'error': '没有找到成员或成员没有账户',
                    'message': f'成员ID {member_id_param} 不存在或没有关联账户'
                }), 400
        else:
            family = Family.query.first()
            if not family:
                return jsonify({
                    'success': False,
                    'error': '没有找到家庭数据',
                    'message': '请先创建家庭数据'
                }), 400
            account_ids = get_family_account_ids(family.id)
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': '没有找到可用的账户'
            }), 400
        
        logger.info(f"获取当前月历数据，账户: {account_ids}")
        
        # 获取当前月份数据
        calendar_data = daily_stats_service.get_current_month_calendar(account_ids, ownership_map)
        
        return jsonify({
            'success': True,
            'data': calendar_data.to_dict(),
            'meta': {
                'request_time': datetime.utcnow().isoformat(),
                'is_current_month': True
            }
        })
        
    except Exception as e:
        logger.error(f"获取当前月历数据失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '获取当前月历数据失败',
            'message': str(e)
        }), 500


@daily_stats_bp.route('/day/<date_str>', methods=['GET'])
def get_daily_floating_pnl(date_str: str):
    """
    获取指定日期的浮动盈亏详情
    
    Path Parameters:
    - date_str: 日期字符串 (YYYY-MM-DD)
    
    Query Parameters:
    - account_ids: 账户ID列表（可选，默认用户所有账户）
    
    Returns:
    {
        "success": true,
        "data": {
            "date": "2024-01-15",
            "account_ids": [1, 2],
            "floating_pnl": {
                "total_assets": 105000.0,
                "unrealized_gain": 8500.0,
                "daily_change": 750.0,
                "daily_return_pct": 0.72
            },
            "current": {
                "total_assets": 105000.0,
                "stock_market_value": 95000.0,
                "cash_balance": 10000.0
            },
            "previous": {
                "total_assets": 104250.0,
                "stock_market_value": 94500.0,
                "cash_balance": 9750.0
            },
            "change": {
                "total_assets_change": 750.0,
                "total_assets_change_pct": 0.72,
                "stock_market_value_change": 500.0,
                "cash_balance_change": 250.0
            }
        }
    }
    """
    try:
        # 解析日期
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({
                'success': False,
                'error': '日期格式无效',
                'message': '日期格式必须为 YYYY-MM-DD'
            }), 400
        
        # 获取账户ID列表
        account_ids_param = request.args.get('account_ids')
        member_id_param = request.args.get('member_id', type=int)

        ownership_map = None

        if account_ids_param:
            try:
                account_ids = [int(id.strip()) for id in account_ids_param.split(',') if id.strip()]
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '账户ID格式无效'
                }), 400
            if member_id_param:
                ownership_map = get_member_ownership_map(member_id_param)
        else:
            if member_id_param:
                account_ids = get_member_account_ids(member_id_param)
                ownership_map = get_member_ownership_map(member_id_param)
            else:
                family = Family.query.first()
                if not family:
                    return jsonify({
                        'success': False,
                        'error': '没有找到家庭数据',
                        'message': '请先创建家庭数据'
                    }), 400
                account_ids = get_family_account_ids(family.id)
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': '没有找到可用的账户'
            }), 400
        
        logger.info(f"获取{target_date}的浮动盈亏，账户: {account_ids}")
        
        # 获取浮动盈亏详情
        pnl_data = daily_stats_service.get_daily_floating_pnl(account_ids, target_date, ownership_map)
        
        return jsonify({
            'success': True,
            'data': pnl_data,
            'meta': {
                'request_time': datetime.utcnow().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"获取浮动盈亏失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '获取浮动盈亏失败',
            'message': str(e)
        }), 500


@daily_stats_bp.route('/calendar/summary', methods=['GET'])
def get_calendar_summary():
    """
    获取月历汇总统计信息
    
    Query Parameters:
    - year: 年份（可选，默认当前年）
    - month: 月份（可选，默认当前月）
    - account_ids: 账户ID列表（可选，默认用户所有账户）
    
    Returns:
    {
        "success": true,
        "data": {
            "calendar_data": {...},  // 完整月历数据
            "statistics": {
                "positive_days": 15,
                "negative_days": 6,
                "total_trading_days": 21,
                "transaction_days": 8,
                "win_rate": 71.43,
                "avg_daily_change": 125.50,
                "max_daily_gain": 2500.0,
                "max_daily_loss": -800.0
            }
        }
    }
    """
    try:
        # 解析参数
        year = request.args.get('year', type=int, default=date.today().year)
        month = request.args.get('month', type=int, default=date.today().month)
        
        # 获取账户ID列表
        account_ids_param = request.args.get('account_ids')
        member_id_param = request.args.get('member_id', type=int)

        ownership_map = None

        if account_ids_param:
            try:
                account_ids = [int(id.strip()) for id in account_ids_param.split(',') if id.strip()]
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '账户ID格式无效'
                }), 400
            if member_id_param:
                ownership_map = get_member_ownership_map(member_id_param)
        else:
            if member_id_param:
                account_ids = get_member_account_ids(member_id_param)
                ownership_map = get_member_ownership_map(member_id_param)
            else:
                family = Family.query.first()
                if not family:
                    return jsonify({
                        'success': False,
                        'error': '没有找到家庭数据',
                        'message': '请先创建家庭数据'
                    }), 400
                account_ids = get_family_account_ids(family.id)
        
        if not account_ids:
            return jsonify({
                'success': False,
                'error': '没有找到可用的账户'
            }), 400
        
        # 验证参数
        if not (1 <= month <= 12):
            return jsonify({
                'success': False,
                'error': '月份参数无效'
            }), 400
        
        logger.info(f"获取月历汇总统计: {year}-{month}, 账户: {account_ids}")
        
        # 获取汇总统计
        summary_data = daily_stats_service.get_calendar_summary_stats(account_ids, year, month, ownership_map)
        
        return jsonify({
            'success': True,
            'data': summary_data,
            'meta': {
                'request_time': datetime.utcnow().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"获取月历汇总统计失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '获取月历汇总统计失败',
            'message': str(e)
        }), 500


@daily_stats_bp.route('/cache/status', methods=['GET'])
def get_cache_status():
    """
    获取缓存状态信息（用于监控和调试）
    
    Returns:
    {
        "success": true,
        "data": {
            "strategy": "balanced",
            "memory_cache": {
                "asset_snapshots_count": 150,
                "monthly_calendars_count": 3,
                "total_memory_entries": 153
            },
            "price_cache": {
                "total_records": 15000,
                "date_range": {
                    "earliest_date": "2023-01-01",
                    "latest_date": "2024-01-31"
                }
            }
        }
    }
    """
    try:
        cache_stats = daily_stats_cache_service.get_cache_statistics()
        
        return jsonify({
            'success': True,
            'data': cache_stats,
            'meta': {
                'request_time': datetime.utcnow().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"获取缓存状态失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '获取缓存状态失败',
            'message': str(e)
        }), 500


@daily_stats_bp.route('/cache/cleanup', methods=['POST'])
def cleanup_cache():
    """
    清理过期缓存（管理接口）
    
    Returns:
    {
        "success": true,
        "message": "缓存清理完成"
    }
    """
    try:
        # 暂时跳过权限检查，允许所有用户清理缓存
        
        logger.info("执行缓存清理")
        
        # 执行缓存清理
        daily_stats_cache_service.cleanup_expired_cache()
        
        return jsonify({
            'success': True,
            'message': '缓存清理完成',
            'meta': {
                'request_time': datetime.utcnow().isoformat()
            }
        })
        
    except Exception as e:
        logger.error(f"缓存清理失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': '缓存清理失败',
            'message': str(e)
        }), 500


@daily_stats_bp.errorhandler(404)
def not_found(error):
    """404错误处理"""
    return jsonify({
        'success': False,
        'error': '接口不存在',
        'message': '请检查API路径是否正确'
    }), 404


@daily_stats_bp.errorhandler(500)
def internal_error(error):
    """500错误处理"""
    logger.error(f"Daily Stats API内部错误: {error}", exc_info=True)
    return jsonify({
        'success': False,
        'error': '服务器内部错误',
        'message': '请稍后重试或联系管理员'
    }), 500
