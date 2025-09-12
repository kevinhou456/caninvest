#!/usr/bin/env python3
"""
数据一致性校验API端点
"""

from flask import jsonify, request
from app.api import bp
from app.services.asset_valuation_service import AssetValuationService
from app.services.data_change_listener import get_data_change_listener
from app.models.family import Family
from app.models.account import Account
from app.models.member import Member
import logging

logger = logging.getLogger(__name__)


@bp.route('/validation/data-consistency', methods=['GET'])
def validate_data_consistency():
    """
    验证数据一致性
    检查所有账户的数据一致性
    """
    try:
        # 获取查询参数
        account_id = request.args.get('account_id', type=int)
        family_id = request.args.get('family_id', type=int)
        
        asset_service = AssetValuationService()
        
        results = {}
        total_errors = 0
        
        # 如果指定了账户ID，只检查该账户
        if account_id:
            account = Account.query.get(account_id)
            if not account:
                return jsonify({
                    'success': False,
                    'error': f'账户 {account_id} 不存在'
                }), 404
            
            errors = asset_service.validate_data_consistency(account_id)
            results[f'account_{account_id}'] = {
                'account_name': account.name,
                'errors': errors,
                'error_count': len(errors)
            }
            total_errors += len(errors)
            
        else:
            # 检查所有账户或指定家庭的账户
            if family_id:
                family = Family.query.get(family_id)
                if not family:
                    return jsonify({
                        'success': False,
                        'error': f'家庭 {family_id} 不存在'
                    }), 404
                accounts = Account.query.filter_by(family_id=family_id).all()
            else:
                # 默认检查第一个家庭的所有账户
                family = Family.query.first()
                if not family:
                    return jsonify({
                        'success': False,
                        'error': '未找到任何家庭'
                    }), 404
                accounts = Account.query.filter_by(family_id=family.id).all()
            
            for account in accounts:
                errors = asset_service.validate_data_consistency(account.id)
                results[f'account_{account.id}'] = {
                    'account_name': account.name,
                    'errors': errors,
                    'error_count': len(errors)
                }
                total_errors += len(errors)
        
        # 准备返回结果
        response_data = {
            'success': True,
            'total_errors': total_errors,
            'is_consistent': total_errors == 0,
            'validation_results': results,
            'summary': {
                'total_accounts_checked': len(results),
                'accounts_with_errors': len([r for r in results.values() if r['error_count'] > 0]),
                'accounts_consistent': len([r for r in results.values() if r['error_count'] == 0])
            }
        }
        
        if total_errors > 0:
            response_data['message'] = f'发现 {total_errors} 个数据一致性问题'
        else:
            response_data['message'] = '所有数据一致性检查通过'
        
        logger.info(f"数据一致性校验完成: {len(results)}个账户, {total_errors}个错误")
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"数据一致性校验失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'数据一致性校验失败: {str(e)}'
        }), 500


@bp.route('/validation/asset-snapshot/<int:account_id>', methods=['GET'])
def get_asset_snapshot(account_id):
    """
    获取账户资产快照（用于调试和验证）
    """
    try:
        account = Account.query.get(account_id)
        if not account:
            return jsonify({
                'success': False,
                'error': f'账户 {account_id} 不存在'
            }), 404
        
        # 获取日期参数
        target_date = request.args.get('date')
        if target_date:
            from datetime import datetime
            try:
                target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({
                    'success': False,
                    'error': '日期格式错误，请使用 YYYY-MM-DD 格式'
                }), 400
        
        asset_service = AssetValuationService()
        snapshot = asset_service.get_asset_snapshot(account_id, target_date)
        
        return jsonify({
            'success': True,
            'account_name': account.name,
            'snapshot': snapshot.to_dict()
        })
        
    except Exception as e:
        logger.error(f"获取资产快照失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'获取资产快照失败: {str(e)}'
        }), 500


@bp.route('/validation/cache-stats', methods=['GET'])
def get_cache_stats():
    """
    获取缓存统计信息
    """
    try:
        listener = get_data_change_listener()
        stats = listener.get_cache_stats()
        
        return jsonify({
            'success': True,
            'cache_stats': stats
        })
        
    except Exception as e:
        logger.error(f"获取缓存统计失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'获取缓存统计失败: {str(e)}'
        }), 500


@bp.route('/validation/cache-invalidate', methods=['POST'])
def invalidate_cache():
    """
    手动失效缓存
    """
    try:
        data = request.get_json() or {}
        account_id = data.get('account_id')
        from_date = data.get('from_date')
        clear_all = data.get('clear_all', False)
        
        listener = get_data_change_listener()
        
        if clear_all:
            listener.manual_invalidate_all_cache()
            message = '已清除所有缓存'
        elif account_id:
            from datetime import datetime
            if from_date:
                try:
                    from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({
                        'success': False,
                        'error': '日期格式错误，请使用 YYYY-MM-DD 格式'
                    }), 400
            
            listener.manual_invalidate_account(account_id, from_date)
            message = f'已失效账户 {account_id} 的缓存'
            if from_date:
                message += f'（起始日期: {from_date}）'
        else:
            return jsonify({
                'success': False,
                'error': '请指定 account_id 或设置 clear_all=true'
            }), 400
        
        return jsonify({
            'success': True,
            'message': message
        })
        
    except Exception as e:
        logger.error(f"缓存失效失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'缓存失效失败: {str(e)}'
        }), 500


@bp.route('/validation/reports/<int:account_id>', methods=['GET'])
def get_validation_reports(account_id):
    """
    获取账户的各种报表用于验证
    """
    try:
        account = Account.query.get(account_id)
        if not account:
            return jsonify({
                'success': False,
                'error': f'账户 {account_id} 不存在'
            }), 404
        
        from app.services.report_service import ReportService
        report_service = ReportService()
        
        # 获取各种报表
        try:
            daily_report = report_service.get_daily_report(account_id)
        except Exception as e:
            daily_report = {'error': str(e)}
        
        try:
            last_30_days = report_service.get_last_n_days_report(account_id, 30)
        except Exception as e:
            last_30_days = {'error': str(e)}
        
        try:
            monthly_report = report_service.get_monthly_report(
                account_id, 
                request.args.get('year', type=int, default=2025),
                request.args.get('month', type=int, default=1)
            )
        except Exception as e:
            monthly_report = {'error': str(e)}
        
        return jsonify({
            'success': True,
            'account_name': account.name,
            'reports': {
                'daily': daily_report,
                'last_30_days': last_30_days,
                'monthly': monthly_report
            }
        })
        
    except Exception as e:
        logger.error(f"获取验证报表失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'获取验证报表失败: {str(e)}'
        }), 500


@bp.route('/validation/available-periods/<int:account_id>', methods=['GET'])
def get_available_periods(account_id):
    """
    获取账户的可用报表周期
    """
    try:
        account = Account.query.get(account_id)
        if not account:
            return jsonify({
                'success': False,
                'error': f'账户 {account_id} 不存在'
            }), 404
        
        from app.services.report_service import ReportService
        report_service = ReportService()
        
        periods = report_service.get_available_periods(account_id)
        
        return jsonify({
            'success': True,
            'account_name': account.name,
            'available_periods': periods
        })
        
    except Exception as e:
        logger.error(f"获取可用周期失败: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'获取可用周期失败: {str(e)}'
        }), 500