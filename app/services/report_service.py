#!/usr/bin/env python3
"""
统一报表服务
负责生成各种时间周期的资产报表和分析数据
支持日、月、季度、年度报表
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from decimal import Decimal
import logging
from calendar import monthrange

from app.services.asset_valuation_service import AssetValuationService, AssetSnapshot

logger = logging.getLogger(__name__)


class ReportService:
    """统一报表服务 - 提供各种时间维度的资产分析报表"""
    
    def __init__(self):
        self.asset_service = AssetValuationService()
        
    def get_daily_report(self, account_id: int, target_date: Optional[date] = None) -> Dict:
        """
        获取日度报表
        
        Args:
            account_id: 账户ID
            target_date: 目标日期，默认为今天
            
        Returns:
            包含当日资产快照和与前一日对比的报表
        """
        if target_date is None:
            target_date = date.today()
            
        logger.info(f"生成账户{account_id}的{target_date}日度报表")
        
        # 获取当日快照
        current_snapshot = self.asset_service.get_asset_snapshot(account_id, target_date)
        
        # 获取前一日快照（排除周末）
        previous_date = self._get_previous_business_day(target_date)
        previous_snapshot = self.asset_service.get_asset_snapshot(account_id, previous_date)
        
        # 计算日度变化
        daily_change = self._calculate_change(current_snapshot, previous_snapshot)
        
        return {
            'type': 'daily',
            'date': target_date.isoformat(),
            'account_id': account_id,
            'current': current_snapshot.to_dict(),
            'previous': previous_snapshot.to_dict(),
            'change': daily_change,
            'period_label': f"{target_date.strftime('%Y-%m-%d')} 日报"
        }
    
    def get_monthly_report(self, account_id: int, year: int, month: int) -> Dict:
        """
        获取月度报表
        
        Args:
            account_id: 账户ID
            year: 年份
            month: 月份
            
        Returns:
            包含月末资产快照和月度变化的报表
        """
        logger.info(f"生成账户{account_id}的{year}-{month:02d}月度报表")
        
        # 月末日期
        last_day = monthrange(year, month)[1]
        month_end = date(year, month, last_day)
        
        # 上月末日期
        if month == 1:
            prev_month_end = date(year - 1, 12, 31)
        else:
            prev_last_day = monthrange(year, month - 1)[1]
            prev_month_end = date(year, month - 1, prev_last_day)
        
        # 获取快照
        current_snapshot = self.asset_service.get_asset_snapshot(account_id, month_end)
        previous_snapshot = self.asset_service.get_asset_snapshot(account_id, prev_month_end)
        
        # 计算月度变化
        monthly_change = self._calculate_change(current_snapshot, previous_snapshot)
        
        return {
            'type': 'monthly',
            'period': f"{year}-{month:02d}",
            'account_id': account_id,
            'current': current_snapshot.to_dict(),
            'previous': previous_snapshot.to_dict(),
            'change': monthly_change,
            'period_label': f"{year}年{month}月报"
        }
    
    def get_quarterly_report(self, account_id: int, year: int, quarter: int) -> Dict:
        """
        获取季度报表
        
        Args:
            account_id: 账户ID
            year: 年份
            quarter: 季度 (1-4)
            
        Returns:
            包含季末资产快照和季度变化的报表
        """
        if quarter < 1 or quarter > 4:
            raise ValueError("季度必须在1-4之间")
            
        logger.info(f"生成账户{account_id}的{year}年Q{quarter}季度报表")
        
        # 计算季度结束日期
        quarter_end_month = quarter * 3
        last_day = monthrange(year, quarter_end_month)[1]
        quarter_end = date(year, quarter_end_month, last_day)
        
        # 计算上季度结束日期
        if quarter == 1:
            prev_quarter_end = date(year - 1, 12, 31)
        else:
            prev_quarter_end_month = (quarter - 1) * 3
            prev_last_day = monthrange(year, prev_quarter_end_month)[1]
            prev_quarter_end = date(year, prev_quarter_end_month, prev_last_day)
        
        # 获取快照
        current_snapshot = self.asset_service.get_asset_snapshot(account_id, quarter_end)
        previous_snapshot = self.asset_service.get_asset_snapshot(account_id, prev_quarter_end)
        
        # 计算季度变化
        quarterly_change = self._calculate_change(current_snapshot, previous_snapshot)
        
        return {
            'type': 'quarterly',
            'period': f"{year}-Q{quarter}",
            'account_id': account_id,
            'current': current_snapshot.to_dict(),
            'previous': previous_snapshot.to_dict(),
            'change': quarterly_change,
            'period_label': f"{year}年第{quarter}季度报"
        }
    
    def get_yearly_report(self, account_id: int, year: int) -> Dict:
        """
        获取年度报表
        
        Args:
            account_id: 账户ID
            year: 年份
            
        Returns:
            包含年末资产快照和年度变化的报表
        """
        logger.info(f"生成账户{account_id}的{year}年度报表")
        
        # 年末日期
        year_end = date(year, 12, 31)
        prev_year_end = date(year - 1, 12, 31)
        
        # 获取快照
        current_snapshot = self.asset_service.get_asset_snapshot(account_id, year_end)
        previous_snapshot = self.asset_service.get_asset_snapshot(account_id, prev_year_end)
        
        # 计算年度变化
        yearly_change = self._calculate_change(current_snapshot, previous_snapshot)
        
        return {
            'type': 'yearly',
            'period': str(year),
            'account_id': account_id,
            'current': current_snapshot.to_dict(),
            'previous': previous_snapshot.to_dict(),
            'change': yearly_change,
            'period_label': f"{year}年度报"
        }
    
    def get_last_n_days_report(self, account_id: int, days: int = 30) -> Dict:
        """
        获取最近N天报表
        
        Args:
            account_id: 账户ID
            days: 天数，默认30天
            
        Returns:
            包含最近N天的每日资产变化趋势
        """
        logger.info(f"生成账户{account_id}的最近{days}天报表")
        
        today = date.today()
        start_date = today - timedelta(days=days)
        
        # 获取当前快照和起始快照
        current_snapshot = self.asset_service.get_asset_snapshot(account_id, today)
        start_snapshot = self.asset_service.get_asset_snapshot(account_id, start_date)
        
        # 生成每日数据点（可选，用于趋势图）
        daily_points = []
        for i in range(0, days + 1, max(1, days // 10)):  # 最多10个数据点
            point_date = start_date + timedelta(days=i)
            if point_date > today:
                point_date = today
            snapshot = self.asset_service.get_asset_snapshot(account_id, point_date)
            daily_points.append({
                'date': point_date.isoformat(),
                'total_assets': float(snapshot.total_assets)
            })
        
        # 计算期间变化
        period_change = self._calculate_change(current_snapshot, start_snapshot)
        
        return {
            'type': 'period',
            'period': f"last_{days}_days",
            'account_id': account_id,
            'current': current_snapshot.to_dict(),
            'start': start_snapshot.to_dict(),
            'change': period_change,
            'daily_points': daily_points,
            'period_label': f"最近{days}天"
        }
    
    def get_portfolio_performance_report(self, account_id: int, start_date: date, end_date: date) -> Dict:
        """
        获取投资组合绩效报表
        
        Args:
            account_id: 账户ID
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            详细的投资组合绩效分析
        """
        logger.info(f"生成账户{account_id}的{start_date}至{end_date}绩效报表")
        
        start_snapshot = self.asset_service.get_asset_snapshot(account_id, start_date)
        end_snapshot = self.asset_service.get_asset_snapshot(account_id, end_date)
        
        # 计算绩效指标
        period_change = self._calculate_change(end_snapshot, start_snapshot)
        
        # 计算年化收益率
        days_diff = (end_date - start_date).days
        if days_diff > 0 and start_snapshot.total_assets > 0:
            total_return = float(period_change['total_assets_change']) / float(start_snapshot.total_assets)
            if days_diff >= 365:
                annualized_return = ((1 + total_return) ** (365.25 / days_diff)) - 1
            else:
                annualized_return = total_return * (365.25 / days_diff)
        else:
            annualized_return = 0
        
        # 股票 vs 现金配置分析
        start_stock_ratio = float(start_snapshot.stock_market_value) / float(start_snapshot.total_assets) if start_snapshot.total_assets > 0 else 0
        end_stock_ratio = float(end_snapshot.stock_market_value) / float(end_snapshot.total_assets) if end_snapshot.total_assets > 0 else 0
        
        return {
            'type': 'performance',
            'period': f"{start_date.isoformat()}_to_{end_date.isoformat()}",
            'account_id': account_id,
            'start': start_snapshot.to_dict(),
            'end': end_snapshot.to_dict(),
            'change': period_change,
            'days': days_diff,
            'annualized_return': round(annualized_return * 100, 2),  # 转换为百分比
            'total_return_pct': round((float(period_change['total_assets_change']) / float(start_snapshot.total_assets)) * 100, 2) if start_snapshot.total_assets > 0 else 0,
            'allocation': {
                'start': {
                    'stock_ratio': round(start_stock_ratio * 100, 2),
                    'cash_ratio': round((1 - start_stock_ratio) * 100, 2)
                },
                'end': {
                    'stock_ratio': round(end_stock_ratio * 100, 2),
                    'cash_ratio': round((1 - end_stock_ratio) * 100, 2)
                }
            },
            'period_label': f"{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 绩效报告"
        }
    
    def _calculate_change(self, current: AssetSnapshot, previous: AssetSnapshot) -> Dict:
        """计算两个快照之间的变化"""
        return {
            'total_assets_change': float(current.total_assets - previous.total_assets),
            'total_assets_change_pct': self._calculate_percentage_change(
                current.total_assets, previous.total_assets
            ),
            'stock_market_value_change': float(current.stock_market_value - previous.stock_market_value),
            'stock_market_value_change_pct': self._calculate_percentage_change(
                current.stock_market_value, previous.stock_market_value
            ),
            'cash_balance_change': float(current.cash_balance_total_cad - previous.cash_balance_total_cad),
            'cash_balance_change_pct': self._calculate_percentage_change(
                current.cash_balance_total_cad, previous.cash_balance_total_cad
            )
        }
    
    def _calculate_percentage_change(self, current: Decimal, previous: Decimal) -> float:
        """计算百分比变化"""
        if previous == 0:
            return 0 if current == 0 else 100
        return float((current - previous) / previous * 100)
    
    def _get_previous_business_day(self, target_date: date) -> date:
        """获取前一个工作日"""
        previous_date = target_date - timedelta(days=1)
        
        # 跳过周末
        while previous_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
            previous_date -= timedelta(days=1)
        
        return previous_date
    
    def get_available_periods(self, account_id: int) -> Dict:
        """
        获取可用的报表周期
        基于账户的交易历史确定可生成的报表范围
        
        Returns:
            可用的年份、季度、月份列表
        """
        from app.models.transaction import Transaction
        
        # 获取最早和最晚的交易日期
        earliest_transaction = Transaction.query.filter_by(
            account_id=account_id
        ).order_by(Transaction.trade_date.asc()).first()
        
        latest_transaction = Transaction.query.filter_by(
            account_id=account_id
        ).order_by(Transaction.trade_date.desc()).first()
        
        if not earliest_transaction or not latest_transaction:
            return {
                'years': [],
                'quarters': [],
                'months': [],
                'date_range': None
            }
        
        start_year = earliest_transaction.date.year
        end_year = latest_transaction.date.year
        
        # 生成可用年份
        years = list(range(start_year, end_year + 1))
        
        # 生成可用季度
        quarters = []
        for year in years:
            for quarter in range(1, 5):
                quarters.append(f"{year}-Q{quarter}")
        
        # 生成可用月份
        months = []
        current_year = start_year
        current_month = earliest_transaction.date.month
        end_month = latest_transaction.date.month if latest_transaction.date.year == end_year else 12
        
        while current_year <= end_year:
            month_end = 12 if current_year < end_year else (end_month if current_year == end_year else 12)
            for month in range(current_month, month_end + 1):
                months.append(f"{current_year}-{month:02d}")
            current_year += 1
            current_month = 1
        
        return {
            'years': years,
            'quarters': quarters,
            'months': months,
            'date_range': {
                'start': earliest_transaction.date.isoformat(),
                'end': latest_transaction.date.isoformat()
            }
        }