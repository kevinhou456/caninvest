#!/usr/bin/env python3
"""
日统计服务 - 为月历视图提供每日浮动盈亏数据
基于现有统一服务架构，确保模块化和高可维护性

设计原则:
1. 复用现有服务：最大程度利用ReportService、AssetValuationService等
2. 智能缓存：利用SmartHistoryManager优化历史数据访问
3. 增量计算：基于两个时间点的快照计算日变化
4. 可扩展性：易于添加新的日统计指标
5. 高性能：通过批量计算和缓存优化减少重复计算
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
import logging
from calendar import monthrange
from dataclasses import dataclass
from enum import Enum

from app.services.asset_valuation_service import AssetValuationService
from app.services.report_service import ReportService
from app.services.smart_history_manager import SmartHistoryManager
from app.services.daily_stats_cache_service import daily_stats_cache_service

logger = logging.getLogger(__name__)


class DailyStatsType(Enum):
    """日统计类型"""
    ASSET_VALUE = "asset_value"          # 总资产价值
    UNREALIZED_GAIN = "unrealized_gain"  # 浮动盈亏
    REALIZED_GAIN = "realized_gain"      # 已实现收益
    DAILY_CHANGE = "daily_change"        # 日变化金额
    DAILY_RETURN = "daily_return"        # 日收益率


@dataclass
class DailyStatsPoint:
    """单日统计数据点"""
    date: date
    account_id: int
    
    # 基础数据
    total_assets: Decimal = Decimal('0')
    stock_market_value: Decimal = Decimal('0')
    cash_balance: Decimal = Decimal('0')
    
    # 盈亏数据
    unrealized_gain: Decimal = Decimal('0')
    realized_gain: Decimal = Decimal('0')
    total_return: Decimal = Decimal('0')
    
    # 变化数据（相对于前一个交易日）
    daily_change: Decimal = Decimal('0')
    daily_return_pct: Decimal = Decimal('0')
    
    # 是否为交易日
    is_trading_day: bool = False
    has_transactions: bool = False
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'date': self.date.isoformat(),
            'account_id': self.account_id,
            'total_assets': float(self.total_assets),
            'stock_market_value': float(self.stock_market_value),
            'cash_balance': float(self.cash_balance),
            'unrealized_gain': float(self.unrealized_gain),
            'realized_gain': float(self.realized_gain),
            'total_return': float(self.total_return),
            'daily_change': float(self.daily_change),
            'daily_return_pct': float(self.daily_return_pct),
            'is_trading_day': self.is_trading_day,
            'has_transactions': self.has_transactions
        }


@dataclass
class MonthlyCalendarData:
    """月历数据"""
    year: int
    month: int
    account_ids: List[int]
    
    # 日统计数据点（按日期索引）
    daily_stats: Dict[str, DailyStatsPoint]  # key: 'YYYY-MM-DD'
    
    # 月度汇总
    month_start_assets: Decimal = Decimal('0')
    month_end_assets: Decimal = Decimal('0')
    month_total_change: Decimal = Decimal('0')
    month_return_pct: Decimal = Decimal('0')
    
    # 统计信息
    trading_days_count: int = 0
    transaction_days_count: int = 0
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'year': self.year,
            'month': self.month,
            'account_ids': self.account_ids,
            'daily_stats': {date_str: point.to_dict() for date_str, point in self.daily_stats.items()},
            'month_summary': {
                'start_assets': float(self.month_start_assets),
                'end_assets': float(self.month_end_assets),
                'total_change': float(self.month_total_change),
                'return_pct': float(self.month_return_pct),
                'trading_days_count': self.trading_days_count,
                'transaction_days_count': self.transaction_days_count
            }
        }


class DailyStatsService:
    """日统计服务 - 月历视图的核心服务"""
    
    def __init__(self):
        self.asset_service = AssetValuationService()
        self.report_service = ReportService()
        self.history_manager = SmartHistoryManager()
        
    def get_monthly_calendar_data(self, account_ids: List[int], 
                                  year: int, month: int) -> MonthlyCalendarData:
        """
        获取指定月份的完整月历数据
        
        Args:
            account_ids: 账户ID列表
            year: 年份
            month: 月份
            
        Returns:
            MonthlyCalendarData对象，包含整月的每日统计
        """
        logger.info(f"生成{year}年{month}月的月历数据，账户: {account_ids}")
        
        # 计算月份日期范围
        start_date, end_date = self._get_month_date_range(year, month)
        
        # 创建月历数据对象
        calendar_data = MonthlyCalendarData(
            year=year,
            month=month,
            account_ids=account_ids,
            daily_stats={}
        )
        
        # 批量计算每日统计
        daily_stats = self._calculate_daily_stats_batch(account_ids, start_date, end_date)
        calendar_data.daily_stats = daily_stats
        
        # 计算月度汇总
        self._calculate_monthly_summary(calendar_data, start_date, end_date)
        
        logger.info(f"月历数据生成完成，包含{len(daily_stats)}天的数据")
        return calendar_data
    
    def get_current_month_calendar(self, account_ids: List[int]) -> MonthlyCalendarData:
        """获取当前月份的月历数据"""
        today = date.today()
        return self.get_monthly_calendar_data(account_ids, today.year, today.month)
    
    def get_daily_floating_pnl(self, account_ids: List[int], target_date: date) -> Dict:
        """
        获取指定日期的浮动盈亏详情
        
        Args:
            account_ids: 账户ID列表
            target_date: 目标日期
            
        Returns:
            包含详细浮动盈亏信息的字典
        """
        logger.info(f"计算{target_date}的浮动盈亏，账户: {account_ids}")
        
        # 复用现有的日度报表功能
        combined_report = self._get_combined_daily_report(account_ids, target_date)
        
        # 计算前一交易日的对比
        prev_trading_date = self.report_service._get_previous_business_day(target_date)
        prev_report = self._get_combined_daily_report(account_ids, prev_trading_date)
        
        # 计算变化
        daily_change = self._calculate_daily_change(combined_report, prev_report)
        
        return {
            'date': target_date.isoformat(),
            'account_ids': account_ids,
            'current': combined_report,
            'previous': prev_report,
            'change': daily_change,
            'floating_pnl': {
                'total_assets': combined_report['total_assets'],
                'unrealized_gain': combined_report['total_unrealized_gain'],
                'daily_change': daily_change['total_assets_change'],
                'daily_return_pct': daily_change['total_assets_change_pct']
            }
        }
    
    def _calculate_daily_stats_batch(self, account_ids: List[int], 
                                    start_date: date, end_date: date) -> Dict[str, DailyStatsPoint]:
        """
        批量计算日期范围内的每日统计
        使用现有的AssetValuationService进行优化计算
        """
        daily_stats = {}
        
        # 获取该月的所有交易日期（用于标记has_transactions）
        transaction_dates = self._get_transaction_dates_in_range(account_ids, start_date, end_date)
        
        # 逐日计算（这里可以进一步优化为批量计算）
        current_date = start_date
        prev_stats = None
        
        while current_date <= end_date:
            date_str = current_date.isoformat()
            
            # 为每个账户计算资产快照
            combined_snapshot = self._get_combined_asset_snapshot(account_ids, current_date)
            
            # 创建日统计点
            stats_point = DailyStatsPoint(
                date=current_date,
                account_id=0,  # 组合账户
                total_assets=combined_snapshot['total_assets'],
                stock_market_value=combined_snapshot['stock_market_value'],
                cash_balance=combined_snapshot['cash_balance_total_cad'],
                unrealized_gain=combined_snapshot.get('unrealized_gain', Decimal('0')),
                is_trading_day=self._is_trading_day(current_date),
                has_transactions=current_date in transaction_dates
            )
            
            # 计算日变化（相对于前一个有数据的日期）
            if prev_stats:
                self._calculate_daily_change_for_point(stats_point, prev_stats)
            
            daily_stats[date_str] = stats_point
            prev_stats = stats_point
            
            current_date += timedelta(days=1)
        
        return daily_stats
    
    def _get_combined_asset_snapshot(self, account_ids: List[int], target_date: date) -> Dict:
        """
        获取多个账户的合并资产快照
        集成智能缓存服务，优化性能
        """
        combined_data = {
            'total_assets': Decimal('0'),
            'stock_market_value': Decimal('0'),
            'cash_balance_cad': Decimal('0'),
            'cash_balance_usd': Decimal('0'),
            'cash_balance_total_cad': Decimal('0'),
            'unrealized_gain': Decimal('0')
        }
        
        for account_id in account_ids:
            # 首先尝试从缓存获取
            cached_snapshot = daily_stats_cache_service.get_cached_asset_snapshot(account_id, target_date)
            
            if cached_snapshot:
                # 使用缓存数据
                combined_data['total_assets'] += cached_snapshot.total_assets
                combined_data['stock_market_value'] += cached_snapshot.stock_market_value
                combined_data['cash_balance_cad'] += cached_snapshot.cash_balance_cad
                combined_data['cash_balance_usd'] += cached_snapshot.cash_balance_usd
                combined_data['cash_balance_total_cad'] += cached_snapshot.cash_balance_total_cad
            else:
                # 缓存未命中，使用AssetValuationService计算
                snapshot = self.asset_service.get_asset_snapshot(account_id, target_date)
                
                combined_data['total_assets'] += snapshot.total_assets
                combined_data['stock_market_value'] += snapshot.stock_market_value
                combined_data['cash_balance_cad'] += snapshot.cash_balance_cad
                combined_data['cash_balance_usd'] += snapshot.cash_balance_usd
                combined_data['cash_balance_total_cad'] += snapshot.cash_balance_total_cad
                
                # 将计算结果存入缓存
                snapshot_data = {
                    'total_assets': snapshot.total_assets,
                    'stock_market_value': snapshot.stock_market_value,
                    'cash_balance_cad': snapshot.cash_balance_cad,
                    'cash_balance_usd': snapshot.cash_balance_usd,
                    'cash_balance_total_cad': snapshot.cash_balance_total_cad
                }
                daily_stats_cache_service.cache_asset_snapshot(account_id, target_date, snapshot_data)
        
        return combined_data
    
    def _get_combined_daily_report(self, account_ids: List[int], target_date: date) -> Dict:
        """
        获取多个账户的合并日度报表
        复用ReportService的功能
        """
        combined_report = {
            'total_assets': Decimal('0'),
            'stock_market_value': Decimal('0'),
            'cash_balance': Decimal('0'),
            'total_unrealized_gain': Decimal('0')
        }
        
        for account_id in account_ids:
            try:
                daily_report = self.report_service.get_daily_report(account_id, target_date)
                current_data = daily_report['current']
                
                combined_report['total_assets'] += Decimal(str(current_data['total_assets']))
                combined_report['stock_market_value'] += Decimal(str(current_data['stock_market_value']))
                combined_report['cash_balance'] += Decimal(str(current_data['cash_balance']['total_cad']))
                
            except Exception as e:
                logger.warning(f"获取账户{account_id}日度报表失败: {e}")
                continue
        
        return combined_report
    
    def _calculate_daily_change(self, current: Dict, previous: Dict) -> Dict:
        """计算日变化，复用ReportService的逻辑"""
        current_assets = current.get('total_assets', Decimal('0'))
        prev_assets = previous.get('total_assets', Decimal('0'))
        
        change = current_assets - prev_assets
        change_pct = Decimal('0')
        
        if prev_assets > 0:
            change_pct = (change / prev_assets) * 100
        
        return {
            'total_assets_change': change,
            'total_assets_change_pct': change_pct,
            'stock_market_value_change': current.get('stock_market_value', Decimal('0')) - previous.get('stock_market_value', Decimal('0')),
            'cash_balance_change': current.get('cash_balance', Decimal('0')) - previous.get('cash_balance', Decimal('0'))
        }
    
    def _calculate_daily_change_for_point(self, current_point: DailyStatsPoint, 
                                        prev_point: DailyStatsPoint):
        """为统计点计算日变化"""
        current_point.daily_change = current_point.total_assets - prev_point.total_assets
        
        if prev_point.total_assets > 0:
            current_point.daily_return_pct = (current_point.daily_change / prev_point.total_assets) * 100
    
    def _calculate_monthly_summary(self, calendar_data: MonthlyCalendarData, 
                                  start_date: date, end_date: date):
        """计算月度汇总数据"""
        if not calendar_data.daily_stats:
            return
        
        # 获取月初和月末数据
        start_point = calendar_data.daily_stats.get(start_date.isoformat())
        end_point = calendar_data.daily_stats.get(end_date.isoformat())
        
        if start_point and end_point:
            calendar_data.month_start_assets = start_point.total_assets
            calendar_data.month_end_assets = end_point.total_assets
            calendar_data.month_total_change = end_point.total_assets - start_point.total_assets
            
            if start_point.total_assets > 0:
                calendar_data.month_return_pct = (calendar_data.month_total_change / start_point.total_assets) * 100
        
        # 统计交易日和有交易的日子
        for point in calendar_data.daily_stats.values():
            if point.is_trading_day:
                calendar_data.trading_days_count += 1
            if point.has_transactions:
                calendar_data.transaction_days_count += 1
    
    def _get_month_date_range(self, year: int, month: int) -> Tuple[date, date]:
        """获取月份的日期范围"""
        start_date = date(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = date(year, month, last_day)
        return start_date, end_date
    
    def _is_trading_day(self, target_date: date) -> bool:
        """判断是否为交易日（简化版，排除周末）"""
        return target_date.weekday() < 5  # 0-4 为周一到周五
    
    def _get_transaction_dates_in_range(self, account_ids: List[int], 
                                       start_date: date, end_date: date) -> set:
        """获取日期范围内有交易的日期集合"""
        from app.models.transaction import Transaction
        
        transactions = Transaction.query.filter(
            Transaction.account_id.in_(account_ids),
            Transaction.trade_date >= start_date,
            Transaction.trade_date <= end_date
        ).with_entities(Transaction.trade_date).distinct().all()
        
        return {tx.trade_date for tx in transactions}
    
    def get_calendar_summary_stats(self, account_ids: List[int], 
                                  year: int, month: int) -> Dict:
        """
        获取月历的汇总统计信息
        用于前端显示月度概览
        """
        calendar_data = self.get_monthly_calendar_data(account_ids, year, month)
        
        # 计算额外的统计指标
        daily_changes = [point.daily_change for point in calendar_data.daily_stats.values() 
                        if point.daily_change != 0]
        
        positive_days = len([change for change in daily_changes if change > 0])
        negative_days = len([change for change in daily_changes if change < 0])
        
        return {
            'calendar_data': calendar_data.to_dict(),
            'statistics': {
                'positive_days': positive_days,
                'negative_days': negative_days,
                'total_trading_days': calendar_data.trading_days_count,
                'transaction_days': calendar_data.transaction_days_count,
                'win_rate': (positive_days / len(daily_changes) * 100) if daily_changes else 0,
                'avg_daily_change': float(sum(daily_changes) / len(daily_changes)) if daily_changes else 0,
                'max_daily_gain': float(max(daily_changes)) if daily_changes else 0,
                'max_daily_loss': float(min(daily_changes)) if daily_changes else 0
            }
        }


# 全局服务实例
daily_stats_service = DailyStatsService()