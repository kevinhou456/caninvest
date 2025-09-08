"""
货币转换服务

功能：
1. CAD/USD 汇率获取和缓存
2. 货币金额转换
3. 汇率历史记录
"""

import requests
import yfinance as yf
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional
import logging
from sqlalchemy import func

from app import db

logger = logging.getLogger(__name__)


class ExchangeRate(db.Model):
    """汇率历史记录表"""
    __tablename__ = 'exchange_rates'
    
    id = db.Column(db.Integer, primary_key=True)
    from_currency = db.Column(db.String(3), nullable=False, comment='基础货币')
    to_currency = db.Column(db.String(3), nullable=False, comment='目标货币')
    rate = db.Column(db.Numeric(10, 6), nullable=False, comment='汇率')
    date = db.Column(db.Date, nullable=False, comment='汇率日期')
    source = db.Column(db.String(50), default='API', comment='数据来源')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    
    __table_args__ = (
        db.UniqueConstraint('from_currency', 'to_currency', 'date', name='unique_rate_per_day'),
    )
    
    def __repr__(self):
        return f'<ExchangeRate {self.from_currency}/{self.to_currency}: {self.rate} on {self.date}>'


class CurrencyService:
    """货币转换服务"""
    
    # 默认汇率（作为备选）
    DEFAULT_CAD_USD_RATE = Decimal('0.74')  # 1 CAD = 0.74 USD
    DEFAULT_USD_CAD_RATE = Decimal('1.35')  # 1 USD = 1.35 CAD
    
    def __init__(self):
        self._cache = {}
        self._cache_expiry = {}  # 缓存过期时间
    
    def get_current_rate(self, from_currency: str, to_currency: str) -> Decimal:
        """
        获取当前汇率
        
        Args:
            from_currency: 基础货币 (CAD/USD)
            to_currency: 目标货币 (USD/CAD)
            
        Returns:
            汇率（Decimal）
        """
        if from_currency == to_currency:
            return Decimal('1.0')
        
        # 检查内存缓存（5分钟有效期）
        cache_key = f"{from_currency}_{to_currency}"
        now = datetime.utcnow()
        
        if (cache_key in self._cache and 
            cache_key in self._cache_expiry and 
            now < self._cache_expiry[cache_key]):
            logger.debug(f"Using cached rate for {from_currency}/{to_currency}: {self._cache[cache_key]}")
            return self._cache[cache_key]
        
        # 从数据库获取最新汇率（当日数据）
        db_rate = self._get_latest_rate_from_db(from_currency, to_currency)
        if db_rate:
            self._cache[cache_key] = db_rate
            self._cache_expiry[cache_key] = now + timedelta(minutes=5)
            return db_rate
        
        # 如果数据库没有当日数据，尝试从Yahoo Finance API获取
        api_rate = self._fetch_rate_from_api(from_currency, to_currency)
        if api_rate:
            # 保存到数据库
            self._save_rate_to_db(from_currency, to_currency, api_rate, date.today())
            # 缓存5分钟
            self._cache[cache_key] = api_rate
            self._cache_expiry[cache_key] = now + timedelta(minutes=5)
            return api_rate
        
        # 使用默认汇率
        default_rate = self._get_default_rate(from_currency, to_currency)
        logger.warning(f"Using default rate for {from_currency}/{to_currency}: {default_rate}")
        return default_rate
    
    def _get_latest_rate_from_db(self, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """从数据库获取最新汇率"""
        # 优先查找当日汇率
        today = date.today()
        
        rate_record = ExchangeRate.query.filter(
            ExchangeRate.from_currency == from_currency,
            ExchangeRate.to_currency == to_currency,
            ExchangeRate.date == today
        ).first()
        
        
        
        return Decimal(str(rate_record.rate)) if rate_record else None
    
    def _fetch_rate_from_api(self, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """
        从Yahoo Finance API获取汇率
        
        使用Yahoo Finance获取实时汇率数据，支持CAD/USD货币对
        """
        try:
            # 构建货币对代码 (如: CADUSD=X, USDCAD=X)
            if from_currency == to_currency:
                return Decimal('1.0')
            
            currency_pair = f"{from_currency}{to_currency}=X"
            
            # 使用Yahoo Finance获取汇率
            ticker = yf.Ticker(currency_pair)
            data = ticker.history(period="1d", interval="1m")
            
            if not data.empty:
                # 获取最新收盘价
                latest_rate = data['Close'].iloc[-1]
                rate = Decimal(str(round(latest_rate, 6)))
                logger.info(f"Fetched rate from Yahoo Finance: {from_currency}/{to_currency} = {rate}")
                return rate
            else:
                logger.warning(f"No data returned from Yahoo Finance for {currency_pair}")
                
        except Exception as e:
            logger.error(f"Failed to fetch rate from Yahoo Finance: {e}")
        
        return None
    
    def _save_rate_to_db(self, from_currency: str, to_currency: str, rate: Decimal, rate_date: date):
        """保存汇率到数据库"""
        try:
            # 检查是否已存在
            existing = ExchangeRate.query.filter(
                ExchangeRate.from_currency == from_currency,
                ExchangeRate.to_currency == to_currency,
                ExchangeRate.date == rate_date
            ).first()
            
            if existing:
                existing.rate = rate
                existing.created_at = datetime.utcnow()
            else:
                new_rate = ExchangeRate(
                    from_currency=from_currency,
                    to_currency=to_currency,
                    rate=rate,
                    date=rate_date,
                    source='API'
                )
                db.session.add(new_rate)
            
            db.session.commit()
            logger.info(f"Saved rate to DB: {from_currency}/{to_currency} = {rate} on {rate_date}")
            
        except Exception as e:
            logger.error(f"Failed to save rate to DB: {e}")
            db.session.rollback()
    
    def _get_default_rate(self, from_currency: str, to_currency: str) -> Decimal:
        """获取默认汇率"""
        if from_currency == 'CAD' and to_currency == 'USD':
            return self.DEFAULT_CAD_USD_RATE
        elif from_currency == 'USD' and to_currency == 'CAD':
            return self.DEFAULT_USD_CAD_RATE
        else:
            return Decimal('1.0')
    
    def convert_amount(self, amount: Decimal, from_currency: str, to_currency: str) -> Decimal:
        """
        转换金额
        
        Args:
            amount: 要转换的金额
            from_currency: 源货币
            to_currency: 目标货币
            
        Returns:
            转换后的金额
        """
        if from_currency == to_currency:
            return amount
        
        rate = self.get_current_rate(from_currency, to_currency)
        converted_amount = amount * rate
        
        # 保留2位小数
        return converted_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    def get_cad_usd_rates(self) -> Dict[str, Decimal]:
        """
        获取CAD/USD双向汇率
        
        Returns:
            包含双向汇率的字典
        """
        cad_to_usd = self.get_current_rate('CAD', 'USD')
        usd_to_cad = self.get_current_rate('USD', 'CAD')
        
        return {
            'cad_to_usd': cad_to_usd,
            'usd_to_cad': usd_to_cad,
            'updated_date': date.today().isoformat()
        }
    
    def format_currency_amount(self, amount: Decimal, currency: str, 
                             show_both_currencies: bool = True) -> Dict[str, str]:
        """
        格式化货币显示
        
        Args:
            amount: 金额
            currency: 原始货币
            show_both_currencies: 是否显示双货币
            
        Returns:
            格式化后的货币字符串字典
        """
        primary_amount = amount
        primary_currency = currency
        
        result = {
            'primary': f"${primary_amount:,.2f} {primary_currency}",
            'primary_amount': float(primary_amount),
            'primary_currency': primary_currency
        }
        
        if show_both_currencies:
            # 转换到另一种货币
            other_currency = 'USD' if currency == 'CAD' else 'CAD'
            converted_amount = self.convert_amount(amount, currency, other_currency)
            
            result.update({
                'secondary': f"${converted_amount:,.2f} {other_currency}",
                'secondary_amount': float(converted_amount),
                'secondary_currency': other_currency,
                'exchange_rate': float(self.get_current_rate(currency, other_currency))
            })
        
        return result
    
    def get_rate_history(self, from_currency: str, to_currency: str, days: int = 30) -> list:
        """
        获取汇率历史
        
        Args:
            from_currency: 基础货币
            to_currency: 目标货币  
            days: 历史天数
            
        Returns:
            汇率历史列表
        """
        start_date = date.today() - timedelta(days=days)
        
        rates = ExchangeRate.query.filter(
            ExchangeRate.from_currency == from_currency,
            ExchangeRate.to_currency == to_currency,
            ExchangeRate.date >= start_date
        ).order_by(ExchangeRate.date.desc()).all()
        
        return [
            {
                'date': rate.date.isoformat(),
                'rate': float(rate.rate),
                'source': rate.source
            }
            for rate in rates
        ]
    
    def update_rates_daily(self):
        """
        每日更新汇率任务
        
        这个方法可以被定时任务调用，确保汇率数据的时效性
        """
        try:
            # 更新主要货币对
            currency_pairs = [
                ('CAD', 'USD'),
                ('USD', 'CAD')
            ]
            
            for from_curr, to_curr in currency_pairs:
                rate = self._fetch_rate_from_api(from_curr, to_curr)
                if rate:
                    self._save_rate_to_db(from_curr, to_curr, rate, date.today())
                    logger.info(f"Updated daily rate: {from_curr}/{to_curr} = {rate}")
            
            # 清除过期缓存
            self.clear_cache()
            
        except Exception as e:
            logger.error(f"Failed to update daily rates: {e}")
    
    def clear_cache(self):
        """清除汇率缓存"""
        self._cache.clear()
        self._cache_expiry.clear()
    
    def _clean_expired_cache(self):
        """清理过期的缓存项"""
        now = datetime.utcnow()
        expired_keys = [key for key, expiry_time in self._cache_expiry.items() 
                       if now >= expiry_time]
        
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_expiry.pop(key, None)


# 全局货币服务实例
currency_service = CurrencyService()