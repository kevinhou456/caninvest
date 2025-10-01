"""
货币转换服务

功能：
1. CAD/USD 汇率获取和缓存
2. 货币金额转换
3. 汇率历史记录
4. 年度平均汇率自动获取
"""

import requests
import yfinance as yf
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional, List
import logging
from sqlalchemy import func
import json

from app import db

logger = logging.getLogger(__name__)


def _log_yfinance_call(api_name: str, identifier: str, **kwargs):
    details = " ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
    message = f"[yfinance] {api_name} id={identifier} {details}".strip()
    logger.debug(message)
    print(message)


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
    
    # 默认汇率（作为备选，基于2024年平均值）
    DEFAULT_CAD_USD_RATE = Decimal('0.7384')  # 1 CAD = 0.7384 USD
    DEFAULT_USD_CAD_RATE = Decimal('1.3542')  # 1 USD = 1.3542 CAD
    
    def __init__(self):
        self._cache = {}
        self._cache_expiry = {}  # 缓存过期时间

        # 创建自定义session以避免反爬虫
        self._session = None
        self._setup_session()

    def _setup_session(self):
        """设置自定义HTTP会话"""
        try:
            import requests
            self._session = requests.Session()

            # 设置更真实的User-Agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            self._session.headers.update(headers)
            logger.debug("Custom session configured with headers")
        except Exception as e:
            logger.warning(f"Failed to setup custom session: {e}")
            self._session = None

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
            logger.info(f"Attempting to fetch rate for {currency_pair}")

            # 添加调试信息
            import yfinance
            logger.info(f"yfinance version: {yfinance.__version__}")

            # 使用Yahoo Finance获取汇率（不使用自定义session，让yfinance自己处理）
            _log_yfinance_call('Ticker.init', currency_pair)
            ticker = yf.Ticker(currency_pair)
            logger.debug(f"Created ticker object for {currency_pair}")

            # 尝试不同的时间段，从最短开始
            periods = ["1d", "5d", "1mo"]
            for period in periods:
                try:
                    _log_yfinance_call('Ticker.history', currency_pair, period=period)
                    data = ticker.history(period=period)
                    logger.debug(f"Data shape: {data.shape if not data.empty else 'empty'}")

                    if not data.empty:
                        # 获取最新收盘价
                        latest_rate = data['Close'].iloc[-1]
                        rate = Decimal(str(round(latest_rate, 6)))
                        logger.info(f"SUCCESS: Fetched rate from Yahoo Finance: {from_currency}/{to_currency} = {rate} (period: {period})")
                        return rate
                    else:
                        logger.warning(f"Empty data for period {period}")
                except Exception as inner_e:
                    logger.error(f"Failed to fetch with period {period}: {str(inner_e)}, type: {type(inner_e).__name__}")
                    continue

            logger.warning(f"No data returned from Yahoo Finance for {currency_pair} with any period")

        except Exception as e:
            logger.error(f"Failed to fetch rate from Yahoo Finance: {str(e)}, type: {type(e).__name__}")
            import traceback
            logger.debug(f"Full traceback: {traceback.format_exc()}")

        # 备用方案：尝试反向汇率
        try:
            if from_currency != to_currency:
                reverse_pair = f"{to_currency}{from_currency}=X"
                logger.info(f"Trying reverse pair: {reverse_pair}")

                _log_yfinance_call('Ticker.init', reverse_pair)
                ticker = yf.Ticker(reverse_pair)
                _log_yfinance_call('Ticker.history', reverse_pair, period="1d")
                data = ticker.history(period="1d")

                if not data.empty:
                    reverse_rate = data['Close'].iloc[-1]
                    rate = Decimal('1.0') / Decimal(str(round(reverse_rate, 6)))
                    logger.info(f"SUCCESS: Fetched reverse rate from Yahoo Finance: {to_currency}/{from_currency} = {reverse_rate}, calculated {from_currency}/{to_currency} = {rate}")
                    return rate
                else:
                    logger.warning(f"Empty data for reverse pair {reverse_pair}")
        except Exception as e:
            logger.error(f"Reverse rate fetch also failed: {str(e)}, type: {type(e).__name__}")

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

    def get_annual_average_rate(self, year: int, from_currency: str = 'USD', to_currency: str = 'CAD') -> Optional[Decimal]:
        """
        获取年度平均汇率

        Args:
            year: 年份
            from_currency: 基础货币 (默认USD)
            to_currency: 目标货币 (默认CAD)

        Returns:
            年度平均汇率 (Decimal)
        """
        current_year = datetime.now().year

        # 检查缓存
        cache_key = f"annual_{from_currency}_{to_currency}_{year}"
        if cache_key in self._cache:
            logger.debug(f"Using cached annual rate for {year}: {self._cache[cache_key]}")
            return self._cache[cache_key]

        # 检查数据库中是否已有年度平均汇率
        db_rate = self._get_annual_rate_from_db(year, from_currency, to_currency)
        if db_rate:
            self._cache[cache_key] = db_rate
            return db_rate

        # 如果是当前年份，使用Yahoo Finance计算年初至今平均值
        if year == current_year:
            rate = self._calculate_current_year_average(from_currency, to_currency)
        else:
            # 历史年份优先从加拿大银行获取
            rate = self._fetch_annual_rate_from_bank_of_canada(year, from_currency, to_currency)
            if not rate:
                # 备选方案：通过Yahoo Finance获取历史数据计算
                rate = self._calculate_historical_year_average(year, from_currency, to_currency)

        if rate:
            # 保存到数据库和缓存
            self._save_annual_rate_to_db(year, from_currency, to_currency, rate)
            self._cache[cache_key] = rate
            return rate

        logger.warning(f"Unable to get annual rate for {year} {from_currency}/{to_currency}")
        return None

    def _get_annual_rate_from_db(self, year: int, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """从数据库获取年度平均汇率"""
        # 查找年度平均汇率（使用年份的1月1日作为标识）
        year_date = date(year, 1, 1)

        rate_record = ExchangeRate.query.filter(
            ExchangeRate.from_currency == from_currency,
            ExchangeRate.to_currency == to_currency,
            ExchangeRate.date == year_date,
            ExchangeRate.source == 'ANNUAL_AVERAGE'
        ).first()

        return Decimal(str(rate_record.rate)) if rate_record else None

    def _fetch_annual_rate_from_bank_of_canada(self, year: int, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """
        从加拿大银行获取年度平均汇率

        加拿大银行API文档: https://www.bankofcanada.ca/valet/docs
        """
        try:
            # 加拿大银行的API端点
            # USD/CAD 的系列ID是 FXUSDCAD
            if from_currency == 'USD' and to_currency == 'CAD':
                series_id = 'FXUSDCAD'
            elif from_currency == 'CAD' and to_currency == 'USD':
                # 对于CAD/USD，我们获取USD/CAD然后取倒数
                series_id = 'FXUSDCAD'
            else:
                logger.warning(f"Unsupported currency pair for Bank of Canada: {from_currency}/{to_currency}")
                return None

            # 构建API URL - 获取年度数据
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            url = f"https://www.bankofcanada.ca/valet/observations/{series_id}/json"
            params = {
                'start_date': start_date,
                'end_date': end_date
            }

            logger.info(f"Fetching annual rate from Bank of Canada for {year}")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            observations = data.get('observations', [])

            if observations:
                # 计算年度平均值
                valid_rates = []
                for obs in observations:
                    if obs.get(f'd.{series_id}') and obs[f'd.{series_id}']['v'] is not None:
                        valid_rates.append(float(obs[f'd.{series_id}']['v']))

                if valid_rates:
                    annual_average = sum(valid_rates) / len(valid_rates)

                    # 如果请求的是CAD/USD，取倒数
                    if from_currency == 'CAD' and to_currency == 'USD':
                        annual_average = 1.0 / annual_average

                    rate = Decimal(str(round(annual_average, 6)))
                    logger.info(f"Fetched annual rate from Bank of Canada: {from_currency}/{to_currency} {year} = {rate}")
                    return rate

            logger.warning(f"No valid data from Bank of Canada for {year}")

        except Exception as e:
            logger.error(f"Failed to fetch annual rate from Bank of Canada: {e}")

        return None

    def _calculate_current_year_average(self, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """计算当前年份的年初至今平均汇率（使用Yahoo Finance）"""
        try:
            current_year = datetime.now().year
            start_date = date(current_year, 1, 1)
            end_date = date.today()

            # 构建货币对代码
            currency_pair = f"{from_currency}{to_currency}=X"

            logger.info(f"Calculating current year average for {currency_pair}")
            ticker = yf.Ticker(currency_pair)

            # 获取年初至今的历史数据
            data = ticker.history(start=start_date, end=end_date, interval="1d")

            if not data.empty and 'Close' in data.columns:
                # 计算平均收盘价
                average_rate = data['Close'].mean()
                rate = Decimal(str(round(average_rate, 6)))
                logger.info(f"Calculated current year average: {from_currency}/{to_currency} = {rate}")
                return rate
            else:
                logger.warning(f"No data returned from Yahoo Finance for {currency_pair}")

        except Exception as e:
            logger.error(f"Failed to calculate current year average: {e}")

        return None

    def _calculate_historical_year_average(self, year: int, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """计算历史年份的年度平均汇率（使用Yahoo Finance）"""
        try:
            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            # 构建货币对代码
            currency_pair = f"{from_currency}{to_currency}=X"

            logger.info(f"Calculating historical year average for {currency_pair} in {year}")
            ticker = yf.Ticker(currency_pair)

            # 获取历史数据
            data = ticker.history(start=start_date, end=end_date, interval="1d")

            if not data.empty and 'Close' in data.columns:
                # 计算平均收盘价
                average_rate = data['Close'].mean()
                rate = Decimal(str(round(average_rate, 6)))
                logger.info(f"Calculated historical year average: {from_currency}/{to_currency} {year} = {rate}")
                return rate
            else:
                logger.warning(f"No historical data for {currency_pair} in {year}")

        except Exception as e:
            logger.error(f"Failed to calculate historical year average: {e}")

        return None

    def _save_annual_rate_to_db(self, year: int, from_currency: str, to_currency: str, rate: Decimal):
        """保存年度平均汇率到数据库"""
        try:
            # 使用年份的1月1日作为年度平均汇率的标识日期
            year_date = date(year, 1, 1)

            # 检查是否已存在
            existing = ExchangeRate.query.filter(
                ExchangeRate.from_currency == from_currency,
                ExchangeRate.to_currency == to_currency,
                ExchangeRate.date == year_date,
                ExchangeRate.source == 'ANNUAL_AVERAGE'
            ).first()

            if existing:
                existing.rate = rate
                existing.created_at = datetime.utcnow()
            else:
                new_rate = ExchangeRate(
                    from_currency=from_currency,
                    to_currency=to_currency,
                    rate=rate,
                    date=year_date,
                    source='ANNUAL_AVERAGE'
                )
                db.session.add(new_rate)

            db.session.commit()
            logger.info(f"Saved annual rate to DB: {from_currency}/{to_currency} {year} = {rate}")

        except Exception as e:
            logger.error(f"Failed to save annual rate to DB: {e}")
            db.session.rollback()

    def get_annual_rates_for_years(self, years: List[int], from_currency: str = 'USD', to_currency: str = 'CAD') -> Dict[int, Optional[Decimal]]:
        """
        批量获取多个年份的年度平均汇率

        Args:
            years: 年份列表
            from_currency: 基础货币
            to_currency: 目标货币

        Returns:
            年份到汇率的字典
        """
        result = {}
        for year in years:
            result[year] = self.get_annual_average_rate(year, from_currency, to_currency)

        return result


# 全局货币服务实例
currency_service = CurrencyService()
