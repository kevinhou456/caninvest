"""
货币转换服务

功能：
1. CAD/USD 汇率获取和缓存
2. 货币金额转换
3. 汇率历史记录
4. 年度平均汇率自动获取
"""

import requests
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional, List
import logging
from sqlalchemy import func
import json

from app import db
from flask import current_app

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
        
        # 如果数据库没有当日数据，从 Bank of Canada Valet API 获取
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
    
    def _fetch_boc_daily_rates(self, from_currency: str, to_currency: str,
                               start_date: date, end_date: date) -> Dict[date, Decimal]:
        """
        从 Bank of Canada Valet API 批量获取每日汇率。
        仅支持 USD/CAD（系列 FXUSDCAD）。
        返回 {date: Decimal}，失败时返回空字典。
        API 文档: https://www.bankofcanada.ca/valet/docs
        """
        if from_currency == to_currency:
            return {}
        if set([from_currency, to_currency]) != {'USD', 'CAD'}:
            logger.warning(f"Bank of Canada API only supports USD/CAD, got {from_currency}/{to_currency}")
            return {}

        invert = (from_currency == 'CAD')  # FXUSDCAD = 1 USD in CAD; if we want CAD→USD, invert
        series_id = 'FXUSDCAD'
        url = f"https://www.bankofcanada.ca/valet/observations/{series_id}/json"
        params = {'start_date': start_date.isoformat(), 'end_date': end_date.isoformat()}

        try:
            logger.info(f"Fetching daily rates from Bank of Canada: {start_date} to {end_date}")
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            observations = response.json().get('observations', [])

            result: Dict[date, Decimal] = {}
            for obs in observations:
                d_str = obs.get('d')
                val = (obs.get(series_id) or {}).get('v')
                if d_str and val is not None:
                    try:
                        rate_val = Decimal(str(val))
                        if invert:
                            rate_val = (Decimal('1') / rate_val).quantize(Decimal('0.000001'))
                        else:
                            rate_val = rate_val.quantize(Decimal('0.000001'))
                        result[date.fromisoformat(d_str)] = rate_val
                    except Exception:
                        pass

            logger.info(f"Bank of Canada returned {len(result)} daily rates")
            return result

        except Exception as e:
            logger.error(f"Failed to fetch daily rates from Bank of Canada: {e}")
            return {}

    def _fetch_rate_from_api(self, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """
        从 Bank of Canada Valet API 获取最新汇率（recent=1 取最近一个交易日）。
        """
        if from_currency == to_currency:
            return Decimal('1.0')

        if set([from_currency, to_currency]) != {'USD', 'CAD'}:
            logger.warning(f"Bank of Canada API only supports USD/CAD, got {from_currency}/{to_currency}")
            return self._get_default_rate(from_currency, to_currency)

        invert = (from_currency == 'CAD')
        series_id = 'FXUSDCAD'
        url = f"https://www.bankofcanada.ca/valet/observations/{series_id}/json"

        try:
            logger.info(f"Fetching current rate from Bank of Canada (recent=1)")
            response = requests.get(url, params={'recent': 1}, timeout=15)
            response.raise_for_status()
            observations = response.json().get('observations', [])

            if observations:
                obs = observations[-1]
                val = (obs.get(series_id) or {}).get('v')
                if val is not None:
                    rate_val = Decimal(str(val))
                    if invert:
                        rate_val = (Decimal('1') / rate_val).quantize(Decimal('0.000001'))
                    else:
                        rate_val = rate_val.quantize(Decimal('0.000001'))
                    obs_date = date.fromisoformat(obs['d']) if obs.get('d') else date.today()
                    logger.info(f"Bank of Canada current rate ({obs_date}): {from_currency}/{to_currency} = {rate_val}")
                    return rate_val

            logger.warning("Bank of Canada returned no recent observations")

        except Exception as e:
            logger.error(f"Failed to fetch current rate from Bank of Canada: {e}")

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

        # 从 Bank of Canada Valet API 获取年度平均汇率
        if year == current_year:
            rate = self._calculate_current_year_average(from_currency, to_currency)
        else:
            rate = self._fetch_annual_rate_from_bank_of_canada(year, from_currency, to_currency)
            if not rate:
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
                    if obs.get(series_id) and obs[series_id].get('v') is not None:
                        valid_rates.append(float(obs[series_id]['v']))

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
        """计算当前年份的年初至今平均汇率（使用 Bank of Canada Valet API）"""
        current_year = datetime.now().year
        start_date = date(current_year, 1, 1)
        end_date = date.today()
        daily = self._fetch_boc_daily_rates(from_currency, to_currency, start_date, end_date)
        if not daily:
            return None
        avg = sum(daily.values()) / len(daily)
        rate = Decimal(str(round(float(avg), 6)))
        logger.info(f"Current year average ({from_currency}/{to_currency}): {rate}")
        return rate

    def _calculate_historical_year_average(self, year: int, from_currency: str, to_currency: str) -> Optional[Decimal]:
        """计算历史年份的年度平均汇率（使用 Bank of Canada Valet API）"""
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        daily = self._fetch_boc_daily_rates(from_currency, to_currency, start_date, end_date)
        if not daily:
            return None
        avg = sum(daily.values()) / len(daily)
        rate = Decimal(str(round(float(avg), 6)))
        logger.info(f"Historical year average {year} ({from_currency}/{to_currency}): {rate}")
        return rate

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

    def get_rates_for_dates(self, dates: List[date], from_currency: str = 'USD', to_currency: str = 'CAD') -> Dict[date, Decimal]:
        """
        批量获取多个日期的汇率（用于T5008按日汇率计算）。
        优先查 exchange_rates 缓存表，缺失则从 Bank of Canada Valet API 批量拉取后一次性写入缓存，
        最后fallback到年度平均。对于周末/节假日，往前找最近7天内的有效交易日汇率。

        Returns:
            {date: Decimal} 每个请求日期对应的汇率
        """
        if from_currency == to_currency:
            return {d: Decimal('1.0') for d in dates}

        unique_dates = sorted(set(dates))
        if not unique_dates:
            return {}

        min_date = unique_dates[0] - timedelta(days=7)
        max_date = unique_dates[-1]

        # 1. 批量查缓存表（exchange_rates，排除年度平均）
        db_records = ExchangeRate.query.filter(
            ExchangeRate.from_currency == from_currency,
            ExchangeRate.to_currency == to_currency,
            ExchangeRate.date >= min_date,
            ExchangeRate.date <= max_date,
            ExchangeRate.source != 'ANNUAL_AVERAGE'
        ).order_by(ExchangeRate.date.asc()).all()

        db_rate_map: Dict[date, Decimal] = {r.date: Decimal(str(r.rate)) for r in db_records}

        def _find_nearest(target: date) -> Optional[Decimal]:
            """往前找最近7天内已缓存的汇率（处理周末/节假日）"""
            for delta in range(8):
                d = target - timedelta(days=delta)
                if d in db_rate_map:
                    return db_rate_map[d]
            return None

        result: Dict[date, Decimal] = {}
        missing_dates = []
        for d in unique_dates:
            r = _find_nearest(d)
            if r is not None:
                result[d] = r
            else:
                missing_dates.append(d)

        if missing_dates:
            # 2. 从 Bank of Canada Valet API 批量拉取缺失范围的历史数据
            newly_fetched: Dict[date, Decimal] = {}
            fetch_start = min(missing_dates) - timedelta(days=10)
            fetch_end = max(missing_dates) + timedelta(days=2)
            fetched = self._fetch_boc_daily_rates(from_currency, to_currency, fetch_start, fetch_end)
            for fetched_date, rate_val in fetched.items():
                if fetched_date not in db_rate_map:
                    db_rate_map[fetched_date] = rate_val
                    newly_fetched[fetched_date] = rate_val

            # 一次性写入缓存表（单次提交，避免在报表计算中散乱提交）
            if newly_fetched:
                self._batch_save_rates_to_cache(from_currency, to_currency, newly_fetched)

            # 重新匹配缺失日期
            still_missing = []
            for d in missing_dates:
                r = _find_nearest(d)
                if r is not None:
                    result[d] = r
                else:
                    still_missing.append(d)
            missing_dates = still_missing

        # 3. 最终fallback：年度平均
        if missing_dates:
            year_rates: Dict[int, Decimal] = {}
            for d in missing_dates:
                if d.year not in year_rates:
                    annual = self.get_annual_average_rate(d.year, from_currency, to_currency)
                    year_rates[d.year] = annual or self._get_default_rate(from_currency, to_currency)
                result[d] = year_rates[d.year]
                logger.warning(f"Using annual average fallback for {d} ({from_currency}/{to_currency}): {result[d]}")

        return result

    def _batch_save_rates_to_cache(self, from_currency: str, to_currency: str, rates: Dict[date, Decimal]):
        """将批量获取的历史汇率一次性写入 exchange_rates 缓存表（单次提交）。"""
        if not rates:
            return
        try:
            # 查出已存在的日期，避免重复插入
            existing_dates = {
                r.date for r in ExchangeRate.query.filter(
                    ExchangeRate.from_currency == from_currency,
                    ExchangeRate.to_currency == to_currency,
                    ExchangeRate.date.in_(list(rates.keys())),
                    ExchangeRate.source != 'ANNUAL_AVERAGE'
                ).with_entities(ExchangeRate.date).all()
            }
            for rate_date, rate in rates.items():
                if rate_date not in existing_dates:
                    db.session.add(ExchangeRate(
                        from_currency=from_currency,
                        to_currency=to_currency,
                        rate=rate,
                        date=rate_date,
                        source='DAILY_CACHE'
                    ))
            db.session.commit()
            logger.info(f"Cached {len(rates) - len(existing_dates)} daily rates for {from_currency}/{to_currency}")
        except Exception as e:
            logger.warning(f"Failed to cache daily rates: {e}")
            db.session.rollback()

    def refresh_annual_rates_from_bank_of_canada(
        self,
        years: List[int],
        from_currency: str = 'USD',
        to_currency: str = 'CAD'
    ) -> Dict[int, Optional[Decimal]]:
        """强制从加拿大银行刷新指定年份的年度平均汇率"""
        refreshed_rates: Dict[int, Optional[Decimal]] = {}

        for year in years:
            cache_key = f"annual_{from_currency}_{to_currency}_{year}"
            rate = self._fetch_annual_rate_from_bank_of_canada(year, from_currency, to_currency)

            if rate:
                # 清理缓存并保存最新值
                if cache_key in self._cache:
                    del self._cache[cache_key]
                self._save_annual_rate_to_db(year, from_currency, to_currency, rate)
                refreshed_rates[year] = rate
            else:
                # 如果加拿大银行没有数据，保持现有逻辑尝试其它渠道
                refreshed_rates[year] = self.get_annual_average_rate(year, from_currency, to_currency)

        return refreshed_rates


# 全局货币服务实例
currency_service = CurrencyService()
