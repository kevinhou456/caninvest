"""
后台任务调度器
使用APScheduler实现定时任务
"""

import logging
from datetime import datetime
from flask import current_app
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.services.stock_price_service import StockPriceService
from app.models.stocks_cache import StocksCache
from app import db

logger = logging.getLogger(__name__)

class TaskScheduler:
    """任务调度器"""
    
    def __init__(self, app=None):
        self.scheduler = None
        self.app = app
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """初始化调度器"""
        self.app = app
        
        # 创建调度器
        self.scheduler = BackgroundScheduler(
            timezone='UTC',
            daemon=True
        )
        
        # 添加定时任务
        self._add_jobs()
        
        # 启动调度器
        if app.config.get('SCHEDULER_API_ENABLED', True):
            try:
                self.scheduler.start()
                logger.info("Task scheduler started successfully")
            except Exception as e:
                logger.error(f"Failed to start scheduler: {e}")
    
    def _add_jobs(self):
        """添加定时任务"""
        
        # 1. 股票价格更新任务 - 交易时间内每15分钟执行一次
        self.scheduler.add_job(
            func=self.update_stock_prices_trading_hours,
            trigger=CronTrigger(
                minute='*/15',  # 每15分钟
                hour='14-21',   # UTC时间14:30-21:00 对应ET时间9:30-16:00
                day_of_week='mon-fri'  # 周一到周五
            ),
            id='stock_price_update_trading',
            name='Stock Price Update (Trading Hours)',
            replace_existing=True
        )
        
        # 2. 股票价格更新任务 - 非交易时间每小时执行一次  
        self.scheduler.add_job(
            func=self.update_stock_prices_non_trading,
            trigger=IntervalTrigger(hours=1),
            id='stock_price_update_non_trading',
            name='Stock Price Update (Non-Trading Hours)',
            replace_existing=True
        )
        
        # 3. 清理过期缓存任务 - 每天凌晨2点执行
        self.scheduler.add_job(
            func=self.cleanup_expired_cache,
            trigger=CronTrigger(hour=2, minute=0),
            id='cleanup_expired_cache',
            name='Cleanup Expired Cache',
            replace_existing=True
        )
        
        # 4. 数据库维护任务 - 每周日凌晨3点执行
        self.scheduler.add_job(
            func=self.database_maintenance,
            trigger=CronTrigger(day_of_week='sun', hour=3, minute=0),
            id='database_maintenance',
            name='Database Maintenance',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs added successfully")
    
    def update_stock_prices_trading_hours(self):
        """交易时间内更新股票价格"""
        with self.app.app_context():
            try:
                logger.info("Starting stock price update (trading hours)")
                
                # 只更新需要更新的股票
                stocks_needing_update = StocksCache.get_stocks_needing_update()
                if not stocks_needing_update:
                    logger.info("No stocks need price updates")
                    return
                
                # 限制同时更新的股票数量以避免API限制
                max_updates = 20
                # 获取需要更新的股票的(symbol, currency)对
                symbol_currency_pairs = [(stock.symbol, stock.currency) for stock in stocks_needing_update[:max_updates]]
                
                price_service = StockPriceService()
                results = price_service.update_prices_for_symbols(symbol_currency_pairs)
                
                logger.info(f"Stock price update completed: {results}")
                
            except Exception as e:
                logger.error(f"Error updating stock prices (trading hours): {e}")
    
    def update_stock_prices_non_trading(self):
        """非交易时间更新股票价格"""
        with self.app.app_context():
            try:
                # 检查是否在交易时间
                now = datetime.utcnow()
                sample_stock = StocksCache.query.first()
                if sample_stock and sample_stock.is_trading_hours(now):
                    # 如果是交易时间，跳过此任务（由交易时间任务处理）
                    return
                
                logger.info("Starting stock price update (non-trading hours)")
                
                # 非交易时间更新频率较低，可以更新更多股票
                stocks_needing_update = StocksCache.get_stocks_needing_update()
                if not stocks_needing_update:
                    logger.info("No stocks need price updates")
                    return
                
                # 非交易时间可以更新更多股票
                max_updates = 50
                # 获取需要更新的股票的(symbol, currency)对
                symbol_currency_pairs = [(stock.symbol, stock.currency) for stock in stocks_needing_update[:max_updates]]
                
                price_service = StockPriceService()
                results = price_service.update_prices_for_symbols(symbol_currency_pairs)
                
                logger.info(f"Stock price update completed: {results}")
                
            except Exception as e:
                logger.error(f"Error updating stock prices (non-trading hours): {e}")
    
    def cleanup_expired_cache(self):
        """清理过期缓存"""
        with self.app.app_context():
            try:
                logger.info("Starting cache cleanup")
                
                # 清理过期的价格缓存
                from app.models.price_cache import StockPriceCache
                expired_count = StockPriceCache.cleanup_expired()
                
                # 清理旧的价格更新日志
                price_service = StockPriceService()
                cleanup_results = price_service.cleanup_old_cache(days=7)
                
                logger.info(f"Cache cleanup completed: expired_cache={expired_count}, "
                           f"cleanup_results={cleanup_results}")
                
            except Exception as e:
                logger.error(f"Error during cache cleanup: {e}")
    
    def database_maintenance(self):
        """数据库维护任务"""
        with self.app.app_context():
            try:
                logger.info("Starting database maintenance")
                
                # 分析数据库表
                db.engine.execute('ANALYZE;')
                
                # 清理旧的日志记录（保留3个月）
                from app.models.price_cache import PriceUpdateLog
                from datetime import timedelta
                cutoff_date = datetime.utcnow() - timedelta(days=90)
                
                deleted_logs = PriceUpdateLog.query.filter(
                    PriceUpdateLog.date < cutoff_date.date()
                ).delete()
                
                db.session.commit()
                
                logger.info(f"Database maintenance completed: deleted_logs={deleted_logs}")
                
            except Exception as e:
                logger.error(f"Error during database maintenance: {e}")
    
    def get_job_status(self):
        """获取任务状态"""
        if not self.scheduler:
            return {'status': 'not_initialized'}
        
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })
        
        return {
            'status': 'running' if self.scheduler.running else 'stopped',
            'jobs': jobs
        }
    
    def shutdown(self):
        """关闭调度器"""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Task scheduler shutdown")

# 全局调度器实例
scheduler = TaskScheduler()