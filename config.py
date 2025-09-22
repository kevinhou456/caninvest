import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    """基础配置类"""
    
    # Flask核心配置
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # 数据库配置 - 使用项目根目录确保可访问性
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    
    # 国际化配置
    LANGUAGES = ['en', 'zh_CN', 'zh_Hans_CN']
    BABEL_DEFAULT_LOCALE = 'en'
    BABEL_TRANSLATION_DIRECTORIES = 'translations'
    BABEL_DEFAULT_TIMEZONE = 'UTC'
    
    # 文件上传配置
    UPLOAD_FOLDER = os.path.join(basedir, 'static', 'uploads')
    EXPORT_FOLDER = os.path.join(basedir, 'static', 'exports')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB
    ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'csv', 'xlsx'}
    
    # 股票价格API配置
    ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')
    
    # 股票价格缓存配置
    PRICE_CACHE_TTL = 900  # 15分钟
    PRICE_UPDATE_INTERVAL = 300  # 5分钟
    MAX_DAILY_PRICE_REQUESTS = 200
    
    # OCR配置
    OCR_ENGINE = 'tesseract'  # 'tesseract', 'easyocr', 'paddleocr'
    OCR_LANGUAGES = ['eng', 'chi_sim']
    
    # 任务调度配置
    SCHEDULER_API_ENABLED = True
    SCHEDULER_AUTO_START = False  # 暂时禁用自动启动，保留代码供将来使用
    
    # 日志配置
    LOG_LEVEL = 'INFO'
    LOG_FILE = os.path.join(basedir, 'logs', 'app.log')
    
    @staticmethod
    def init_app(app):
        """应用初始化回调"""
        # 创建必要的目录
        for folder in [Config.UPLOAD_FOLDER, Config.EXPORT_FOLDER,
                      os.path.dirname(Config.LOG_FILE)]:
            if not os.path.exists(folder):
                os.makedirs(folder)

class DevelopmentConfig(Config):
    """开发环境配置"""
    DEBUG = True
    SQLALCHEMY_ECHO = False
    LOG_LEVEL = 'DEBUG'
    
    # 开发模式下的股票价格配置
    PRICE_CACHE_TTL = 300  # 5分钟缓存
    MAX_DAILY_PRICE_REQUESTS = 500
    SCHEDULER_AUTO_START = False  # 开发环境暂时禁用自动启动

class TestingConfig(Config):
    """测试环境配置"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    WTF_CSRF_ENABLED = False
    
    # 测试模式禁用外部API调用
    ENABLE_STOCK_PRICE_FETCH = False

class ProductionConfig(Config):
    """生产环境配置"""
    DEBUG = False
    
    # 生产环境数据库
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'postgresql://username:password@localhost/canadian_investment'
    
    # 安全配置
    SESSION_COOKIE_SECURE = True
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=1)
    
    # 日志配置
    LOG_LEVEL = 'WARNING'
    
    @classmethod
    def init_app(cls, app):
        Config.init_app(app)
        
        # 生产环境日志配置
        import logging
        from logging.handlers import RotatingFileHandler
        
        if not app.debug and not app.testing:
            if not os.path.exists('logs'):
                os.mkdir('logs')
            
            file_handler = RotatingFileHandler('logs/app.log', 
                                             maxBytes=10240, backupCount=10)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s '
                '[in %(pathname)s:%(lineno)d]'
            ))
            file_handler.setLevel(logging.INFO)
            app.logger.addHandler(file_handler)
            
            app.logger.setLevel(logging.INFO)
            app.logger.info('Canadian Investment System startup')

# 配置字典
config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}