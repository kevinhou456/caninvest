"""
导入任务模型
"""

from datetime import datetime
from enum import Enum
from app import db

class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

class ImportTask(db.Model):
    """CSV导入任务模型"""
    
    __tablename__ = 'import_tasks'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    filename = db.Column(db.String(255), nullable=False, comment='文件名')
    original_filename = db.Column(db.String(255), comment='原始文件名')
    file_path = db.Column(db.String(500), comment='文件路径')
    broker_format = db.Column(db.String(50), comment='券商格式')
    detected_format = db.Column(db.String(50), comment='自动检测的格式')
    status = db.Column(db.Enum(TaskStatus), default=TaskStatus.PENDING, comment='任务状态')
    total_rows = db.Column(db.Integer, default=0, comment='总行数')
    processed_rows = db.Column(db.Integer, default=0, comment='已处理行数')
    imported_count = db.Column(db.Integer, default=0, comment='成功导入数')
    failed_count = db.Column(db.Integer, default=0, comment='失败数')
    skipped_count = db.Column(db.Integer, default=0, comment='跳过数')
    error_details = db.Column(db.Text, comment='错误详情')
    processing_log = db.Column(db.Text, comment='处理日志')
    created_by = db.Column(db.Integer, db.ForeignKey('members.id'), comment='创建者')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    started_at = db.Column(db.DateTime, comment='开始时间')
    completed_at = db.Column(db.DateTime, comment='完成时间')
    
    # 关系
    creator = db.relationship('Member', foreign_keys=[created_by])
    
    def __repr__(self):
        return f'<ImportTask {self.filename} {self.status.value}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'account': {
                'id': self.account.id,
                'name': self.account.name
            } if self.account else None,
            'filename': self.filename,
            'original_filename': self.original_filename,
            'broker_format': self.broker_format,
            'detected_format': self.detected_format,
            'status': self.status.value,
            'total_rows': self.total_rows,
            'processed_rows': self.processed_rows,
            'imported_count': self.imported_count,
            'failed_count': self.failed_count,
            'skipped_count': self.skipped_count,
            'progress_percentage': self.progress_percentage,
            'error_details': self.error_details,
            'created_by': self.created_by,
            'creator': {
                'id': self.creator.id,
                'name': self.creator.name
            } if self.creator else None,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration': self.duration
        }
    
    @property
    def progress_percentage(self):
        """进度百分比"""
        if self.total_rows == 0:
            return 0
        return (self.processed_rows / self.total_rows) * 100
    
    @property
    def duration(self):
        """任务持续时间（秒）"""
        if not self.started_at:
            return None
        
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()
    
    def start_processing(self):
        """开始处理"""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.utcnow()
        db.session.commit()
    
    def complete_successfully(self):
        """成功完成"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        db.session.commit()
    
    def fail_with_error(self, error_message):
        """失败并记录错误"""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error_details = error_message
        db.session.commit()
    
    def update_progress(self, processed_rows, imported_count, failed_count, skipped_count=0):
        """更新进度"""
        self.processed_rows = processed_rows
        self.imported_count = imported_count
        self.failed_count = failed_count
        self.skipped_count = skipped_count
        db.session.commit()
    
    def add_log(self, message):
        """添加处理日志"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        
        if self.processing_log:
            self.processing_log += log_entry
        else:
            self.processing_log = log_entry
        
        db.session.commit()

class OCRTask(db.Model):
    """OCR识别任务模型"""
    
    __tablename__ = 'ocr_tasks'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, comment='账户ID')
    image_filename = db.Column(db.String(255), nullable=False, comment='图片文件名')
    original_filename = db.Column(db.String(255), comment='原始文件名')
    image_path = db.Column(db.String(500), comment='图片路径')
    detected_broker = db.Column(db.String(50), comment='检测到的券商')
    ocr_engine = db.Column(db.String(20), default='tesseract', comment='OCR引擎')
    status = db.Column(db.Enum(TaskStatus), default=TaskStatus.PENDING, comment='任务状态')
    confidence_score = db.Column(db.Numeric(5, 4), comment='整体置信度')
    transactions_detected = db.Column(db.Integer, default=0, comment='检测到的交易数')
    transactions_imported = db.Column(db.Integer, default=0, comment='导入的交易数')
    needs_review = db.Column(db.Boolean, default=False, comment='需要人工审核')
    ocr_raw_text = db.Column(db.Text, comment='OCR原始文本')
    error_details = db.Column(db.Text, comment='错误详情')
    processing_log = db.Column(db.Text, comment='处理日志')
    created_by = db.Column(db.Integer, db.ForeignKey('members.id'), comment='创建者')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    started_at = db.Column(db.DateTime, comment='开始时间')
    completed_at = db.Column(db.DateTime, comment='完成时间')
    
    # 关系
    creator = db.relationship('Member', foreign_keys=[created_by])
    pending_transactions = db.relationship('OCRTransactionPending', backref='ocr_task', lazy='dynamic', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<OCRTask {self.image_filename} {self.status.value}>'
    
    def to_dict(self, include_transactions=False):
        result = {
            'id': self.id,
            'account_id': self.account_id,
            'account': {
                'id': self.account.id,
                'name': self.account.name
            } if self.account else None,
            'image_filename': self.image_filename,
            'original_filename': self.original_filename,
            'detected_broker': self.detected_broker,
            'ocr_engine': self.ocr_engine,
            'status': self.status.value,
            'confidence_score': float(self.confidence_score) if self.confidence_score else None,
            'transactions_detected': self.transactions_detected,
            'transactions_imported': self.transactions_imported,
            'needs_review': self.needs_review,
            'error_details': self.error_details,
            'created_by': self.created_by,
            'creator': {
                'id': self.creator.id,
                'name': self.creator.name
            } if self.creator else None,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration': self.duration
        }
        
        if include_transactions:
            result['transactions'] = [t.to_dict() for t in self.pending_transactions]
        
        return result
    
    @property
    def duration(self):
        """任务持续时间（秒）"""
        if not self.started_at:
            return None
        
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()
    
    def start_processing(self):
        """开始处理"""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.utcnow()
        db.session.commit()
    
    def complete_successfully(self, confidence_score=None, needs_review=False):
        """成功完成"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        if confidence_score is not None:
            self.confidence_score = confidence_score
        self.needs_review = needs_review
        db.session.commit()
    
    def fail_with_error(self, error_message):
        """失败并记录错误"""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.error_details = error_message
        db.session.commit()
    
    def add_log(self, message):
        """添加处理日志"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {message}\n"
        
        if self.processing_log:
            self.processing_log += log_entry
        else:
            self.processing_log = log_entry
        
        db.session.commit()

class OCRTransactionPending(db.Model):
    """OCR识别待审核交易记录"""
    
    __tablename__ = 'ocr_transactions_pending'
    
    id = db.Column(db.Integer, primary_key=True)
    ocr_task_id = db.Column(db.Integer, db.ForeignKey('ocr_tasks.id'), nullable=False)
    symbol = db.Column(db.String(20), comment='股票代码')
    transaction_type = db.Column(db.String(10), comment='交易类型')
    quantity = db.Column(db.Numeric(15, 4), comment='数量')
    price_per_share = db.Column(db.Numeric(15, 4), comment='单价')
    transaction_fee = db.Column(db.Numeric(10, 2), default=0, comment='手续费')
    trade_date = db.Column(db.Date, comment='交易日期')
    confidence_score = db.Column(db.Numeric(5, 4), comment='置信度')
    needs_review = db.Column(db.Boolean, default=False, comment='需要审核')
    raw_text = db.Column(db.Text, comment='原始识别文本')
    status = db.Column(db.String(20), default='pending', comment='状态: pending/approved/rejected/modified')
    notes = db.Column(db.Text, comment='审核备注')
    reviewed_by = db.Column(db.Integer, db.ForeignKey('members.id'), comment='审核者')
    reviewed_at = db.Column(db.DateTime, comment='审核时间')
    created_at = db.Column(db.DateTime, default=datetime.utcnow, comment='创建时间')
    
    # 关系
    reviewer = db.relationship('Member', foreign_keys=[reviewed_by])
    
    def __repr__(self):
        return f'<OCRTransactionPending {self.symbol} {self.transaction_type} {self.status}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'ocr_task_id': self.ocr_task_id,
            'symbol': self.symbol,
            'transaction_type': self.transaction_type,
            'quantity': float(self.quantity) if self.quantity else None,
            'price_per_share': float(self.price_per_share) if self.price_per_share else None,
            'transaction_fee': float(self.transaction_fee) if self.transaction_fee else None,
            'total_amount': float(self.total_amount) if self.total_amount else None,
            'trade_date': self.trade_date.isoformat() if self.trade_date else None,
            'confidence_score': float(self.confidence_score) if self.confidence_score else None,
            'needs_review': self.needs_review,
            'raw_text': self.raw_text,
            'status': self.status,
            'notes': self.notes,
            'reviewed_by': self.reviewed_by,
            'reviewer': {
                'id': self.reviewer.id,
                'name': self.reviewer.name
            } if self.reviewer else None,
            'reviewed_at': self.reviewed_at.isoformat() if self.reviewed_at else None,
            'created_at': self.created_at.isoformat()
        }
    
    @property
    def total_amount(self):
        """交易总金额"""
        if self.quantity and self.price_per_share:
            return self.quantity * self.price_per_share
        return None
    
    def approve(self, reviewer_id, notes=None):
        """批准交易"""
        self.status = 'approved'
        self.reviewed_by = reviewer_id
        self.reviewed_at = datetime.utcnow()
        if notes:
            self.notes = notes
        db.session.commit()
    
    def reject(self, reviewer_id, notes=None):
        """拒绝交易"""
        self.status = 'rejected'
        self.reviewed_by = reviewer_id
        self.reviewed_at = datetime.utcnow()
        if notes:
            self.notes = notes
        db.session.commit()
    
    def modify_and_approve(self, reviewer_id, **kwargs):
        """修改并批准"""
        # 更新字段
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        self.status = 'approved'
        self.reviewed_by = reviewer_id
        self.reviewed_at = datetime.utcnow()
        db.session.commit()
    
    def convert_to_transaction(self, account_id, member_id=None):
        """转换为正式交易记录"""
        if self.status != 'approved':
            return None
        
        from app.models.transaction import Transaction
        from app.models.stock import Stock
        
        # 获取或创建股票记录
        stock = Stock.get_or_create(symbol=self.symbol)
        
        # 创建交易记录
        transaction = Transaction(
            account_id=account_id,
            stock_id=stock.id,
            member_id=member_id,
            transaction_type=self.transaction_type,
            quantity=self.quantity,
            price_per_share=self.price_per_share,
            transaction_fee=self.transaction_fee or 0,
            trade_date=self.trade_date,
            notes=f'Imported from OCR task {self.ocr_task_id}' + 
                  (f'. Review notes: {self.notes}' if self.notes else '')
        )
        
        db.session.add(transaction)
        return transaction