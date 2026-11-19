"""
T3 Box 42 (Return of Capital) 记录
"""
from datetime import datetime
from app import db


class T3Box42(db.Model):
    __tablename__ = 't3_box42'

    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id', ondelete='CASCADE'), nullable=False)
    stock = db.Column(db.String(20), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    box42_amount = db.Column(db.Numeric(15, 2), nullable=False, default=0)
    currency = db.Column(db.String(3), nullable=False, default='CAD')
    notes = db.Column(db.String(500), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('account_id', 'stock', 'year', name='uq_t3_box42_account_stock_year'),
    )

    account = db.relationship('Account', backref=db.backref('t3_box42_records', lazy='dynamic'))

    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'stock': self.stock,
            'year': self.year,
            'box42_amount': float(self.box42_amount),
            'currency': self.currency,
            'notes': self.notes,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
