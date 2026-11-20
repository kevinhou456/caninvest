from datetime import datetime
from app import db


class OverviewSnapshot(db.Model):
    """【已停用】曾用于缓存总览页数据。2026-11 起代码不再读写此表，仅保留表结构。"""

    __tablename__ = 'overview_snapshots'

    id = db.Column(db.Integer, primary_key=True)
    snapshot_key = db.Column(db.String(40), nullable=False, index=True)
    snapshot_date = db.Column(db.Date, nullable=False, index=True)
    account_ids_json = db.Column(db.Text, nullable=False)
    ownership_json = db.Column(db.Text, nullable=True)
    selected_account_id = db.Column(db.Integer, nullable=True)
    data_json = db.Column(db.Text, nullable=False)
    symbols_json = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('snapshot_date', 'snapshot_key', name='uq_overview_snapshot_key_date'),
    )

