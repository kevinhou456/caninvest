from datetime import datetime
from app import db


class ReportAnalysisCache(db.Model):
    """Cached analysis payload for annual/quarterly/monthly reports."""

    __tablename__ = 'report_analysis_cache'

    id = db.Column(db.Integer, primary_key=True)
    cache_type = db.Column(db.String(20), nullable=False, index=True)
    cache_key = db.Column(db.String(40), nullable=False, index=True)
    family_id = db.Column(db.Integer, nullable=False)
    member_id = db.Column(db.Integer, nullable=True)
    account_id = db.Column(db.Integer, nullable=True)
    account_ids_json = db.Column(db.Text, nullable=False)
    params_json = db.Column(db.Text, nullable=False)
    data_json = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('cache_type', 'cache_key', name='uq_report_analysis_cache_type_key'),
    )

