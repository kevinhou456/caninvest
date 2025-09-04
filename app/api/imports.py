"""
数据导入API（CSV和OCR）
"""

import os
import uuid
from flask import request, jsonify, current_app
from flask_babel import _
from werkzeug.utils import secure_filename
from app import db
from app.models.import_task import ImportTask, OCRTask, TaskStatus
from app.models.account import Account
from . import bp

def allowed_file(filename, allowed_extensions):
    """检查文件扩展名"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

@bp.route('/accounts/<int:account_id>/transactions/import-csv', methods=['POST'])
def import_csv_transactions(account_id):
    """导入CSV交易记录"""
    account = Account.query.get_or_404(account_id)
    
    # 检查文件
    if 'file' not in request.files:
        return jsonify({'error': _('No file uploaded')}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': _('No file selected')}), 400
    
    if not allowed_file(file.filename, {'csv'}):
        return jsonify({'error': _('Invalid file format. Only CSV files are allowed.')}), 400
    
    # 获取券商格式
    broker_format = request.form.get('broker_format')
    
    # 保存文件
    original_filename = secure_filename(file.filename)
    file_extension = original_filename.rsplit('.', 1)[1].lower()
    unique_filename = f"{uuid.uuid4().hex}.{file_extension}"
    file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], unique_filename)
    
    try:
        file.save(file_path)
    except Exception as e:
        return jsonify({'error': f'Failed to save file: {str(e)}'}), 500
    
    # 创建导入任务
    import_task = ImportTask(
        account_id=account_id,
        filename=unique_filename,
        original_filename=original_filename,
        file_path=file_path,
        broker_format=broker_format,
        status=TaskStatus.PENDING,
        created_by=request.json.get('created_by') if request.is_json else None
    )
    
    db.session.add(import_task)
    db.session.commit()
    
    # 这里应该启动异步任务处理CSV
    # from app.tasks import process_csv_import
    # process_csv_import.delay(import_task.id)
    
    # 暂时返回任务信息，实际处理会在后台进行
    return jsonify({
        'success': True,
        'task_id': import_task.id,
        'message': _('CSV import task created successfully'),
        'task': import_task.to_dict()
    }), 201

@bp.route('/accounts/<int:account_id>/transactions/import-screenshot', methods=['POST'])
def import_screenshot_transactions(account_id):
    """上传截图进行OCR识别"""
    account = Account.query.get_or_404(account_id)
    
    # 检查文件
    if 'image' not in request.files:
        return jsonify({'error': _('No image uploaded')}), 400
    
    image_file = request.files['image']
    if image_file.filename == '':
        return jsonify({'error': _('No image selected')}), 400
    
    # 验证图像格式
    allowed_image_extensions = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff'}
    if not allowed_file(image_file.filename, allowed_image_extensions):
        return jsonify({'error': _('Invalid image format')}), 400
    
    # 保存图像文件
    original_filename = secure_filename(image_file.filename)
    file_extension = original_filename.rsplit('.', 1)[1].lower()
    unique_filename = f"{uuid.uuid4().hex}.{file_extension}"
    image_path = os.path.join(current_app.config['UPLOAD_FOLDER'], unique_filename)
    
    try:
        image_file.save(image_path)
    except Exception as e:
        return jsonify({'error': f'Failed to save image: {str(e)}'}), 500
    
    # 创建OCR任务
    ocr_task = OCRTask(
        account_id=account_id,
        image_filename=unique_filename,
        original_filename=original_filename,
        image_path=image_path,
        ocr_engine=current_app.config.get('OCR_ENGINE', 'tesseract'),
        status=TaskStatus.PENDING,
        created_by=request.json.get('created_by') if request.is_json else None
    )
    
    db.session.add(ocr_task)
    db.session.commit()
    
    # 这里应该启动异步OCR处理任务
    # from app.tasks import process_screenshot_ocr
    # process_screenshot_ocr.delay(ocr_task.id)
    
    return jsonify({
        'success': True,
        'task_id': ocr_task.id,
        'message': _('OCR processing task created successfully'),
        'task': ocr_task.to_dict()
    }), 201

@bp.route('/import-tasks/<int:task_id>', methods=['GET'])
def get_import_task(task_id):
    """获取导入任务状态"""
    task = ImportTask.query.get_or_404(task_id)
    return jsonify(task.to_dict())

@bp.route('/ocr-tasks/<int:task_id>', methods=['GET'])
def get_ocr_task(task_id):
    """获取OCR任务状态"""
    task = OCRTask.query.get_or_404(task_id)
    include_transactions = request.args.get('include_transactions', 'false').lower() == 'true'
    return jsonify(task.to_dict(include_transactions=include_transactions))

@bp.route('/ocr-tasks/<int:task_id>/review', methods=['POST'])
def submit_ocr_review(task_id):
    """提交OCR审核结果"""
    ocr_task = OCRTask.query.get_or_404(task_id)
    
    if ocr_task.status != TaskStatus.COMPLETED:
        return jsonify({'error': _('Task is not ready for review')}), 400
    
    data = request.get_json()
    if not data or 'transactions' not in data:
        return jsonify({'error': _('Transaction data required')}), 400
    
    reviewer_id = data.get('reviewer_id')
    if not reviewer_id:
        return jsonify({'error': _('Reviewer ID required')}), 400
    
    approved_count = 0
    rejected_count = 0
    
    for transaction_data in data['transactions']:
        pending_id = transaction_data.get('id')
        action = transaction_data.get('action')  # 'approve', 'reject', 'modify'
        
        if not pending_id or not action:
            continue
        
        from app.models.import_task import OCRTransactionPending
        pending_txn = OCRTransactionPending.query.filter_by(
            id=pending_id,
            ocr_task_id=task_id
        ).first()
        
        if not pending_txn:
            continue
        
        if action == 'approve':
            pending_txn.approve(reviewer_id, transaction_data.get('notes'))
            approved_count += 1
        elif action == 'reject':
            pending_txn.reject(reviewer_id, transaction_data.get('notes'))
            rejected_count += 1
        elif action == 'modify':
            # 更新字段然后批准
            updates = transaction_data.get('updates', {})
            pending_txn.modify_and_approve(reviewer_id, **updates)
            approved_count += 1
    
    # 创建正式的交易记录
    from app.models.transaction import Transaction
    
    approved_transactions = ocr_task.pending_transactions.filter_by(status='approved').all()
    created_transactions = []
    
    for pending_txn in approved_transactions:
        transaction = pending_txn.convert_to_transaction(
            account_id=ocr_task.account_id,
            member_id=reviewer_id
        )
        if transaction:
            # 更新持仓
            transaction.update_holdings()
            created_transactions.append(transaction)
    
    # 更新OCR任务统计
    ocr_task.transactions_imported = len(created_transactions)
    ocr_task.needs_review = False
    
    db.session.commit()
    
    return jsonify({
        'message': _('OCR review completed successfully'),
        'approved_count': approved_count,
        'rejected_count': rejected_count,
        'imported_count': len(created_transactions),
        'task': ocr_task.to_dict()
    })

@bp.route('/import-tasks', methods=['GET'])
def get_import_tasks():
    """获取导入任务列表"""
    account_id = request.args.get('account_id', type=int)
    status = request.args.get('status')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    query = ImportTask.query
    
    if account_id:
        query = query.filter_by(account_id=account_id)
    if status and status in [s.value for s in TaskStatus]:
        query = query.filter_by(status=TaskStatus(status))
    
    tasks = query.order_by(ImportTask.created_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'tasks': [task.to_dict() for task in tasks.items],
        'pagination': {
            'page': tasks.page,
            'pages': tasks.pages,
            'per_page': tasks.per_page,
            'total': tasks.total
        }
    })

@bp.route('/ocr-tasks', methods=['GET'])
def get_ocr_tasks():
    """获取OCR任务列表"""
    account_id = request.args.get('account_id', type=int)
    status = request.args.get('status')
    needs_review = request.args.get('needs_review', type=bool)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    
    query = OCRTask.query
    
    if account_id:
        query = query.filter_by(account_id=account_id)
    if status and status in [s.value for s in TaskStatus]:
        query = query.filter_by(status=TaskStatus(status))
    if needs_review is not None:
        query = query.filter_by(needs_review=needs_review)
    
    tasks = query.order_by(OCRTask.created_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'tasks': [task.to_dict() for task in tasks.items],
        'pagination': {
            'page': tasks.page,
            'pages': tasks.pages,
            'per_page': tasks.per_page,
            'total': tasks.total
        }
    })

@bp.route('/accounts/<int:account_id>/transactions/export-csv', methods=['GET'])
def export_transactions_csv(account_id):
    """导出交易记录为CSV"""
    account = Account.query.get_or_404(account_id)
    
    # 获取参数
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        from app.services.csv_service import CSVTransactionService
        
        csv_service = CSVTransactionService()
        file_path = csv_service.export_transactions_to_csv(
            account_id=account_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # 生成下载URL
        filename = os.path.basename(file_path)
        download_url = f'/static/exports/{filename}'
        
        return jsonify({
            'success': True,
            'message': _('CSV export completed successfully'),
            'download_url': download_url,
            'filename': filename
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@bp.route('/import-tasks/<int:task_id>/retry', methods=['POST'])
def retry_import_task(task_id):
    """重试失败的导入任务"""
    task = ImportTask.query.get_or_404(task_id)
    
    if task.status != TaskStatus.FAILED:
        return jsonify({'error': _('Only failed tasks can be retried')}), 400
    
    # 重置任务状态
    task.status = TaskStatus.PENDING
    task.error_details = None
    task.started_at = None
    task.completed_at = None
    task.processed_rows = 0
    task.imported_count = 0
    task.failed_count = 0
    task.skipped_count = 0
    
    db.session.commit()
    
    # 重新启动处理
    # from app.tasks import process_csv_import
    # process_csv_import.delay(task.id)
    
    return jsonify({
        'message': _('Import task restarted successfully'),
        'task': task.to_dict()
    })

@bp.route('/import-tasks/<int:task_id>', methods=['DELETE'])
def delete_import_task(task_id):
    """删除导入任务"""
    task = ImportTask.query.get_or_404(task_id)
    
    # 删除相关文件
    if task.file_path and os.path.exists(task.file_path):
        try:
            os.remove(task.file_path)
        except OSError:
            pass  # 文件可能已被删除
    
    db.session.delete(task)
    db.session.commit()
    
    return jsonify({
        'message': _('Import task deleted successfully')
    })

@bp.route('/ocr-tasks/<int:task_id>', methods=['DELETE'])
def delete_ocr_task(task_id):
    """删除OCR任务"""
    task = OCRTask.query.get_or_404(task_id)
    
    # 删除相关文件
    if task.image_path and os.path.exists(task.image_path):
        try:
            os.remove(task.image_path)
        except OSError:
            pass
    
    db.session.delete(task)
    db.session.commit()
    
    return jsonify({
        'message': _('OCR task deleted successfully')
    })