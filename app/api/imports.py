"""
数据导入API（CSV和OCR）
"""

import os
import uuid
from datetime import datetime
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

@bp.route('/csv-preview', methods=['POST'])
def csv_preview():
    """CSV预览和分析端点"""
    import pandas as pd
    import uuid
    import os
    
    # 检查文件
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': _('No file uploaded')}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': _('No file selected')}), 400
    
    if not allowed_file(file.filename, {'csv'}):
        return jsonify({'success': False, 'error': _('Invalid file format. Only CSV files are allowed.')}), 400
    
    try:
        # 使用通用的分隔符检测
        from app.utils.csv_utils import detect_csv_delimiter_from_fileobj
        delimiter = detect_csv_delimiter_from_fileobj(file)
        
        # 读取CSV文件
        df = pd.read_csv(file, encoding='utf-8-sig', sep=delimiter)
        
        if df.empty:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400
        
        # 生成会话ID并临时保存文件
        session_id = str(uuid.uuid4())
        temp_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', '/tmp'), 'csv_sessions')
        os.makedirs(temp_dir, exist_ok=True)
        
        temp_file_path = os.path.join(temp_dir, f'{session_id}.csv')
        
        # 重置文件指针并保存
        file.seek(0)
        file.save(temp_file_path)
        
        # 获取列名
        columns = df.columns.tolist()
        
        # 获取前10行数据用于预览
        preview_data = []
        for index, row in df.head(10).iterrows():
            row_data = {}
            for col in columns:
                value = row[col]
                # 处理NaN值
                if pd.isna(value):
                    row_data[col] = ''
                else:
                    row_data[col] = str(value)
            preview_data.append(row_data)
        
        # 首先尝试通过表头自动匹配已保存的格式
        from app.models.csv_format import CsvFormat
        auto_matched_format = CsvFormat.find_by_headers(columns)
        
        if auto_matched_format:
            # 找到完全匹配的格式，使用保存的映射
            suggested_mapping = auto_matched_format.mappings
            print(f"DEBUG: Auto-matched format '{auto_matched_format.format_name}' (used {auto_matched_format.usage_count} times)")
        else:
            # 没有找到匹配，使用智能建议
            suggested_mapping = smart_header_mapping(columns) or {}
            print("DEBUG: No auto-match found, using smart mapping")
        
        # 检查用户是否手动选择了其他格式（优先级较低）
        saved_format_name = request.form.get('saved_format', '').strip()
        format_name = request.form.get('format_name', '').strip()
        
        if saved_format_name and not auto_matched_format:
            # 只在没有自动匹配时才使用用户选择的格式
            manual_format = CsvFormat.get_by_name(saved_format_name)
            if manual_format:
                suggested_mapping = manual_format.mappings
                print(f"DEBUG: Using manually selected format '{saved_format_name}'")
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'columns': columns,
            'preview_data': preview_data,
            'suggested_mapping': suggested_mapping,
            'total_rows': len(df),
            'auto_matched_format': {
                'name': auto_matched_format.format_name,
                'usage_count': auto_matched_format.usage_count
            } if auto_matched_format else None
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'CSV analysis error: {str(e)}'}), 500

@bp.route('/import-csv-with-mapping', methods=['POST'])
def import_csv_with_mapping():
    """使用用户映射导入CSV"""
    import pandas as pd
    import os
    from app.models.transaction import Transaction
    
    # 重置日期格式检测，确保每次导入都重新检测
    reset_date_format_detection()
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': _('No data provided')}), 400
    
    session_id = data.get('session_id')
    account_id = data.get('account_id')
    column_mappings = data.get('column_mappings', {})
    format_name = data.get('format_name', '').strip()
    
    if not session_id or not account_id or not column_mappings:
        return jsonify({'success': False, 'error': _('Missing required data')}), 400
    
    # 验证账户
    account = Account.query.get_or_404(account_id)
    
    try:
        # 读取临时文件
        temp_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', '/tmp'), 'csv_sessions')
        temp_file_path = os.path.join(temp_dir, f'{session_id}.csv')
        
        if not os.path.exists(temp_file_path):
            return jsonify({'success': False, 'error': _('Session expired, please upload the file again')}), 400
        
        # 使用通用的分隔符检测
        from app.utils.csv_utils import detect_csv_delimiter_from_file
        delimiter = detect_csv_delimiter_from_file(temp_file_path)
        
        df = pd.read_csv(temp_file_path, encoding='utf-8-sig', sep=delimiter)
        
        # 处理数据
        transactions_data, processing_errors = process_csv_with_mapping(df, column_mappings)
        
        if not transactions_data:
            error_msg = _('No valid transactions found in CSV file')
            if processing_errors:
                error_msg += f". {_('Sample errors')}: " + "; ".join(processing_errors[:3])
            return jsonify({'success': False, 'error': error_msg}), 400
        
        # 创建交易记录
        created_count = 0
        failed_count = 0
        skipped_count = 0
        errors = []
        
        for row_data in transactions_data:
            try:
                # 检查是否为重复记录
                trade_date = row_data['trade_date']
                type = row_data['type']
                stock = row_data['stock']
                quantity = row_data['quantity']
                price = row_data['price']
                currency = row_data.get('currency', 'USD')
                fee = row_data.get('fee', 0)
                notes = row_data.get('notes', '')
                amount = row_data.get('amount', None)
                
                if Transaction.is_duplicate(
                    account_id=account_id,
                    trade_date=trade_date,
                    type=type,
                    stock=stock,
                    quantity=quantity,
                    price=price,
                    currency=currency,
                    fee=fee,
                    notes=notes
                ):
                    skipped_count += 1
                    print(f"DEBUG: Skipping duplicate transaction - {type} {quantity} {stock} on {trade_date}")
                    continue
                
                transaction = Transaction(
                    account_id=account_id,
                    trade_date=trade_date,
                    type=type,
                    stock=stock,
                    quantity=quantity,
                    price=price,
                    amount=amount,
                    currency=currency,
                    fee=fee,
                    notes=notes
                )
                
                db.session.add(transaction)
                created_count += 1
                print(f"DEBUG: Adding new transaction - {type} {quantity} {stock} on {trade_date}")
                
            except Exception as e:
                failed_count += 1
                errors.append(f"Row {row_data.get('row_num', '?')}: {str(e)}")
        
        db.session.commit()
        
        # 保存格式映射以备将来使用
        if created_count > 0:
            # 重新读取CSV以获取原始headers，使用相同的分隔符检测逻辑
            from app.utils.csv_utils import detect_csv_delimiter_from_file
            delimiter = detect_csv_delimiter_from_file(temp_file_path)
            
            df_headers = pd.read_csv(temp_file_path, encoding='utf-8-sig', nrows=0, sep=delimiter)
            original_headers = df_headers.columns.tolist()
            
            # 如果用户没有输入格式名称，自动生成一个友好的名称
            if not format_name or not format_name.strip():
                from datetime import datetime
                timestamp = datetime.now().strftime('%m-%d %H:%M')
                # 尝试根据表头生成更友好的名称
                if len(original_headers) >= 3:
                    # 使用前3个列名生成名称
                    sample_headers = ', '.join(original_headers[:3])
                    format_name = f"Format ({sample_headers}...) {timestamp}"
                else:
                    format_name = f"Auto-saved format {timestamp}"
                print(f"DEBUG: Auto-generated format name: {format_name}")
            
            save_format_mapping(format_name, original_headers, column_mappings)
        
        # 清理临时文件
        try:
            os.remove(temp_file_path)
        except:
            pass
        
        return jsonify({
            'success': True,
            'message': _('CSV import completed'),
            'created_count': created_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,  # 添加跳过的重复记录数量
            'errors': errors[:10],  # 最多返回10个错误
            'redirect_url': f'/transactions?account_id={account_id}'  # 添加重定向URL
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Import error: {str(e)}'}), 500

def process_csv_with_mapping(df, column_mappings):
    """使用用户映射处理CSV数据"""
    global _detected_date_format
    _detected_date_format = None  # 重置日期格式检测
    
    transactions = []
    processing_errors = []
    
    print(f"DEBUG: Processing {len(df)} rows with mappings: {column_mappings}")
    print(f"DEBUG: Available DataFrame columns: {df.columns.tolist()}")
    
    # 验证映射的列是否存在于DataFrame中
    missing_columns = []
    for field, column_name in column_mappings.items():
        if column_name and column_name not in df.columns:
            missing_columns.append(f"'{column_name}' (mapped from {field})")
    
    if missing_columns:
        error_msg = f"Missing columns in CSV: {', '.join(missing_columns)}"
        print(f"ERROR: {error_msg}")
        return [], [error_msg]
    
    # 预先收集所有日期样本用于格式检测
    date_samples = []
    if 'date' in column_mappings and column_mappings['date']:
        for index, row in df.iterrows():
            date_str = str(row[column_mappings['date']]).strip()
            if date_str and date_str.lower() not in ['nan', 'none', '']:
                date_samples.append(date_str)
        
        # 检测日期格式
        if date_samples:
            _detected_date_format = detect_date_format(date_samples)
    
    for index, row in df.iterrows():
        row_num = index + 2
        try:
            # 解析各字段
            row_data = {'row_num': row_num}
            
            # 日期
            if 'date' in column_mappings and column_mappings['date']:
                date_str = str(row[column_mappings['date']]).strip()
                print(f"DEBUG: Row {row_num} - Date string: '{date_str}'")
                trade_date = parse_date(date_str, date_samples)
                if not trade_date:
                    processing_errors.append(f"Row {row_num}: Invalid date '{date_str}'")
                    continue
                row_data['trade_date'] = trade_date
            else:
                processing_errors.append(f"Row {row_num}: No date mapping")
                continue
            
            # 交易类型
            if 'type' in column_mappings and column_mappings['type']:
                type_str = str(row[column_mappings['type']]).strip().upper()
                print(f"DEBUG: Row {row_num} - Type string: '{type_str}'")
                transaction_type = parse_transaction_type(type_str)
                if not transaction_type:
                    processing_errors.append(f"Row {row_num}: Invalid transaction type '{type_str}'")
                    continue
                row_data['type'] = transaction_type
            else:
                processing_errors.append(f"Row {row_num}: No type mapping")
                continue
            
            # 股票代码 - 只对股票交易类型要求股票代码
            requires_symbol = transaction_type in ['BUY', 'SELL', 'DIVIDEND']
            
            if 'symbol' in column_mappings and column_mappings['symbol']:
                symbol = str(row[column_mappings['symbol']]).strip().upper()
                print(f"DEBUG: Row {row_num} - Symbol: '{symbol}'")
                if symbol and symbol != 'NAN':
                    row_data['stock'] = symbol
                elif requires_symbol:
                    processing_errors.append(f"Row {row_num}: Invalid symbol '{symbol}' for transaction type '{transaction_type}'")
                    continue
                else:
                    row_data['stock'] = None  # 对于现金交易不设置股票代码
            else:
                if requires_symbol:
                    processing_errors.append(f"Row {row_num}: No symbol mapping for transaction type '{transaction_type}'")
                    continue
                else:
                    row_data['stock'] = None  # 对于现金交易不设置股票代码
            
            # 处理amount字段（现金交易的总金额）
            amount_value = None
            if 'amount' in column_mappings and column_mappings['amount']:
                amount_str = str(row[column_mappings['amount']]).replace(',', '').replace('$', '')
                print(f"DEBUG: Row {row_num} - Amount string: '{amount_str}'")
                if amount_str and amount_str.lower() not in ['nan', '', 'none']:
                    try:
                        amount_value = abs(float(amount_str))
                        row_data['amount'] = amount_value
                    except (ValueError, TypeError):
                        amount_value = None

            # 数量和价格处理 - 根据交易类型决定逻辑
            try:
                if transaction_type in ['DEPOSIT', 'WITHDRAWAL', 'DIVIDEND', 'INTEREST']:
                    # 现金交易：优先使用amount值，quantity和price可选
                    if amount_value is not None:
                        # 有amount值时，quantity和price可以为0（表示不使用）
                        if 'quantity' in column_mappings and column_mappings['quantity']:
                            quantity_str = str(row[column_mappings['quantity']]).replace(',', '').replace('$', '')
                            if quantity_str and quantity_str.lower() not in ['nan', '', 'none']:
                                row_data['quantity'] = abs(float(quantity_str))
                            else:
                                row_data['quantity'] = 0  # 可以为0
                        else:
                            row_data['quantity'] = 0
                        
                        if 'price' in column_mappings and column_mappings['price']:
                            price_str = str(row[column_mappings['price']]).replace(',', '').replace('$', '')
                            if price_str and price_str.lower() not in ['nan', '', 'none']:
                                row_data['price'] = abs(float(price_str))
                            else:
                                row_data['price'] = 0  # 可以为0
                        else:
                            row_data['price'] = 0
                    else:
                        # 没有amount值时，使用传统的quantity*price逻辑
                        if 'quantity' in column_mappings and column_mappings['quantity']:
                            quantity_str = str(row[column_mappings['quantity']]).replace(',', '').replace('$', '')
                            if quantity_str and quantity_str.lower() not in ['nan', '', 'none']:
                                row_data['quantity'] = abs(float(quantity_str))
                            else:
                                row_data['quantity'] = 1.0
                        else:
                            row_data['quantity'] = 1.0
                        
                        if 'price' in column_mappings and column_mappings['price']:
                            price_str = str(row[column_mappings['price']]).replace(',', '').replace('$', '')
                            if price_str and price_str.lower() not in ['nan', '', 'none']:
                                row_data['price'] = abs(float(price_str))
                            else:
                                row_data['price'] = 1.0
                        else:
                            row_data['price'] = 1.0
                else:
                    # 股票交易：必须有quantity和price，忽略amount
                    if 'quantity' in column_mappings and column_mappings['quantity']:
                        quantity_str = str(row[column_mappings['quantity']]).replace(',', '').replace('$', '')
                        print(f"DEBUG: Row {row_num} - Quantity string: '{quantity_str}'")
                        if quantity_str and quantity_str.lower() not in ['nan', '', 'none']:
                            row_data['quantity'] = abs(float(quantity_str))
                        else:
                            processing_errors.append(f"Row {row_num}: Invalid quantity for stock transaction")
                            continue
                    else:
                        processing_errors.append(f"Row {row_num}: Quantity required for stock transaction")
                        continue
                    
                    if 'price' in column_mappings and column_mappings['price']:
                        price_str = str(row[column_mappings['price']]).replace(',', '').replace('$', '')
                        print(f"DEBUG: Row {row_num} - Price string: '{price_str}'")
                        if price_str and price_str.lower() not in ['nan', '', 'none']:
                            row_data['price'] = abs(float(price_str))
                        else:
                            processing_errors.append(f"Row {row_num}: Invalid price for stock transaction")
                            continue
                    else:
                        processing_errors.append(f"Row {row_num}: Price required for stock transaction")
                        continue
                    
                    # 股票交易不使用amount字段
                    row_data['amount'] = None
                    
            except (ValueError, TypeError) as e:
                processing_errors.append(f"Row {row_num}: Invalid number format - {str(e)}")
                continue
            
            # 可选字段
            # 货币 - 优先从专门的货币列获取，否则从描述字段中智能提取
            currency = 'USD'  # 默认值
            
            if 'currency' in column_mappings and column_mappings['currency']:
                curr_str = str(row[column_mappings['currency']]).strip().upper()
                if curr_str and curr_str != 'NAN':
                    # 如果是标准的3位货币代码，直接使用
                    if len(curr_str) == 3 and curr_str.isalpha():
                        currency = curr_str
                    else:
                        # 否则从文本中智能提取货币（如 "CAD RRSP" -> "CAD"）
                        if 'CAD' in curr_str or 'CANADIAN' in curr_str:
                            currency = 'CAD'
                        elif 'USD' in curr_str or 'US ' in curr_str or 'AMERICAN' in curr_str:
                            currency = 'USD'
                        elif 'EUR' in curr_str or 'EURO' in curr_str:
                            currency = 'EUR'
                        print(f"DEBUG: Row {row_num} - Extracted currency '{currency}' from currency field: '{curr_str[:50]}...'")
                else:
                    print(f"DEBUG: Row {row_num} - Empty currency field, using default USD")
            else:
                # 从描述字段中智能提取货币
                description_text = ''
                if 'description' in column_mappings and column_mappings['description']:
                    description_text = str(row[column_mappings['description']]).strip().upper()
                
                # 检查账户描述字段
                account_desc = ''
                if 'account_desc' in column_mappings and column_mappings['account_desc']:
                    account_desc = str(row[column_mappings['account_desc']]).strip().upper()
                
                # 合并所有可能包含货币信息的文本
                combined_text = f"{description_text} {account_desc}".strip()
                
                # 检测货币代码
                if 'CAD' in combined_text or 'CANADIAN' in combined_text:
                    currency = 'CAD'
                elif 'USD' in combined_text or 'US ' in combined_text or 'AMERICAN' in combined_text:
                    currency = 'USD'
                elif 'EUR' in combined_text or 'EURO' in combined_text:
                    currency = 'EUR'
                
                print(f"DEBUG: Row {row_num} - Detected currency '{currency}' from text: '{combined_text[:50]}...'")
            
            row_data['currency'] = currency
            
            # 手续费
            if 'fee' in column_mappings and column_mappings['fee']:
                try:
                    fee_str = str(row[column_mappings['fee']]).replace(',', '').replace('$', '')
                    if fee_str and fee_str != 'nan' and fee_str != 'NAN':
                        row_data['fee'] = float(fee_str)
                    else:
                        row_data['fee'] = 0
                except (ValueError, TypeError):
                    row_data['fee'] = 0
            else:
                row_data['fee'] = 0
            
            # 备注
            if 'notes' in column_mappings and column_mappings['notes']:
                notes_str = str(row[column_mappings['notes']])
                if notes_str and notes_str != 'nan' and notes_str != 'NAN':
                    row_data['notes'] = notes_str.strip()
                else:
                    row_data['notes'] = ''
            else:
                row_data['notes'] = ''
            
            transactions.append(row_data)
            print(f"DEBUG: Row {row_num} - Successfully processed")
            
        except Exception as e:
            error_msg = f"Row {row_num}: Unexpected error - {str(e)}"
            processing_errors.append(error_msg)
            print(f"ERROR: {error_msg}")
            print(f"DEBUG: Exception type: {type(e).__name__}")
            print(f"DEBUG: Available columns: {df.columns.tolist()}")
            print(f"DEBUG: Column mappings: {column_mappings}")
            continue
    
    print(f"DEBUG: Processed {len(transactions)} valid transactions out of {len(df)} rows")
    if processing_errors:
        print(f"DEBUG: Processing errors: {processing_errors[:5]}")  # 打印前5个错误
    
    # 返回结果和错误信息
    return transactions, processing_errors

def get_saved_format(format_name):
    """获取保存的格式映射"""
    from app.models.csv_format import CsvFormat
    
    format_record = CsvFormat.get_by_name(format_name)
    if format_record:
        return format_record.mappings
    return {}

def save_format_mapping(format_name, headers, column_mappings):
    """保存格式映射"""
    from app.models.csv_format import CsvFormat
    
    try:
        CsvFormat.create_or_update(format_name, headers, column_mappings)
        db.session.commit()
        print(f"DEBUG: Saved format mapping '{format_name}' with fingerprint")
    except Exception as e:
        print(f"Error saving format mapping: {str(e)}")
        db.session.rollback()

@bp.route('/csv-formats', methods=['GET'])
def get_csv_formats():
    """获取保存的CSV格式列表"""
    from app.models.csv_format import CsvFormat
    
    formats = CsvFormat.get_popular_formats(20)  # 获取前20个热门格式
    
    return jsonify({
        'success': True,
        'formats': [format.to_dict() for format in formats]
    })

@bp.route('/import-csv', methods=['POST'])
def import_csv_flexible():
    """智能CSV导入 - 支持多种券商格式"""
    import pandas as pd
    import re
    from app.models.transaction import Transaction
    
    # 检查文件
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': _('No file uploaded')}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'error': _('No file selected')}), 400
    
    if not allowed_file(file.filename, {'csv'}):
        return jsonify({'success': False, 'error': _('Invalid file format. Only CSV files are allowed.')}), 400
    
    # 获取账户ID
    account_id = request.form.get('account_id')
    if not account_id:
        return jsonify({'success': False, 'error': _('Account ID required')}), 400
    
    account = Account.query.get_or_404(account_id)
    
    try:
        # 读取CSV文件
        df = pd.read_csv(file, encoding='utf-8-sig')  # 处理BOM
        
        if df.empty:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400
        
        # 智能表头匹配
        header_mapping = smart_header_mapping(df.columns.tolist()) or {}
        
        # 处理数据
        transactions_data, processing_errors = process_csv_data(df, header_mapping)
        
        if not transactions_data:
            error_msg = _('No valid transactions found in CSV file')
            if processing_errors:
                error_msg += f". {_('Sample errors')}: " + "; ".join(processing_errors[:3])
            return jsonify({'success': False, 'error': error_msg}), 400
        
        # 创建交易记录
        created_count = 0
        failed_count = 0
        skipped_count = 0
        errors = []
        
        for row_data in transactions_data:
            try:
                # 检查是否为重复记录
                trade_date = row_data['trade_date']
                type = row_data['type']
                stock = row_data['stock']
                quantity = row_data['quantity']
                price = row_data['price']
                currency = row_data['currency']
                fee = row_data.get('fee', 0)
                notes = row_data.get('notes', '')
                
                if Transaction.is_duplicate(
                    account_id=account_id,
                    trade_date=trade_date,
                    type=type,
                    stock=stock,
                    quantity=quantity,
                    price=price,
                    currency=currency,
                    fee=fee,
                    notes=notes
                ):
                    skipped_count += 1
                    print(f"DEBUG: Skipping duplicate transaction - {type} {quantity} {stock} on {trade_date}")
                    continue
                
                transaction = Transaction(
                    account_id=account_id,
                    trade_date=trade_date,
                    type=type,
                    stock=stock,
                    quantity=quantity,
                    price=price,
                    currency=currency,
                    fee=fee,
                    notes=notes
                )
                
                db.session.add(transaction)
                created_count += 1
                print(f"DEBUG: Adding new transaction - {type} {quantity} {stock} on {trade_date}")
                
            except Exception as e:
                failed_count += 1
                errors.append(f"Row {row_data.get('row_num', '?')}: {str(e)}")
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': _('CSV import completed'),
            'created_count': created_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,  # 添加跳过的重复记录数量
            'errors': errors[:10],  # 最多返回10个错误
            'redirect_url': f'/transactions?account_id={account_id}'  # 添加重定向URL
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'CSV processing error: {str(e)}'}), 500

def smart_header_mapping(headers):
    """智能匹配CSV表头"""
    # 标准化表头（去除空格、转小写）
    normalized_headers = [h.strip().lower().replace(' ', '_') for h in headers]
    
    # 定义不同券商可能的表头映射
    header_patterns = {
        'date': [
            'date', 'trade_date', 'transaction_date', 'settlement_date',
            '日期', '交易日期', '成交日期', 'fecha', 'datum',
            'transaction_dt', 'trade_dt', 'exec_date', 'execution_date'
        ],
        'type': [
            'type', 'transaction_type', 'action', 'side', 'buy/sell',
            '类型', '交易类型', '操作', 'tipo', 'typ',
            'order_type', 'trans_type', 'activity'
        ],
        'symbol': [
            'symbol', 'stock', 'ticker', 'stock_symbol', 'instrument',
            '股票代码', '代码', '证券代码', 'símbolo', 'symbol',
            'security', 'asset', 'stock_code', 'instrument_code'
        ],
        'quantity': [
            'quantity', 'shares', 'amount', 'qty', 'volume',
            '数量', '股数', '份额', 'cantidad', 'menge',
            'share_quantity', 'units', 'lots'
        ],
        'price': [
            'price', 'unit_price', 'price_per_share', 'execution_price',
            '价格', '单价', '成交价', 'precio', 'preis',
            'avg_price', 'fill_price', 'trade_price'
        ],
        'currency': [
            'currency', 'ccy', 'curr', 'denomination',
            '货币', '币种', 'moneda', 'währung',
            'currency_code', 'ccy_code'
        ],
        'amount': [
            'amount', 'total', 'total_amount', 'cash_amount', 'principal',
            '金额', '总额', '本金', 'cantidad', 'betrag',
            'gross_amount', 'net_amount', 'transaction_amount'
        ],
        'fee': [
            'fee', 'commission', 'fees', 'charges', 'cost',
            '手续费', '佣金', '费用', 'comisión', 'gebühr',
            'transaction_fee', 'brokerage', 'trading_fee'
        ],
        'notes': [
            'notes', 'description', 'comment', 'memo', 'remark',
            '备注', '说明', '描述', 'notas', 'notiz',
            'order_description', 'transaction_description'
        ],
        'account_desc': [
            'account_description', 'account_desc', 'account_name', 'account_type',
            '账户描述', '账户名称', '账户类型', 'descripción_cuenta', 'kontobeschreibung',
            'account', 'acct_desc', 'acct_name', 'portfolio'
        ]
    }
    
    mapping = {}
    
    for field, patterns in header_patterns.items():
        for i, header in enumerate(normalized_headers):
            if any(pattern in header for pattern in patterns):
                mapping[field] = headers[i]  # 使用原始表头名称
                break
    
    # 始终返回mapping，即使没有找到所有必需字段
    # 用户可以在界面上手动调整
    return mapping

def process_csv_data(df, header_mapping):
    """处理CSV数据，标准化格式（委托给统一的处理函数）"""
    # 将header_mapping格式转换为column_mappings格式
    column_mappings = {}
    
    # 映射字段名称
    field_name_mapping = {
        'date': 'date',
        'type': 'type', 
        'symbol': 'symbol',
        'quantity': 'quantity',
        'price': 'price',
        'currency': 'currency',
        'fee': 'fee',
        'notes': 'notes'
    }
    
    for field, column_name in header_mapping.items():
        if field in field_name_mapping and column_name:
            column_mappings[field_name_mapping[field]] = column_name
    
    # 调用统一的处理函数
    return process_csv_with_mapping(df, column_mappings)

# 全局变量来缓存检测到的日期格式
_detected_date_format = None

def reset_date_format_detection():
    """重置日期格式检测"""
    global _detected_date_format
    _detected_date_format = None

def detect_date_format(date_samples):
    """检测CSV文件中的日期格式"""
    print(f"DEBUG: Detecting date format from samples: {date_samples[:5]}")
    
    # 常见日期格式，按优先级排序
    date_formats = [
        '%d/%m/%Y',  # 01/11/2024 = 2024年11月1日 (European format)
        '%m/%d/%Y',  # 01/11/2024 = 2024年1月11日 (US format)
        '%Y/%m/%d',  # 2024/01/11
        '%d-%m-%Y',  # 01-11-2024
        '%m-%d-%Y',  # 01-11-2024
        '%Y-%m-%d',  # 2024-01-11
        '%d.%m.%Y',  # 01.11.2024
        '%m.%d.%Y',  # 01.11.2024
        '%Y.%m.%d',  # 2024.01.11
    ]
    
    format_scores = {}
    
    for fmt in date_formats:
        successful_parses = 0
        valid_dates = []
        
        for sample in date_samples[:10]:  # 检查前10个样本
            if not sample or str(sample).lower() in ['nan', 'none', '']:
                continue
                
            try:
                parsed_date = datetime.strptime(str(sample).strip(), fmt)
                # 检查日期是否合理（比如在1970-2030年范围内）
                if 1970 <= parsed_date.year <= 2030:
                    successful_parses += 1
                    valid_dates.append(parsed_date)
            except ValueError:
                continue
        
        if successful_parses > 0:
            format_scores[fmt] = successful_parses
            print(f"DEBUG: Format {fmt} successfully parsed {successful_parses} dates")
    
    if not format_scores:
        print("DEBUG: No date format detected")
        return None
    
    # 选择成功解析最多样本的格式
    best_format = max(format_scores.items(), key=lambda x: x[1])[0]
    print(f"DEBUG: Best date format detected: {best_format}")
    return best_format

def parse_date(date_str, all_dates=None):
    """智能解析日期，支持格式自动检测"""
    global _detected_date_format
    
    if not date_str or str(date_str).lower() in ['nan', 'none', '']:
        return None
    
    date_str = str(date_str).strip()
    
    # 如果还没有检测到格式，且提供了所有日期样本，则检测格式
    if _detected_date_format is None and all_dates:
        _detected_date_format = detect_date_format(all_dates)
    
    # 如果检测到了格式，优先使用
    if _detected_date_format:
        try:
            return datetime.strptime(date_str, _detected_date_format).date()
        except ValueError:
            print(f"DEBUG: Failed to parse '{date_str}' with detected format {_detected_date_format}")
    
    # 如果检测格式失败，回退到逐个尝试
    fallback_formats = [
        '%d/%m/%Y',  # 优先使用欧洲格式
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%m-%d-%Y',
        '%d.%m.%Y',
        '%m.%d.%Y',
        '%Y.%m.%d',
        '%Y年%m月%d日',
        '%m月%d日%Y年'
    ]
    
    for fmt in fallback_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    print(f"DEBUG: Failed to parse date '{date_str}' with any format")
    return None

def parse_transaction_type(type_str):
    """智能解析交易类型"""
    if not type_str:
        return None
    
    type_str = type_str.upper().strip()
    print(f"DEBUG: parse_transaction_type - Input: '{type_str}'")
    
    # 买入关键词
    buy_keywords = ['BUY', 'BOUGHT', 'PURCHASE', 'LONG', '买入', '买', 'COMPRA', 'KAUF']
    # 卖出关键词  
    sell_keywords = ['SELL', 'SOLD', 'SALE', 'SHORT', '卖出', '卖', 'VENTA', 'VERKAUF']
    # 分红关键词
    dividend_keywords = ['DIVIDEND', 'DIV', 'DIVIDENDE', '分红', '股息', 'DISTRIBUTION', 'DIST']
    # 利息关键词
    interest_keywords = ['INTEREST', 'INT', '利息', 'ZINSEN', 'INTERÉS']
    # 存款/转入关键词 (包括CONTRIBUTION)
    deposit_keywords = ['DEPOSIT', 'TRANSFER IN', 'CASH RECEIPT', 'CONTRIBUTION', '存入', '转入', 'DEPÓSITO', 'EINZAHLUNG']
    # 取款/转出关键词
    withdrawal_keywords = ['WITHDRAWAL', 'TRANSFER OUT', 'CASH PAYMENT', '取出', '转出', 'RETIRO', 'AUSZAHLUNG']
    # 费用关键词
    fee_keywords = ['FEE', 'CHARGE', 'COMMISSION', '费用', '手续费', 'GEBÜHR', 'CARGO']
    
    # 按优先级检查各种交易类型
    if any(keyword in type_str for keyword in dividend_keywords):
        print(f"DEBUG: parse_transaction_type - Matched DIVIDEND for '{type_str}'")
        return 'DIVIDEND'
    elif any(keyword in type_str for keyword in interest_keywords):
        print(f"DEBUG: parse_transaction_type - Matched INTEREST for '{type_str}'")
        return 'INTEREST'  
    elif any(keyword in type_str for keyword in deposit_keywords):
        print(f"DEBUG: parse_transaction_type - Matched DEPOSIT for '{type_str}'")
        return 'DEPOSIT'
    elif any(keyword in type_str for keyword in withdrawal_keywords):
        print(f"DEBUG: parse_transaction_type - Matched WITHDRAWAL for '{type_str}'")
        return 'WITHDRAWAL'
    elif any(keyword in type_str for keyword in fee_keywords):
        print(f"DEBUG: parse_transaction_type - Matched FEE for '{type_str}'")
        return 'FEE'
    elif any(keyword in type_str for keyword in buy_keywords):
        print(f"DEBUG: parse_transaction_type - Matched BUY for '{type_str}'")
        return 'BUY'
    elif any(keyword in type_str for keyword in sell_keywords):
        print(f"DEBUG: parse_transaction_type - Matched SELL for '{type_str}'")
        return 'SELL'
    
    # 如果没有匹配到，返回None表示无效类型
    print(f"DEBUG: parse_transaction_type - Unknown transaction type '{type_str}' - please add mapping")
    return None

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