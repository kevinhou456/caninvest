"""
数据导入API（CSV和OCR）
"""

import io
import json
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


def _build_encoding_candidates(primary_encodings=None):
    """Combine preferred encodings with sensible fallbacks."""
    fallback_encodings = [
        'utf-8-sig', 'utf-8', 'gb2312', 'gbk', 'gb18030', 'big5',
        'latin1', 'cp1252', 'iso-8859-1'
    ]

    primary_list = []
    if primary_encodings:
        if isinstance(primary_encodings, (list, tuple, set)):
            primary_list.extend(primary_encodings)
        else:
            primary_list.append(primary_encodings)

    ordered = primary_list + fallback_encodings
    seen = set()
    result = []
    for encoding in ordered:
        if not encoding:
            continue
        key = encoding.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(encoding)
    return result


def _parse_csv_bytes(file_bytes, preferred_encodings=None):
    """Decode CSV bytes, detect header row, and return parsed DataFrame with metadata."""
    if not file_bytes:
        raise ValueError('CSV file is empty')

    import pandas as pd
    from app.utils.csv_utils import analyze_csv_content

    encodings_to_try = _build_encoding_candidates(preferred_encodings)
    last_error = None

    for encoding in encodings_to_try:
        try:
            decoded_content = file_bytes.decode(encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue

        if not decoded_content.strip():
            continue

        analysis = analyze_csv_content(decoded_content)
        delimiter = analysis.get('delimiter') or ','
        header_index = int(max(0, analysis.get('header_index', 0)))

        stream = io.StringIO(decoded_content)
        try:
            df = pd.read_csv(
                stream,
                sep=delimiter,
                skiprows=header_index,
                skipinitialspace=True
            )
        except Exception as exc:
            last_error = exc
            continue

        if df.empty and analysis.get('field_count', 0) == 0:
            last_error = ValueError('No structured rows detected in CSV content')
            continue

        return {
            'dataframe': df,
            'encoding': encoding,
            'delimiter': delimiter,
            'header_index': header_index,
            'analysis': analysis
        }

    raise ValueError(last_error or 'Unable to decode CSV with available encodings')


def _load_csv_dataframe(file_path, meta_path=None, preferred_encodings=None):
    """Load DataFrame from CSV file using stored metadata or automatic detection."""
    import pandas as pd

    meta = None
    if meta_path and os.path.exists(meta_path):
        try:
            with open(meta_path, 'r', encoding='utf-8') as meta_file:
                meta = json.load(meta_file)
        except Exception:
            meta = None

    if meta:
        encoding = meta.get('encoding') or 'utf-8-sig'
        delimiter = meta.get('delimiter') or ','
        header_index = int(max(0, meta.get('header_index', 0)))
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            sep=delimiter,
            skiprows=header_index,
            skipinitialspace=True
        )
        return df, meta

    with open(file_path, 'rb') as fh:
        file_bytes = fh.read()

    parsed = _parse_csv_bytes(file_bytes, preferred_encodings)
    meta = {
        'encoding': parsed['encoding'],
        'delimiter': parsed['delimiter'],
        'header_index': parsed['header_index']
    }

    if meta_path:
        try:
            with open(meta_path, 'w', encoding='utf-8') as meta_file:
                json.dump(meta, meta_file)
        except Exception:
            pass

    return parsed['dataframe'], meta
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
        # 重置文件指针
        file.seek(0)
        
        # 先读取原始字节内容进行编码检测
        raw_content = file.read(2048)
        file.seek(0)
        
        # 检查文件是否为空
        if not raw_content:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400
        
        
        # 详细编码分析和检测
        def analyze_and_detect_encoding(raw_bytes):
            analysis_info = {
                'file_size': len(raw_bytes),
                'chardet_result': None,
                'manual_tests': [],
                'detected_encoding': None,
                'decoded_content': None
            }
            
            # 使用chardet进行检测
            try:
                import chardet
                chardet_result = chardet.detect(raw_bytes)
                analysis_info['chardet_result'] = chardet_result
            except ImportError:
                pass
            
            return analysis_info
        
        # 执行详细分析
        encoding_analysis = analyze_and_detect_encoding(raw_content)
        
        # 如果chardet检测到的置信度很低或无法检测，尝试常用编码并跳过分析模式
        chardet_result = encoding_analysis.get('chardet_result')

        # 更宽松的编码检测策略，优先尝试常用编码
        detected_encoding = None
        if chardet_result and chardet_result.get('encoding') and chardet_result.get('confidence', 0) >= 0.3:
            detected_encoding = chardet_result.get('encoding')
        else:
            # 如果chardet检测不可靠，尝试常用编码
            common_encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'latin1', 'cp1252']
            for encoding in common_encodings:
                try:
                    raw_content.decode(encoding)
                    detected_encoding = encoding
                    break
                except UnicodeDecodeError:
                    continue

        # 只有在完全无法解码时才触发分析模式
        if not detected_encoding:
            return jsonify({
                'success': False,
                'error': 'Encoding detection required',
                'encoding_analysis': {
                    'file_size': len(raw_content),
                    'chardet_result': chardet_result or {'encoding': None, 'confidence': 0.0, 'language': None},
                    'raw_sample': raw_content[:200].hex(),  # 十六进制表示
                    'ascii_sample': ''.join(chr(b) if 32 <= b <= 126 else f'\\x{b:02x}' for b in raw_content[:100]),
                    'message': f'检测到的编码: {(chardet_result or {}).get("encoding", "None")} (置信度: {(chardet_result or {}).get("confidence", 0):.2f})'
                }
            }), 400

        # 使用已检测到的编码进行解码
        try:
            decoded_content = raw_content.decode(detected_encoding)
        except UnicodeDecodeError:
            # 如果失败，使用latin1作为最后备选
            detected_encoding = 'latin1'
            decoded_content = raw_content.decode(detected_encoding, errors='ignore')
        
        # 检查解码后的内容是否为空
        if not decoded_content.strip():
            return jsonify({'success': False, 'error': _('CSV file is empty or contains only whitespace')}), 400
        
        # 读取完整文件内容以进行表头检测
        file.seek(0)
        full_bytes = file.read()
        if not full_bytes:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400

        encodings_to_try = _build_encoding_candidates(detected_encoding)

        try:
            parsed_csv = _parse_csv_bytes(full_bytes, encodings_to_try)
        except ValueError as parse_error:
            return jsonify({'success': False, 'error': str(parse_error)}), 400

        df = parsed_csv['dataframe']
        successful_encoding = parsed_csv['encoding']
        delimiter = parsed_csv['delimiter']
        header_index = parsed_csv['header_index']


        if df.empty:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400

        if len(df.columns) == 0:
            return jsonify({'success': False, 'error': _('No columns found in CSV file')}), 400
        
        # 生成会话ID并临时保存文件
        session_id = str(uuid.uuid4())
        temp_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', '/tmp'), 'csv_sessions')
        os.makedirs(temp_dir, exist_ok=True)
        
        temp_file_path = os.path.join(temp_dir, f'{session_id}.csv')
        meta_file_path = os.path.join(temp_dir, f'{session_id}.json')
        
        # 保存CSV原始内容和解析元数据
        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(full_bytes)

        try:
            with open(meta_file_path, 'w', encoding='utf-8') as meta_file:
                json.dump({
                    'encoding': successful_encoding,
                    'delimiter': delimiter,
                    'header_index': header_index
                }, meta_file)
        except Exception:
            pass

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
        else:
            # 没有找到匹配，使用智能建议
            suggested_mapping = smart_header_mapping(columns) or {}
        
        # 检查用户是否手动选择了其他格式（优先级较低）
        saved_format_name = request.form.get('saved_format', '').strip()
        format_name = request.form.get('format_name', '').strip()

        if saved_format_name and not auto_matched_format:
            # 只在没有自动匹配时才使用用户选择的格式
            manual_format = CsvFormat.get_by_name(saved_format_name)
            if manual_format:
                suggested_mapping = manual_format.mappings

        # 检测是否包含CFP_Account_ID列（系统导出的文件标识）
        cfp_account_id_detected = 'CFP_Account_ID' in columns
        cfp_account_data = {}

        if cfp_account_id_detected:
            # 获取文件中的账户ID信息
            unique_account_ids = df['CFP_Account_ID'].dropna().unique().tolist()
            # 获取对应的账户信息
            from app.models.account import Account
            from app.services.account_service import AccountService
            account_info = {}
            for account_id in unique_account_ids:
                try:
                    account_id = int(account_id)
                    account = Account.query.get(account_id)
                    if account:
                        account_info[account_id] = {
                            'name': AccountService.get_account_name_with_members(account),
                            'type': account.account_type.name if account.account_type else 'Unknown',
                            'transaction_count': len(df[df['CFP_Account_ID'] == account_id])
                        }
                except (ValueError, AttributeError):
                    continue

            cfp_account_data = {
                'detected': True,
                'account_ids': unique_account_ids,
                'account_info': account_info,
                'total_transactions': len(df)
            }
        else:
            cfp_account_data = {'detected': False}

        # 计算日期范围（若存在日期列，尝试模糊匹配）
        date_range = None
        date_col = None
        for col in df.columns:
            lower = col.lower()
            if 'date' == lower or 'trade_date' == lower or 'trade date' == lower or lower.endswith('date'):
                date_col = col
                break
        if date_col:
            try:
                parsed_dates = pd.to_datetime(df[date_col], errors='coerce')
                parsed_dates = parsed_dates.dropna()
                if not parsed_dates.empty:
                    start_date = parsed_dates.min().date().isoformat()
                    end_date = parsed_dates.max().date().isoformat()
                    date_range = {'start': start_date, 'end': end_date}
            except Exception:
                date_range = None

        return jsonify({
            'success': True,
            'session_id': session_id,
            'columns': columns,
            'preview_data': preview_data,
            'suggested_mapping': suggested_mapping,
            'total_rows': len(df),
            'date_range': date_range,
            'auto_matched_format': {
                'name': auto_matched_format.format_name,
                'usage_count': auto_matched_format.usage_count
            } if auto_matched_format else None,
            'cfp_account_data': cfp_account_data
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'CSV analysis error: {str(e)}'}), 500


@bp.route('/import-csv-smart', methods=['POST'])
def import_csv_smart():
    """智能导入CSV文件（使用CFP_Account_ID）"""
    import pandas as pd
    import os
    from app.models.transaction import Transaction
    from app.models.account import Account
    from datetime import datetime

    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': _('No data provided')}), 400

    session_id = data.get('session_id')
    use_cfp_account_id = data.get('use_cfp_account_id', False)

    if not session_id or not use_cfp_account_id:
        return jsonify({'success': False, 'error': _('Missing required data')}), 400

    try:
        # 加载临时保存的CSV文件
        temp_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', '/tmp'), 'csv_sessions')
        temp_file_path = os.path.join(temp_dir, f'{session_id}.csv')

        if not os.path.exists(temp_file_path):
            return jsonify({'success': False, 'error': _('Session expired or file not found')}), 400

        meta_file_path = os.path.join(temp_dir, f'{session_id}.json')
        df, _meta = _load_csv_dataframe(temp_file_path, meta_file_path)

        # 验证是否包含CFP_Account_ID列
        if 'CFP_Account_ID' not in df.columns:
            return jsonify({'success': False, 'error': _('CFP_Account_ID column not found in file')}), 400

        imported_count = 0
        skipped_count = 0
        error_count = 0
        error_details = []

        # 收集成功导入的账户ID，用于生成跳转链接
        successful_account_ids = []

        # 按账户ID分组处理
        for account_id, group in df.groupby('CFP_Account_ID'):
            try:
                account_id = int(account_id)
                account = Account.query.get(account_id)

                if not account:
                    skipped_count += len(group)
                    error_details.append(f'Account ID {account_id} not found - skipped {len(group)} transactions')
                    continue

                # 记录成功处理的账户
                account_has_imports = False

                # 处理该账户的交易记录
                for index, row in group.iterrows():
                    try:
                        # 解析交易数据
                        transaction_data = {
                            'trade_date': pd.to_datetime(row.get('Date', '')).date(),
                            'type': row.get('Type', ''),
                            'stock': row.get('Stock Symbol', '') or None,
                            'quantity': float(row.get('Quantity', 0)) if row.get('Quantity', '') else 0,
                            'price': float(row.get('Price Per Share', 0)) if row.get('Price Per Share', '') else 0,
                            'fee': float(row.get('Transaction Fee', 0)) if row.get('Transaction Fee', '') else 0,
                            'currency': row.get('Currency', 'CAD'),
                            'notes': row.get('Notes', '') or None,
                            'account_id': account_id
                        }

                        # 验证必需字段
                        if not transaction_data['type'] or not transaction_data['trade_date']:
                            skipped_count += 1
                            continue

                        # 检查是否已存在相同的交易
                        # 对于存入/取出交易，使用不同的重复检测逻辑
                        if transaction_data['type'] in ['DEPOSIT', 'WITHDRAWAL']:
                            existing = Transaction.query.filter_by(
                                account_id=account_id,
                                trade_date=transaction_data['trade_date'],
                                type=transaction_data['type'],
                                quantity=transaction_data['quantity'],
                                currency=transaction_data['currency']
                            ).first()
                        else:
                            # 股票买卖交易的重复检测
                            existing = Transaction.query.filter_by(
                                account_id=account_id,
                                trade_date=transaction_data['trade_date'],
                                type=transaction_data['type'],
                                stock=transaction_data['stock'],
                                quantity=transaction_data['quantity'],
                                price=transaction_data['price']
                            ).first()

                        if existing:
                            skipped_count += 1
                            continue

                        # 创建新交易记录
                        transaction = Transaction(**transaction_data)
                        db.session.add(transaction)
                        imported_count += 1
                        account_has_imports = True

                    except Exception as row_error:
                        error_count += 1
                        error_details.append(f'Row {index}: {str(row_error)}')
                        continue

                # 如果该账户有成功导入的交易，记录账户ID
                if account_has_imports and account_id not in successful_account_ids:
                    successful_account_ids.append(account_id)

            except Exception as account_error:
                error_count += len(group)
                error_details.append(f'Account {account_id}: {str(account_error)}')
                continue

        # 提交所有更改
        db.session.commit()

        # 清理临时文件
        for path in (temp_file_path, meta_file_path):
            try:
                os.remove(path)
            except Exception:
                pass

        # 生成跳转URL
        redirect_url = '/transactions'  # 默认跳转到所有交易记录
        if len(successful_account_ids) == 1:
            # 如果只有一个账户成功导入，跳转到该账户的交易记录页面
            redirect_url = f'/transactions?account_id={successful_account_ids[0]}'
        elif len(successful_account_ids) > 1:
            # 如果有多个账户，跳转到所有交易记录页面
            redirect_url = '/transactions'

        result = {
            'success': True,
            'imported_count': imported_count,
            'skipped_count': skipped_count,
            'error_count': error_count,
            'redirect_url': redirect_url
        }

        if error_details:
            result['error_details'] = error_details[:10]  # 只返回前10个错误

        return jsonify(result)

    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': f'Smart import error: {str(e)}'}), 500

@bp.route('/import-csv-with-mapping', methods=['POST'])
def import_csv_with_mapping():
    """使用用户映射导入CSV"""
    import pandas as pd
    import os
    from app.models.transaction import Transaction
    from datetime import date
    import calendar
    
    # 重置日期格式检测，确保每次导入都重新检测
    reset_date_format_detection()
    
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': _('No data provided')}), 400
    
    session_id = data.get('session_id')
    account_id = data.get('account_id')
    column_mappings = data.get('column_mappings', {})
    format_name = data.get('format_name', '').strip()
    overwrite_range = bool(data.get('overwrite_range', False))
    extend_to_full_month = bool(data.get('extend_to_full_month', False))
    
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
        
        meta_file_path = os.path.join(temp_dir, f'{session_id}.json')
        df, _meta = _load_csv_dataframe(temp_file_path, meta_file_path)
        
        # 处理数据
        transactions_data, processing_errors = process_csv_with_mapping(df, column_mappings)
        
        if not transactions_data:
            error_msg = _('No valid transactions found in CSV file')
            if processing_errors:
                error_msg += f". {_('Sample errors')}: " + "; ".join(processing_errors[:3])
            return jsonify({'success': False, 'error': error_msg}), 400

        # 计算覆盖范围（按交易日期）
        deleted_count = 0
        if overwrite_range:
            dates = [row['trade_date'] for row in transactions_data if row.get('trade_date')]
            if dates:
                start_date = min(dates)
                end_date = max(dates)

                # 可选扩展：当导入记录只在同一月份内时，扩展覆盖到整月
                if extend_to_full_month and start_date and end_date:
                    if start_date.year == end_date.year and start_date.month == end_date.month:
                        start_date = date(start_date.year, start_date.month, 1)
                        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
                        end_date = date(start_date.year, start_date.month, last_day)
                    else:
                        extend_to_full_month = False

                try:
                    deleted_count = Transaction.query.filter(
                        Transaction.account_id == account_id,
                        Transaction.trade_date >= start_date,
                        Transaction.trade_date <= end_date
                    ).delete(synchronize_session=False)
                    db.session.flush()
                except Exception as exc:
                    current_app.logger.exception("Failed to overwrite transactions in range", exc_info=exc)
                    return jsonify({'success': False, 'error': _('Failed to clear existing transactions before import')}), 500
        
        # 创建交易记录
        created_count = 0
        failed_count = 0
        skipped_count = 0
        corrected_count = 0
        errors = []

        
        for row_data in transactions_data:
            try:
                # 检查股票代码是否需要矫正
                original_stock = row_data['stock']
                if original_stock:
                    currency = row_data.get('currency', 'USD')
                    
                    # 第一步：如果交易记录币种是CAD,而股票代码结尾不是.TO则自动添加.TO
                    processed_stock = original_stock
                    if currency == 'CAD' and not processed_stock.upper().endswith('.TO'):
                        processed_stock = processed_stock + '.TO'
                        # print(f"CAD币种股票代码自动添加后缀: {original_stock} -> {processed_stock}")
                        row_data['stock'] = processed_stock
                    
                    # 第二步：检查股票代码矫正表
                    from app.models.stock_symbol_correction import StockSymbolCorrection
                    corrected_stock = StockSymbolCorrection.get_corrected_symbol(processed_stock, currency)
                    if corrected_stock != processed_stock.upper():
                        # print(f"股票代码矫正: {processed_stock}({currency}) -> {corrected_stock}")
                        row_data['stock'] = corrected_stock
                        corrected_count += 1
                    
                    # 第三步：检查是否存在不同币种相同代码的交易记录
                    final_stock = row_data['stock']
                    from app.models.transaction import Transaction
                    existing_currency = Transaction.get_currency_by_stock_symbol(final_stock)
                    if existing_currency and existing_currency != currency:
                        error_msg = f"股票 {final_stock} 已存在使用 {existing_currency} 币种的交易记录，不允许导入使用 {currency} 币种的记录。同一股票代码只能使用一种货币。"
                        print(f"币种冲突检测: {error_msg}")
                        errors.append(error_msg)
                        failed_count += 1
                        continue
                
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

                # 只检查数据库中是否已存在重复记录
                if Transaction.is_duplicate(
                    account_id=account_id,
                    trade_date=trade_date,
                    type=type,
                    stock=stock,
                    quantity=quantity,
                    price=price,
                    currency=currency,
                    fee=fee,
                    notes=notes,
                    amount=amount
                ):
                    skipped_count += 1
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
                
            except Exception as e:
                failed_count += 1
                errors.append(f"Row {row_data.get('row_num', '?')}: {str(e)}")
        
        db.session.commit()
        
        # 保存格式映射以备将来使用
        if created_count > 0:
            original_headers = df.columns.tolist()
            
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
        for path in (temp_file_path, meta_file_path):
            try:
                os.remove(path)
            except Exception:
                pass
        
        return jsonify({
            'success': True,
            'message': _('CSV import completed'),
            'created_count': created_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,  # 添加跳过的重复记录数量
            'corrected_count': corrected_count,  # 添加矫正的股票代码数量
            'deleted_count': deleted_count,
            'errors': errors[:10],  # 最多返回10个错误
            'redirect_url': f'/transactions?account_id={account_id}'  # 添加重定向URL
        })
        
    except Exception as e:
        current_app.logger.exception('import_csv_with_mapping failed')
        return jsonify({'success': False, 'error': f'Import error: {str(e)}'}), 500

def process_csv_with_mapping(df, column_mappings):
    """使用用户映射处理CSV数据"""
    global _detected_date_format
    _detected_date_format = None  # 重置日期格式检测
    
    transactions = []
    processing_errors = []
    cash_out_keywords = {'to reg. act.', 'to reg act.', 'to reg act', 'to reg. account', 'to reg account'}
    
    
    # 检测是否为描述格式的CSV
    # 方式1: 检查CSV列名是否包含标准描述格式列 (date, transaction, description, amount, balance, currency)
    description_format_columns = {'date', 'transaction', 'description', 'amount', 'balance', 'currency'}
    csv_columns_set = set(df.columns.str.lower())
    is_description_format_by_columns = description_format_columns.issubset(csv_columns_set)
    
    # 方式2: 检查用户映射 - 如果多个重要字段都映射到同一个描述列，则可能需要描述解析
    description_column_mapping = None
    fields_mapped_to_description = []
    
    for field, column in column_mappings.items():
        if column and 'description' in column.lower():
            description_column_mapping = column
            fields_mapped_to_description.append(field)
    
    # 如果股票代码、数量、价格等多个字段都映射到描述列，启用描述解析
    # 支持不同的字段名变体
    important_fields = {
        'stock_symbol', 'symbol', 
        'quantity', 'qty',
        'price', 
        'transaction_type', 'type'
    }
    is_description_format_by_mapping = (
        description_column_mapping and 
        len(set(fields_mapped_to_description) & important_fields) >= 2
    )
    
    if is_description_format_by_columns or is_description_format_by_mapping:
        if is_description_format_by_columns:
            print("DEBUG: 检测到描述格式CSV（按列名），使用专门的解析器处理")
        else:
            print(f"DEBUG: 检测到描述格式CSV（按映射），字段 {fields_mapped_to_description} 都映射到 '{description_column_mapping}' 列，使用专门的解析器处理")
        return process_description_format_csv(df, column_mappings)
    
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
                raw_type_str = str(row[column_mappings['type']]).strip()
                type_str = raw_type_str.upper()
                # 特殊描述关键字：To Reg. Act. 视为提现
                description_text_for_type = ''
                if 'description' in column_mappings and column_mappings['description']:
                    description_text_for_type = str(row[column_mappings['description']]).strip().lower()
                if description_text_for_type in cash_out_keywords or raw_type_str.lower() in cash_out_keywords:
                    type_str = 'WITHDRAWAL'
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
                mapped_column = column_mappings['currency']
                curr_str = str(row[mapped_column]).strip().upper()

                # 检查映射的列名是否合理 - 排除明显不相关的列
                column_name_lower = mapped_column.lower()
                invalid_currency_columns = ['balance', 'total', 'amount', 'price', 'value', 'quantity', 'shares']

                # 如果列名包含这些关键词，跳过货币提取
                if any(invalid_word in column_name_lower for invalid_word in invalid_currency_columns):
                    currency = 'USD'  # 使用默认值
                elif curr_str and curr_str != 'NAN':
                    # 如果是标准的3位货币代码，直接使用
                    if len(curr_str) == 3 and curr_str.isalpha():
                        currency = curr_str
                    else:
                        # 从文本中智能提取货币（如 "CAD RRSP" -> "CAD", "Account Description USD" -> "USD"）
                        if 'CAD' in curr_str or 'CANADIAN' in curr_str:
                            currency = 'CAD'
                        elif 'USD' in curr_str or 'US ' in curr_str or 'AMERICAN' in curr_str:
                            currency = 'USD'
                        elif 'EUR' in curr_str or 'EURO' in curr_str:
                            currency = 'EUR'
                
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
            
        except Exception as e:
            error_msg = f"Row {row_num}: Unexpected error - {str(e)}"
            processing_errors.append(error_msg)
            continue

    if processing_errors:
        pass

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
        file.seek(0)
        full_bytes = file.read()
        if not full_bytes:
            return jsonify({'success': False, 'error': _('CSV file is empty')}), 400

        try:
            parsed_csv = _parse_csv_bytes(full_bytes)
        except ValueError as parse_error:
            return jsonify({'success': False, 'error': str(parse_error)}), 400

        df = parsed_csv['dataframe']
        
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
            'type', 'transaction_type', 'action', 'Operation', 'buy/sell',
            '类型', '交易类型', '操作', 'tipo', 'typ',
            'order_type', 'trans_type', 'activity','Operation'
        ],
        'symbol': [
            'symbol', 'stock', 'ticker', 'stock_symbol', 'instrument',
            '股票代码', '代码', '证券代码', 'símbolo', 'symbol',
            'security', 'asset', 'stock_code', 'instrument_code'
        ],
        'quantity': [
            'quantity', 'shares', 'qty', 'volume',
            '数量', '股数', '份额', 'cantidad', 'menge',
            'share_quantity', 'units', 'lots'
        ],
        'price': [
            'price', 'unit_price', 'price_per_share', 'execution_price',
            '价格', '单价', '成交价', 'precio', 'preis',
            'avg_price', 'fill_price', 'trade_price'
        ],
        'currency': [
            'currency', 'ccy', 'denomination',
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
    
    # 如果检测格式失败，回退到逐个尝试，包括自然语言格式
    fallback_formats = [
        '%d/%m/%Y',           # 优先使用欧洲格式
        '%Y-%m-%d',           # ISO format
        '%m/%d/%Y',           # US format
        '%Y/%m/%d',
        '%d-%m-%Y',
        '%m-%d-%Y',
        '%d.%m.%Y',
        '%m.%d.%Y',
        '%Y.%m.%d',
        '%B %d, %Y',          # September 5, 2025
        '%b %d, %Y',          # Sep 5, 2025
        '%d %B %Y',           # 5 September 2025
        '%d %b %Y',           # 5 Sep 2025
        '%B %d %Y',           # September 5 2025 (no comma)
        '%b %d %Y',           # Sep 5 2025 (no comma)
        '%Y年%m月%d日',
        '%m月%d日%Y年'
    ]
    
    for fmt in fallback_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # 如果标准格式都无法解析，尝试使用dateutil作为最终回退
    try:
        from dateutil import parser
        return parser.parse(date_str).date()
    except:
        pass
    
    print(f"DEBUG: Failed to parse date '{date_str}' with any format")
    return None

def process_description_format_csv(df, column_mappings):
    """处理描述格式的CSV - 支持标准列名或用户映射"""
    from app.utils.transaction_parser import TransactionDescriptionParser
    
    transactions = []
    processing_errors = []
    
    print(f"DEBUG: 使用描述格式解析器处理 {len(df)} 行数据")
    print(f"DEBUG: 列映射: {column_mappings}")
    
    # 创建解析器
    parser = TransactionDescriptionParser()
    
    for index, row in df.iterrows():
        row_num = index + 2
        try:
            # 根据用户映射或标准列名获取数据
            def get_field_value(field_names, default=''):
                """根据映射获取字段值，支持多个字段名变体"""
                if isinstance(field_names, str):
                    field_names = [field_names]

                for field_name in field_names:
                    if field_name in column_mappings and column_mappings[field_name]:
                        # 使用用户映射的列
                        mapped_column = column_mappings[field_name]
                        return str(row.get(mapped_column, default)).strip()

                # 尝试标准列名 - 只进行精确匹配，不进行substring匹配
                for field_name in field_names:
                    if field_name in row:
                        return str(row.get(field_name, default)).strip()

                return default
            
            # 构造行字典
            row_dict = {
                'date': get_field_value(['date']),
                'transaction': get_field_value(['transaction_type', 'type', 'transaction']),
                'description': get_field_value(['description']),
                'amount': get_field_value(['amount'], '0'),
                'balance': get_field_value(['balance'], '0'),
                'currency': get_field_value(['currency'], 'USD')
            }
            
            print(f"DEBUG: Row {row_num} 数据: {row_dict}")
            
            # 使用解析器处理
            parsed_data = parser.parse_csv_row(row_dict)
            
            if not parsed_data.get('parsed', False):
                processing_errors.append(f"Row {row_num}: 无法解析描述 - {parsed_data.get('notes', '')}")
                continue
            
            # 转换日期格式
            trade_date = parse_date(parsed_data['date'])
            if not trade_date:
                processing_errors.append(f"Row {row_num}: 无效日期 '{parsed_data['date']}'")
                continue
            
            # 构建交易数据 - 转换为主函数期望的字段名
            transaction_data = {
                'row_num': row_num,
                'trade_date': trade_date,
                'type': parsed_data['transaction_type'],  # 主函数期望 'type'
                'stock': parsed_data['symbol'] or '',     # 主函数期望 'stock'
                'quantity': parsed_data['quantity'],
                'price': parsed_data['price'],
                'currency': parsed_data['currency'],
                'fee': 0.0,  # 默认手续费为0
                'notes': parsed_data['notes'],
                'amount': parsed_data['amount']  # 添加 amount 字段
            }
            
            transactions.append(transaction_data)
            
        except Exception as e:
            error_msg = f"Row {row_num}: 处理行时出错 - {str(e)}"
            processing_errors.append(error_msg)
            print(f"ERROR: {error_msg}")
            continue
    
    print(f"DEBUG: 描述格式解析完成，成功 {len(transactions)} 行，错误 {len(processing_errors)} 行")
    return transactions, processing_errors

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
    # 存款/转入关键词 (包括CONTRIBUTION和CONT)
    deposit_keywords = ['DEPOSIT', 'TRANSFER IN', 'CASH RECEIPT', 'CONTRIBUTION', 'CONT', '存入', '转入', 'DEPÓSITO', 'EINZAHLUNG']
    # 取款/转出关键词
    withdrawal_keywords = [
        'WITHDRAWAL', 'WITHDRAW', 'TRANSFER OUT', 'CASH PAYMENT',
        '取出', '转出', 'RETIRO', 'AUSZAHLUNG',
        'TO REG. ACT', 'TO REG ACT', 'TO REG. ACCOUNT', 'TO REG ACCOUNT'
    ]
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
