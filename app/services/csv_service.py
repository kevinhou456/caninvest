"""
CSV交易记录导入导出服务
"""

import os
import csv
import uuid
import tempfile
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from flask import current_app
from app import db
from app.models.transaction import Transaction
# from app.models.stock import Stock, StockCategory  # Stock models deleted
from app.models.stocks_cache import StocksCache
from app.models.account import Account
from app.models.import_task import ImportTask, TaskStatus
from app.models.stock_symbol_correction import StockSymbolCorrection


class CSVTransactionService:
    """CSV交易记录处理服务"""
    
    SUPPORTED_BROKERS = {
        'questrade': 'Questrade',
        'td': 'TD Direct Investing',
        'interactive_brokers': 'Interactive Brokers',
        'wealthsimple': 'Wealthsimple Trade',
        'generic': 'Generic CSV Format'
    }
    
    def __init__(self):
        self.broker_parsers = {
            'questrade': self._parse_questrade_csv,
            'td': self._parse_td_csv,
            'interactive_brokers': self._parse_ib_csv,
            'wealthsimple': self._parse_wealthsimple_csv,
            'generic': self._parse_generic_csv
        }
    
    def process_csv_import(self, import_task_id: int):
        """处理CSV导入任务"""
        task = ImportTask.query.get(import_task_id)
        if not task:
            raise ValueError(f"Import task {import_task_id} not found")
        
        task.status = TaskStatus.PROCESSING
        task.started_at = datetime.now()
        db.session.commit()
        
        try:
            # 解析CSV文件
            transactions_data = self._parse_csv_file(task.file_path, task.broker_format)
            task.processed_rows = len(transactions_data)
            
            # 导入交易记录
            imported_count, failed_count, errors, corrected_count = self._import_transactions(
                transactions_data, task.account_id
            )
            
            # 保存矫正统计信息到任务结果中
            task.corrected_count = corrected_count
            
            task.imported_count = imported_count
            task.failed_count = failed_count
            task.skipped_count = task.processed_rows - imported_count - failed_count
            
            if failed_count > 0:
                task.error_details = '\n'.join(errors[:10])  # 保存前10个错误
            
            task.status = TaskStatus.COMPLETED if failed_count == 0 else TaskStatus.COMPLETED
            task.completed_at = datetime.now()
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error_details = str(e)
            task.completed_at = datetime.now()
        
        db.session.commit()
        return task
    
    def _parse_csv_file(self, file_path: str, broker_format: str) -> List[Dict]:
        """解析CSV文件"""
        if broker_format not in self.broker_parsers:
            raise ValueError(f"Unsupported broker format: {broker_format}")
        
        parser = self.broker_parsers[broker_format]
        
        with open(file_path, 'r', encoding='utf-8') as file:
            # 尝试检测CSV方言
            sample = file.read(1024)
            file.seek(0)
            
            try:
                # 使用csv.Sniffer自动检测方言
                dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
            except csv.Error:
                # 如果自动检测失败，手动检测最常用的分隔符
                if ';' in sample and sample.count(';') > sample.count(','):
                    class SemicolonDialect(csv.excel):
                        delimiter = ';'
                    dialect = SemicolonDialect()
                elif '\t' in sample and sample.count('\t') > sample.count(','):
                    class TabDialect(csv.excel):
                        delimiter = '\t'
                    dialect = TabDialect()
                else:
                    dialect = csv.excel
            
            reader = csv.DictReader(file, dialect=dialect)
            return parser(reader)
    
    def _parse_questrade_csv(self, reader) -> List[Dict]:
        """解析Questrade CSV格式"""
        transactions = []
        
        for row in reader:
            # Questrade CSV格式示例
            # Settlement Date,Transaction Date,Action,Symbol,Description,Quantity,Price,Gross Amount,Commission,Net Amount,Currency
            try:
                trade_date = datetime.strptime(row['Transaction Date'], '%m/%d/%Y').date()
                settlement_date = datetime.strptime(row['Settlement Date'], '%m/%d/%Y').date()
                
                # 处理股票符号
                symbol = row['Symbol'].strip().upper()
                if not symbol:
                    continue
                
                # 映射交易类型
                action = row['Action'].upper()
                if action in ['BUY', 'SELL']:
                    transaction_type = action
                elif action in ['DIV', 'DIVIDEND']:
                    transaction_type = 'DIVIDEND'
                else:
                    continue  # 跳过不支持的交易类型
                
                transactions.append({
                    'symbol': symbol,
                    'name': row.get('Description', '').strip(),
                    'transaction_type': transaction_type,
                    'quantity': abs(float(row['Quantity'])) if row['Quantity'] else 0,
                    'price_per_share': float(row['Price']) if row['Price'] else 0,
                    'transaction_fee': abs(float(row['Commission'])) if row['Commission'] else 0,
                    'total_amount': abs(float(row['Gross Amount'])) if row['Gross Amount'] else 0,
                    'net_amount': abs(float(row['Net Amount'])) if row['Net Amount'] else 0,
                    'trade_date': trade_date,
                    'settlement_date': settlement_date,
                    'currency': row.get('Currency', 'CAD').upper(),
                    'notes': f'Imported from Questrade CSV - {row.get("Action", "")}',
                    'broker_reference': f"QT-{row.get('Transaction Date', '')}-{symbol}"
                })
                
            except (ValueError, KeyError) as e:
                print(f"Error parsing Questrade row: {e}, Row: {row}")
                continue
        
        return transactions
    
    def _parse_td_csv(self, reader) -> List[Dict]:
        """解析TD Direct Investing CSV格式"""
        transactions = []
        
        for row in reader:
            try:
                # TD CSV格式可能有所不同，这里是一个示例
                trade_date = datetime.strptime(row['Date'], '%Y-%m-%d').date()
                
                symbol = row['Symbol'].strip().upper()
                if not symbol:
                    continue
                
                action = row['Transaction Type'].upper()
                if action in ['BUY', 'SELL']:
                    transaction_type = action
                elif 'DIVIDEND' in action:
                    transaction_type = 'DIVIDEND'
                else:
                    continue
                
                transactions.append({
                    'symbol': symbol,
                    'name': row.get('Description', '').strip(),
                    'transaction_type': transaction_type,
                    'quantity': abs(float(row['Quantity'])) if row['Quantity'] else 0,
                    'price_per_share': float(row['Price']) if row['Price'] else 0,
                    'transaction_fee': abs(float(row['Commission'])) if row['Commission'] else 0,
                    'total_amount': abs(float(row['Amount'])) if row['Amount'] else 0,
                    'trade_date': trade_date,
                    'currency': row.get('Currency', 'CAD').upper(),
                    'notes': f'Imported from TD CSV - {row.get("Transaction Type", "")}',
                    'broker_reference': f"TD-{row.get('Date', '')}-{symbol}"
                })
                
            except (ValueError, KeyError) as e:
                print(f"Error parsing TD row: {e}, Row: {row}")
                continue
        
        return transactions
    
    def _parse_ib_csv(self, reader) -> List[Dict]:
        """解析Interactive Brokers CSV格式"""
        transactions = []
        
        for row in reader:
            try:
                # IB的CSV格式通常比较复杂，这里简化处理
                if row.get('DataDiscriminator') != 'Order':
                    continue
                
                trade_date = datetime.strptime(row['TradeDate'], '%Y%m%d').date()
                
                symbol = row['Symbol'].strip().upper()
                if not symbol:
                    continue
                
                # IB使用Buy/Sell
                action = row['BuySell'].upper()
                transaction_type = 'BUY' if action == 'BOT' else 'SELL'
                
                transactions.append({
                    'symbol': symbol,
                    'name': row.get('Description', '').strip(),
                    'transaction_type': transaction_type,
                    'quantity': abs(float(row['Quantity'])),
                    'price_per_share': float(row['TradePrice']),
                    'transaction_fee': abs(float(row['IBCommission'])),
                    'total_amount': abs(float(row['Proceeds'])),
                    'trade_date': trade_date,
                    'currency': row.get('CurrencyPrimary', 'USD').upper(),
                    'exchange': row.get('ListingExchange', ''),
                    'notes': f'Imported from Interactive Brokers CSV',
                    'broker_reference': f"IB-{row.get('TradeID', '')}"
                })
                
            except (ValueError, KeyError) as e:
                print(f"Error parsing IB row: {e}, Row: {row}")
                continue
        
        return transactions
    
    def _parse_wealthsimple_csv(self, reader) -> List[Dict]:
        """解析Wealthsimple Trade CSV格式"""
        transactions = []
        
        for row in reader:
            try:
                trade_date = datetime.strptime(row['Settled date'], '%Y-%m-%d').date()
                
                symbol = row['Symbol'].strip().upper()
                if not symbol:
                    continue
                
                # Wealthsimple使用Market buy/Market sell
                action = row['Activity type']
                if 'buy' in action.lower():
                    transaction_type = 'BUY'
                elif 'sell' in action.lower():
                    transaction_type = 'SELL'
                elif 'dividend' in action.lower():
                    transaction_type = 'DIVIDEND'
                else:
                    continue
                
                transactions.append({
                    'symbol': symbol,
                    'name': row.get('Name', '').strip(),
                    'transaction_type': transaction_type,
                    'quantity': abs(float(row['Quantity'])) if row['Quantity'] else 0,
                    'price_per_share': float(row['Fill price']) if row['Fill price'] else 0,
                    'transaction_fee': 0,  # Wealthsimple通常免佣金
                    'total_amount': abs(float(row['Market value'])) if row['Market value'] else 0,
                    'trade_date': trade_date,
                    'currency': 'CAD',  # Wealthsimple主要是CAD
                    'notes': f'Imported from Wealthsimple CSV - {row.get("Activity type", "")}',
                    'broker_reference': f"WS-{row.get('Settled date', '')}-{symbol}"
                })
                
            except (ValueError, KeyError) as e:
                print(f"Error parsing Wealthsimple row: {e}, Row: {row}")
                continue
        
        return transactions
    
    def _parse_generic_csv(self, reader) -> List[Dict]:
        """解析通用CSV格式"""
        transactions = []
        
        # 通用格式的列映射
        column_mapping = {
            'date': ['date', 'trade_date', 'trade date', 'settlement_date', 'settlement date'],
            'symbol': ['symbol', 'ticker', 'stock_symbol'],
            'type': ['type', 'transaction_type', 'action', 'operation'],
            'quantity': ['quantity', 'shares', 'qty'],
            'price': ['price', 'price_per_share', 'unit_price'],
            'fee': ['fee', 'commission', 'transaction_fee'],
            'total': ['total', 'amount', 'gross_amount', 'net_amount', 'net amount'],
            'description': ['description', 'name', 'stock_name'],
            'currency': ['currency', 'curr']
        }
        
        # 检测列名
        fieldnames = reader.fieldnames
        detected_columns = {}
        
        print(f"CSV fieldnames: {fieldnames}")
        
        for key, possible_names in column_mapping.items():
            for field in fieldnames:
                # 更灵活的匹配 - 转换为小写并移除空格和下划线
                field_normalized = field.lower().replace('_', '').replace(' ', '')
                possible_normalized = [name.lower().replace('_', '').replace(' ', '') for name in possible_names]
                
                # 对于货币字段，排除可能的余额相关列
                if key == 'currency':
                    # 排除包含balance, current, total等的列名
                    excluded_terms = ['balance', 'current', 'total', 'amount', 'value', 'cash']
                    if any(term in field_normalized for term in excluded_terms):
                        continue
                
                if field_normalized in possible_normalized:
                    detected_columns[key] = field
                    print(f"Mapped '{key}' to column '{field}'")
                    break
        
        print(f"Detected column mapping: {detected_columns}")
        
        for row in reader:
            try:
                # 解析日期
                date_field = detected_columns.get('date', '')
                if not date_field:
                    print(f"No date field mapped. Available fields: {list(row.keys())}")
                    continue
                    
                date_str = row.get(date_field, '')
                if not date_str:
                    print(f"No date value found in field '{date_field}'. Row: {row}")
                    continue
                
                # 尝试多种日期格式
                trade_date = None
                for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y%m%d']:
                    try:
                        trade_date = datetime.strptime(date_str, date_format).date()
                        break
                    except ValueError:
                        continue
                
                if not trade_date:
                    continue
                
                symbol = row.get(detected_columns.get('symbol', '')).strip().upper()
                if not symbol:
                    continue
                
                transaction_type = row.get(detected_columns.get('type', '')).upper().strip()
                
                # 映射常见的操作类型
                type_mapping = {
                    'BUY': 'BUY',
                    'SELL': 'SELL', 
                    'DIVIDEND': 'DIVIDEND',
                    'DIV': 'DIVIDEND',
                    'PURCHASE': 'BUY',
                    'SALE': 'SELL',
                    'ACHAT': 'BUY',  # 法语 - 购买
                    'VENTE': 'SELL',  # 法语 - 出售
                    'DIVIDENDE': 'DIVIDEND'  # 法语 - 分红
                }
                
                transaction_type = type_mapping.get(transaction_type, transaction_type)
                
                if transaction_type not in ['BUY', 'SELL', 'DIVIDEND']:
                    print(f"Skipping unknown transaction type: {transaction_type}")
                    continue
                
                # 获取描述信息
                description = row.get(detected_columns.get('description', ''), row.get('Description', '')).strip()
                
                transactions.append({
                    'symbol': symbol,
                    'name': description,
                    'transaction_type': transaction_type,
                    'quantity': abs(float(row.get(detected_columns.get('quantity', ''), 0))),
                    'price_per_share': float(row.get(detected_columns.get('price', ''), 0)),
                    'transaction_fee': abs(float(row.get(detected_columns.get('fee', ''), 0))),
                    'total_amount': abs(float(row.get(detected_columns.get('total', ''), 0))),
                    'trade_date': trade_date,
                    'currency': row.get(detected_columns.get('currency', ''), 'CAD').upper() if detected_columns.get('currency') else 'CAD',
                    'notes': f'Imported from Generic CSV',
                    'broker_reference': f"CSV-{date_str}-{symbol}"
                })
                
            except (ValueError, KeyError) as e:
                print(f"Error parsing generic row: {e}, Row: {row}")
                continue
        
        return transactions
    
    def _import_transactions(self, transactions_data: List[Dict], account_id: int) -> Tuple[int, int, List[str], int]:
        """导入交易记录到数据库"""
        imported_count = 0
        failed_count = 0
        errors = []
        corrected_count = 0  # 记录矫正的交易数量
        
        account = Account.query.get(account_id)
        if not account:
            raise ValueError(f"Account {account_id} not found")
        
        for txn_data in transactions_data:
            try:
                # 查找或创建股票记录，并检查是否发生了矫正
                stock, was_corrected = self._find_or_create_stock(txn_data)
                if was_corrected:
                    corrected_count += 1
                
                # 检查是否已存在相同的交易（根据broker_reference）
                existing = Transaction.query.filter_by(
                    account_id=account_id,
                    broker_reference=txn_data.get('broker_reference')
                ).first()
                
                if existing:
                    continue  # 跳过重复交易
                
                # 创建交易记录  
                transaction = Transaction(
                    account_id=account_id,
                    stock=txn_data['symbol'],  # 使用矫正后的symbol
                    type=txn_data['transaction_type'],
                    quantity=Decimal(str(txn_data['quantity'])),
                    price=Decimal(str(txn_data['price_per_share'])),
                    fee=Decimal(str(txn_data['transaction_fee'])),
                    amount=Decimal(str(txn_data['total_amount'])),
                    trade_date=txn_data['trade_date'],
                    currency=txn_data.get('currency', 'CAD'),
                    notes=txn_data.get('notes')
                )
                
                db.session.add(transaction)
                db.session.flush()
                
                imported_count += 1
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Row {imported_count + failed_count}: {str(e)}"
                errors.append(error_msg)
                print(error_msg)
                continue
        
        db.session.commit()
        return imported_count, failed_count, errors, corrected_count
    
    def _find_or_create_stock(self, txn_data: Dict) -> Tuple[StocksCache, bool]:
        """查找或创建股票记录，返回股票对象和是否发生了矫正"""
        original_symbol = txn_data['symbol']
        currency = txn_data.get('currency', 'CAD')
        
        # 检查是否有股票代码矫正记录
        corrected_symbol = StockSymbolCorrection.get_corrected_symbol(original_symbol, currency)
        
        # 检查是否发生了矫正
        was_corrected = corrected_symbol != original_symbol.upper()
        
        # 如果股票代码被矫正了，使用矫正后的代码并更新交易记录中的symbol
        if was_corrected:
            print(f"股票代码矫正: {original_symbol}({currency}) -> {corrected_symbol}")
            symbol = corrected_symbol
            # 更新交易数据中的symbol，这样后续创建Transaction时会使用矫正后的代码
            txn_data['symbol'] = corrected_symbol
        else:
            symbol = original_symbol
        
        # 使用联合主键查找现有股票记录
        stock = StocksCache.query.filter_by(symbol=symbol, currency=currency).first()
        
        if stock:
            return stock, was_corrected
        
        # 创建新股票记录
        # 根据交易所推断市场
        exchange = txn_data.get('exchange', '')
        
        if not exchange:
            # 根据股票符号推断交易所
            if symbol.endswith('.TO'):
                exchange = 'TSX'
                currency = 'CAD'  # 确保货币一致性
            elif symbol.endswith('.V'):
                exchange = 'TSXV'
                currency = 'CAD'  # 确保货币一致性
            elif '.' not in symbol:
                exchange = 'NASDAQ'
                # 保持传入的currency，不强制改为USD
            else:
                exchange = 'UNKNOWN'
        
        stock = StocksCache(
            symbol=symbol,
            name=txn_data.get('name', symbol),
            exchange=exchange,
            currency=currency  # 重要：设置currency字段
        )
        
        # 跳过自动分类（StockCategory已删除）
        # TODO: 实现新的分类系统
        # category = self._suggest_stock_category(symbol, txn_data.get('name', ''))
        # if category:
        #     stock.category_id = category.id
        
        db.session.add(stock)
        db.session.flush()
        
        return stock, was_corrected
    
    def _suggest_stock_category(self, symbol: str, name: str) -> Optional[int]:
        """根据股票符号和名称建议分类"""
        # 简单的关键词匹配
        keywords = {
            'Technology': ['tech', 'software', 'apple', 'microsoft', 'google', 'meta', 'tesla', 'nvidia', 'shopify'],
            'Banking': ['bank', 'financial', 'td', 'rbc', 'bmo', 'scotia', 'national'],
            'Energy': ['energy', 'oil', 'gas', 'pipeline', 'enbridge', 'canadian natural', 'suncor'],
            'Real Estate': ['reit', 'real estate', 'property', 'residential'],
            'Healthcare': ['health', 'medical', 'pharma', 'bio', 'hospital'],
            'Index Funds': ['etf', 'index', 'vanguard', 'ishares', 'spdr', 'vti', 'vgro']
        }
        
        search_text = f"{symbol} {name}".lower()
        
        # TODO: 实现新的分类系统
        # for category_name, category_keywords in keywords.items():
        #     for keyword in category_keywords:
        #         if keyword in search_text:
        #             category = StockCategory.query.filter_by(name=category_name).first()
        #             if category:
        #                 return category
        
        return None
    
    def export_transactions_to_csv(self, account_id: int, start_date: str = None, end_date: str = None) -> str:
        """导出交易记录为CSV"""
        account = Account.query.get_or_404(account_id)
        
        # 构建查询
        query = Transaction.query.filter_by(account_id=account_id)
        
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            query = query.filter(Transaction.trade_date >= start_date)
        
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            query = query.filter(Transaction.trade_date <= end_date)
        
        transactions = query.order_by(Transaction.trade_date.desc()).all()
        
        # 创建临时文件
        export_dir = current_app.config.get('EXPORT_FOLDER', tempfile.gettempdir())
        os.makedirs(export_dir, exist_ok=True)
        
        filename = f"{account.name}_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(export_dir, filename)
        
        # 写入CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'Date', 'Symbol', 'Name', 'Type', 'Quantity', 'Price', 'Fee', 'Total', 'Currency', 'Notes'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for txn in transactions:
                writer.writerow({
                    'Date': txn.trade_date.isoformat(),
                    'Symbol': txn.stock.symbol,
                    'Name': txn.stock.name,
                    'Type': txn.transaction_type,
                    'Quantity': float(txn.quantity),
                    'Price': float(txn.price_per_share),
                    'Fee': float(txn.transaction_fee),
                    'Total': float(txn.total_amount),
                    'Currency': txn.currency,
                    'Notes': txn.notes or ''
                })
        
        return filepath