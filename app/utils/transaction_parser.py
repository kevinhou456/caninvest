#!/usr/bin/env python3
"""
交易描述解析器
用于解析包含股票信息的交易描述字段
"""
import re
import logging
from typing import Dict, Optional, Tuple
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

class TransactionDescriptionParser:
    """解析交易描述字段，提取股票代码、名称、数量等信息"""
    
    def __init__(self):
        # 股票交易描述的正则表达式模式
        self.stock_patterns = [
            # 标准格式: "AAPL - Apple Inc.: Bought 10.0000 shares (executed at 2025-02-26)"
            r'^([A-Z]{1,6})\s*-\s*([^:]+?):\s*(Bought|Sold)\s+([\d,]+\.?\d*)\s+shares?',
            
            # 变体格式: "IBIT - iShares Bitcoin Trust ETF - Shares: Bought 10.0000 shares"
            r'^([A-Z]{1,6})\s*-\s*([^:]+?)(?:\s*-\s*[^:]*)?:\s*(Bought|Sold)\s+([\d,]+\.?\d*)\s+shares?',
            
            # 简化格式: "AAPL: Bought 100 shares"
            r'^([A-Z]{1,6}):\s*(Bought|Sold)\s+([\d,]+\.?\d*)\s+shares?',
            
            # 其他可能的格式
            r'([A-Z]{1,6})\s*(?:-[^:]*)?:\s*(Bought|Sold)\s+([\d,]+\.?\d*)\s+shares?'
        ]
        
    def parse_transaction_description(self, description: str, transaction_type: str, 
                                    amount: str, currency: str = 'USD') -> Dict:
        """
        解析交易描述
        
        Args:
            description: 交易描述
            transaction_type: 交易类型 (BUY, SELL, CONT等)
            amount: 交易金额 (字符串格式)
            currency: 货币
            
        Returns:
            解析后的交易信息字典
        """
        result = {
            'symbol': '',
            'stock_name': '',
            'quantity': 0,
            'price': 0,
            'transaction_type': transaction_type,
            'amount': float(amount) if amount else 0,
            'currency': currency,
            'notes': description,
            'parsed': False
        }
        
        # 处理非股票交易（如CONT贡献）
        if transaction_type == 'CONT' or 'Contribution' in description:
            result['transaction_type'] = 'DEPOSIT'
            result['symbol'] = ''
            result['stock_name'] = ''
            result['quantity'] = 0
            result['price'] = 0
            result['parsed'] = True
            return result
        
        # 处理分红
        if 'Dividend' in description or 'DIV' in transaction_type:
            result['transaction_type'] = 'DIVIDEND'
            # 尝试从描述中提取股票代码
            symbol_match = re.search(r'^([A-Z]{1,6})', description)
            if symbol_match:
                result['symbol'] = symbol_match.group(1)
            result['quantity'] = 1
            result['price'] = float(amount) if amount else 0
            result['parsed'] = True
            return result
        
        # 尝试解析股票交易
        for pattern in self.stock_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    
                    if len(groups) >= 4:
                        symbol = groups[0].upper()
                        stock_name = groups[1].strip()
                        action = groups[2].lower()
                        quantity_str = groups[3].replace(',', '')
                        
                        # 解析数量
                        quantity = float(quantity_str)
                        
                        # 计算单价
                        amount_val = float(amount) if amount else 0
                        if quantity > 0:
                            # amount为负数表示买入，正数表示卖出
                            price = abs(amount_val) / quantity
                        else:
                            price = 0
                        
                        # 确定交易类型
                        if action == 'bought' or (amount_val < 0 and transaction_type == 'BUY'):
                            trans_type = 'BUY'
                        elif action == 'sold' or (amount_val > 0 and transaction_type == 'SELL'):
                            trans_type = 'SELL'
                        else:
                            trans_type = transaction_type
                        
                        result.update({
                            'symbol': symbol,
                            'stock_name': stock_name,
                            'quantity': quantity,
                            'price': round(price, 4),
                            'transaction_type': trans_type,
                            'parsed': True
                        })
                        
                        logger.info(f"成功解析交易: {symbol} - {stock_name}, {quantity}股, ${price:.4f}/股")
                        return result
                        
                except (ValueError, IndexError, InvalidOperation) as e:
                    logger.warning(f"解析交易描述时出错: {e}")
                    continue
        
        # 如果无法解析，返回原始信息
        logger.warning(f"无法解析交易描述: {description}")
        result['notes'] = f"无法解析: {description}"
        return result
    
    def parse_csv_row(self, row: Dict) -> Dict:
        """
        解析CSV行数据
        
        Args:
            row: CSV行字典，包含date, transaction, description, amount, balance, currency
            
        Returns:
            标准化的交易记录
        """
        try:
            # 提取基本信息
            date = row.get('date', '')
            transaction_type = row.get('transaction', '').upper()
            description = row.get('description', '')
            amount = row.get('amount', '0')
            balance = row.get('balance', '0')
            currency = row.get('currency', 'USD')
            
            # 解析交易描述
            parsed = self.parse_transaction_description(
                description, transaction_type, amount, currency
            )
            
            # 添加日期和余额信息
            parsed.update({
                'date': date,
                'balance': float(balance) if balance else 0
            })
            
            return parsed
            
        except Exception as e:
            logger.error(f"解析CSV行时出错: {e}")
            return {
                'symbol': '',
                'stock_name': '',
                'quantity': 0,
                'price': 0,
                'transaction_type': 'UNKNOWN',
                'amount': 0,
                'currency': 'USD',
                'date': row.get('date', ''),
                'balance': 0,
                'notes': f"解析错误: {str(e)}",
                'parsed': False
            }

# 测试函数
def test_parser():
    """测试解析器功能"""
    parser = TransactionDescriptionParser()
    
    test_cases = [
        {
            'description': 'AMZN - Amazon.com Inc.: Bought 10.0000 shares (executed at 2025-02-26)',
            'transaction': 'BUY',
            'amount': '-2168.5'
        },
        {
            'description': 'IBIT - iShares Bitcoin Trust ETF - Shares: Bought 10.0000 shares (executed at 2025-08-28)',
            'transaction': 'BUY', 
            'amount': '-1500.0'
        },
        {
            'description': 'Contribution (executed at 2025-02-24), FX Rate: 1.4223',
            'transaction': 'CONT',
            'amount': '13000.0'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n测试案例 {i}:")
        print(f"描述: {test_case['description']}")
        result = parser.parse_transaction_description(
            test_case['description'],
            test_case['transaction'],
            test_case['amount']
        )
        print(f"结果: {result}")

if __name__ == '__main__':
    test_parser()