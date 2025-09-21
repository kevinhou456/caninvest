#!/usr/bin/env python3
"""
测试CSV导入重复交易检测功能
特别针对存入(DEPOSIT)和取出(WITHDRAWAL)交易的重复检测逻辑
"""

def test_duplicate_detection_logic():
    """测试重复交易检测逻辑"""
    print("=== 测试重复交易检测逻辑 ===")

    # 模拟已存在的交易记录
    existing_transactions = [
        {
            'account_id': 1,
            'trade_date': '2024-01-01',
            'type': 'DEPOSIT',
            'stock': None,
            'quantity': 1000.0,
            'price': 0.0,
            'currency': 'CAD',
            'fee': 0.0,
            'notes': '工资存入'
        },
        {
            'account_id': 1,
            'trade_date': '2024-01-02',
            'type': 'WITHDRAWAL',
            'stock': None,
            'quantity': 500.0,
            'price': 0.0,
            'currency': 'CAD',
            'fee': 0.0,
            'notes': '生活费取出'
        },
        {
            'account_id': 1,
            'trade_date': '2024-01-03',
            'type': 'BUY',
            'stock': 'AAPL',
            'quantity': 10.0,
            'price': 150.0,
            'currency': 'USD',
            'fee': 9.99,
            'notes': ''
        }
    ]

    def is_duplicate_new_logic(new_transaction, existing_transactions):
        """新的重复检测逻辑"""
        for existing in existing_transactions:
            # 基本字段匹配
            if (existing['account_id'] != new_transaction['account_id'] or
                existing['trade_date'] != new_transaction['trade_date'] or
                existing['type'] != new_transaction['type'] or
                existing['quantity'] != new_transaction['quantity'] or
                existing['currency'] != new_transaction['currency'] or
                existing['fee'] != new_transaction['fee']):
                continue

            # 对于存入/取出交易，不检查stock和price
            if new_transaction['type'] in ['DEPOSIT', 'WITHDRAWAL']:
                return True

            # 对于股票交易，需要检查stock和price
            if (existing['stock'] == new_transaction['stock'] and
                existing['price'] == new_transaction['price']):
                return True

        return False

    # 测试用例
    test_cases = [
        # 重复的存入交易（应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-01',
                'type': 'DEPOSIT',
                'stock': None,
                'quantity': 1000.0,
                'price': 0.0,
                'currency': 'CAD',
                'fee': 0.0,
                'notes': '同样的工资存入'  # notes不同，但应该被检测为重复
            },
            'expected_duplicate': True,
            'description': '相同的存入交易（notes不同）'
        },

        # 不同金额的存入交易（不应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-01',
                'type': 'DEPOSIT',
                'stock': None,
                'quantity': 1500.0,  # 不同金额
                'price': 0.0,
                'currency': 'CAD',
                'fee': 0.0,
                'notes': '工资存入'
            },
            'expected_duplicate': False,
            'description': '不同金额的存入交易'
        },

        # 重复的取出交易（应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-02',
                'type': 'WITHDRAWAL',
                'stock': None,
                'quantity': 500.0,
                'price': 0.0,
                'currency': 'CAD',
                'fee': 0.0,
                'notes': '房租支出'  # notes不同，但应该被检测为重复
            },
            'expected_duplicate': True,
            'description': '相同的取出交易（notes不同）'
        },

        # 重复的股票买入交易（应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-03',
                'type': 'BUY',
                'stock': 'AAPL',
                'quantity': 10.0,
                'price': 150.0,
                'currency': 'USD',
                'fee': 9.99,
                'notes': '苹果股票买入'  # notes不同，但应该被检测为重复
            },
            'expected_duplicate': True,
            'description': '相同的股票买入交易（notes不同）'
        },

        # 不同价格的股票买入交易（不应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-03',
                'type': 'BUY',
                'stock': 'AAPL',
                'quantity': 10.0,
                'price': 155.0,  # 不同价格
                'currency': 'USD',
                'fee': 9.99,
                'notes': ''
            },
            'expected_duplicate': False,
            'description': '不同价格的股票买入交易'
        },

        # 新的存入交易（不应该被检测为重复）
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-04',
                'type': 'DEPOSIT',
                'stock': None,
                'quantity': 800.0,
                'price': 0.0,
                'currency': 'CAD',
                'fee': 0.0,
                'notes': '奖金存入'
            },
            'expected_duplicate': False,
            'description': '新的存入交易'
        }
    ]

    # 运行测试
    for i, test_case in enumerate(test_cases, 1):
        transaction = test_case['transaction']
        expected = test_case['expected_duplicate']
        description = test_case['description']

        result = is_duplicate_new_logic(transaction, existing_transactions)

        if result == expected:
            print(f"✅ 测试 {i}: {description} - {'重复' if result else '不重复'}")
        else:
            print(f"❌ 测试 {i}: {description} - 预期{'重复' if expected else '不重复'}，实际{'重复' if result else '不重复'}")
            print(f"   交易详情: {transaction}")

def test_smart_import_logic():
    """测试智能导入的重复检测逻辑"""
    print("\n=== 测试智能导入重复检测逻辑 ===")

    # 模拟数据库查询结果
    def mock_query_filter_by(**kwargs):
        # 模拟已存在的交易
        existing_transactions = [
            {'account_id': 1, 'trade_date': '2024-01-01', 'type': 'DEPOSIT', 'quantity': 1000.0, 'currency': 'CAD'},
            {'account_id': 1, 'trade_date': '2024-01-02', 'type': 'BUY', 'stock': 'AAPL', 'quantity': 10.0, 'price': 150.0}
        ]

        for existing in existing_transactions:
            match = True
            for key, value in kwargs.items():
                if existing.get(key) != value:
                    match = False
                    break
            if match:
                return MockResult(existing)
        return MockResult(None)

    class MockResult:
        def __init__(self, data):
            self.data = data

        def first(self):
            return self.data

    # 测试智能导入的重复检测逻辑
    def test_smart_import_duplicate_detection(transaction_data):
        if transaction_data['type'] in ['DEPOSIT', 'WITHDRAWAL']:
            existing = mock_query_filter_by(
                account_id=transaction_data['account_id'],
                trade_date=transaction_data['trade_date'],
                type=transaction_data['type'],
                quantity=transaction_data['quantity'],
                currency=transaction_data['currency']
            ).first()
        else:
            existing = mock_query_filter_by(
                account_id=transaction_data['account_id'],
                trade_date=transaction_data['trade_date'],
                type=transaction_data['type'],
                stock=transaction_data['stock'],
                quantity=transaction_data['quantity'],
                price=transaction_data['price']
            ).first()

        return existing is not None

    test_cases = [
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-01',
                'type': 'DEPOSIT',
                'quantity': 1000.0,
                'currency': 'CAD'
            },
            'expected_duplicate': True,
            'description': '重复的存入交易'
        },
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-01',
                'type': 'DEPOSIT',
                'quantity': 1500.0,  # 不同金额
                'currency': 'CAD'
            },
            'expected_duplicate': False,
            'description': '不同金额的存入交易'
        },
        {
            'transaction': {
                'account_id': 1,
                'trade_date': '2024-01-02',
                'type': 'BUY',
                'stock': 'AAPL',
                'quantity': 10.0,
                'price': 150.0
            },
            'expected_duplicate': True,
            'description': '重复的股票买入交易'
        }
    ]

    for i, test_case in enumerate(test_cases, 1):
        transaction = test_case['transaction']
        expected = test_case['expected_duplicate']
        description = test_case['description']

        result = test_smart_import_duplicate_detection(transaction)

        if result == expected:
            print(f"✅ 智能导入测试 {i}: {description} - {'重复' if result else '不重复'}")
        else:
            print(f"❌ 智能导入测试 {i}: {description} - 预期{'重复' if expected else '不重复'}，实际{'重复' if result else '不重复'}")

def main():
    print("CSV导入重复交易检测测试套件")
    print("=" * 50)

    test_duplicate_detection_logic()
    test_smart_import_logic()

    print("\n" + "=" * 50)
    print("重复检测测试套件完成")
    print("\n修复要点:")
    print("1. 存入/取出交易不检查stock和price字段")
    print("2. 所有交易类型都不强制检查notes字段一致性")
    print("3. 智能导入和常规导入使用相同的重复检测逻辑")

if __name__ == "__main__":
    main()