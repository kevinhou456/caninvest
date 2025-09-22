#!/usr/bin/env python3
"""
yfinance 诊断测试脚本
用于检测不同机器上的yfinance兼容性问题
"""

import sys
import platform
import requests
from datetime import datetime

def print_system_info():
    """打印系统信息"""
    print("="*50)
    print("系统信息诊断")
    print("="*50)
    print(f"操作系统: {platform.system()} {platform.release()}")
    print(f"Python版本: {sys.version}")
    print(f"架构: {platform.machine()}")
    print(f"时间: {datetime.now()}")
    print()

def test_basic_imports():
    """测试基本库导入"""
    print("="*50)
    print("测试库导入")
    print("="*50)

    try:
        import yfinance
        print(f"✅ yfinance导入成功，版本: {yfinance.__version__}")
    except Exception as e:
        print(f"❌ yfinance导入失败: {e}")
        return False

    try:
        import pandas
        print(f"✅ pandas导入成功，版本: {pandas.__version__}")
    except Exception as e:
        print(f"❌ pandas导入失败: {e}")

    try:
        import requests
        print(f"✅ requests导入成功，版本: {requests.__version__}")
    except Exception as e:
        print(f"❌ requests导入失败: {e}")

    print()
    return True

def test_network_connectivity():
    """测试网络连接"""
    print("="*50)
    print("测试网络连接")
    print("="*50)

    urls = [
        "https://finance.yahoo.com",
        "https://query1.finance.yahoo.com/v8/finance/chart/CADUSD=X",
        "https://query2.finance.yahoo.com/v8/finance/chart/CADUSD=X"
    ]

    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            print(f"✅ {url} - 状态码: {response.status_code}")
        except Exception as e:
            print(f"❌ {url} - 错误: {e}")
    print()

def test_yfinance_basic():
    """测试yfinance基本功能"""
    print("="*50)
    print("测试yfinance基本功能")
    print("="*50)

    try:
        import yfinance as yf

        # 测试简单股票
        print("测试股票: AAPL")
        ticker = yf.Ticker("AAPL")
        data = ticker.history(period="1d")
        if not data.empty:
            print(f"✅ AAPL数据获取成功，形状: {data.shape}")
            print(f"   最新价格: {data['Close'].iloc[-1]:.2f}")
        else:
            print("❌ AAPL数据为空")

    except Exception as e:
        print(f"❌ yfinance基本测试失败: {e}")
        import traceback
        print(f"详细错误: {traceback.format_exc()}")
    print()

def test_currency_pairs():
    """测试货币对"""
    print("="*50)
    print("测试货币对")
    print("="*50)

    currency_pairs = [
        "CADUSD=X",
        "USDCAD=X",
        "EURUSD=X",
        "GBPUSD=X"
    ]

    try:
        import yfinance as yf

        for pair in currency_pairs:
            try:
                print(f"测试货币对: {pair}")
                ticker = yf.Ticker(pair)

                # 测试不同时间段
                for period in ["1d", "5d", "1mo"]:
                    try:
                        data = ticker.history(period=period)
                        if not data.empty:
                            latest_rate = data['Close'].iloc[-1]
                            print(f"  ✅ {period}: {latest_rate:.6f} (数据点: {len(data)})")
                            break
                        else:
                            print(f"  ⚠️  {period}: 数据为空")
                    except Exception as inner_e:
                        print(f"  ❌ {period}: {inner_e}")

            except Exception as e:
                print(f"❌ {pair}失败: {e}")

    except Exception as e:
        print(f"❌ 货币对测试失败: {e}")
    print()

def test_with_custom_headers():
    """测试自定义请求头"""
    print("="*50)
    print("测试自定义请求头")
    print("="*50)

    try:
        import yfinance as yf

        # 设置自定义session
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

        ticker = yf.Ticker("CADUSD=X", session=session)
        data = ticker.history(period="1d")

        if not data.empty:
            print(f"✅ 自定义头部成功: {data['Close'].iloc[-1]:.6f}")
        else:
            print("❌ 自定义头部失败: 数据为空")

    except Exception as e:
        print(f"❌ 自定义头部测试失败: {e}")
    print()

def main():
    """主函数"""
    print_system_info()

    if not test_basic_imports():
        print("库导入失败，无法继续测试")
        return

    test_network_connectivity()
    test_yfinance_basic()
    test_currency_pairs()
    test_with_custom_headers()

    print("="*50)
    print("诊断完成")
    print("="*50)
    print("请将输出结果发送给开发者以便分析问题")

if __name__ == "__main__":
    main()