#!/usr/bin/env python3
"""
启动开发服务器
"""

import os
from family_investment import app

if __name__ == '__main__':
    # 设置环境变量
    os.environ['FLASK_ENV'] = 'development'
    os.environ['FLASK_DEBUG'] = '1'
    
    print("🚀 启动加拿大家庭投资管理系统...")
    print("📊 系统功能:")
    print("   • 多成员家庭投资管理")
    print("   • TFSA/RRSP/RESP/FHSA账户支持")
    print("   • 多币种投资跟踪(CAD/USD)")
    print("   • 交易记录和持仓分析")
    print("   • 投资组合报告和风险分析")
    print("   • CSV导入和数据导出")
    print("   • 多语言支持(英语/中文)")
    print()
    print("🌐 访问地址:")
    print("   • 主页: http://localhost:5050/")
    print("   • 仪表板: http://localhost:5050/dashboard")
    print("   • API文档: http://localhost:5050/api/v1/")
    print()
    print("⚠️  注意: 按 Ctrl+C 停止服务器")
    print("=" * 50)
    
    # 启动服务器
    app.run(
        host='0.0.0.0',
        port=5050,
        debug=True,
        threaded=True
    )