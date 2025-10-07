/**
 * 股票代码修正组件 - 可重用的模块
 * 用于验证和修正Yahoo Finance股票代码
 */

class StockSymbolCorrection {
    constructor(options = {}) {
        this.modalId = options.modalId || 'stockCorrectionModal';
        this.onSuccess = options.onSuccess || (() => location.reload());
        this.onError = options.onError || ((error) => alert('Error: ' + error));
        this.stockId = options.stockId || null;
        this.currentSymbol = options.currentSymbol || '';
        this.currentCurrency = options.currentCurrency || 'CAD';
        
        this.initModal();
        this.bindEvents();
    }

    initModal() {
        // 检查modal是否已存在，如果不存在则创建
        if (!document.getElementById(this.modalId)) {
            const modalHtml = this.getModalHtml();
            document.body.insertAdjacentHTML('beforeend', modalHtml);
            console.log('股票修正模态框HTML已添加到页面');
        }
        
        this.modal = document.getElementById(this.modalId);
        this.form = this.modal.querySelector('form');
        
        if (!this.modal) {
            console.error('无法找到模态框元素:', this.modalId);
        }
        if (!this.form) {
            console.error('无法找到表单元素');
        }
        
        console.log('模态框初始化完成:', {
            modalId: this.modalId,
            modal: this.modal,
            form: this.form
        });
    }

    getModalHtml() {
        return `
        <!-- Stock Symbol Correction Modal -->
        <div class="modal fade" id="${this.modalId}" tabindex="-1" aria-labelledby="${this.modalId}Label" aria-hidden="true">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="${this.modalId}Label">
                            <i class="fas fa-edit me-2"></i>修正股票代码
                        </h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <form class="stock-correction-form">
                        <div class="modal-body">
                            <div class="alert alert-info" role="alert">
                                <i class="fas fa-info-circle me-2"></i>
                                无法从Yahoo Finance获取该股票的历史数据。请检查并修正股票代码，确保使用Yahoo Finance认可的格式。
                            </div>
                            
                            <input type="hidden" class="stock-id-input" value="">
                            
                            <div class="mb-3">
                                <label for="${this.modalId}_symbol" class="form-label">股票代码 <span class="text-danger">*</span></label>
                                <div class="input-group">
                                    <input type="text" class="form-control stock-symbol-input" 
                                           id="${this.modalId}_symbol" 
                                           name="symbol" required 
                                           placeholder="例如: AAPL, TSLA, 000001.SS">
                                    <button type="button" class="btn btn-outline-info verify-symbol-btn">
                                        <i class="fas fa-sync"></i> 验证
                                    </button>
                                </div>
                                <div class="form-text">修正后将更新所有相关的交易记录</div>
                            </div>
                            
                            <div class="mb-3">
                                <label for="${this.modalId}_name" class="form-label">股票名称</label>
                                <input type="text" class="form-control stock-name-input" 
                                       id="${this.modalId}_name" 
                                       name="name" readonly>
                                <div class="form-text">仅用于验证股票代码</div>
                            </div>
                            
                            <div class="mb-3">
                                <label for="${this.modalId}_exchange" class="form-label">交易所</label>
                                <input type="text" class="form-control stock-exchange-input" 
                                       id="${this.modalId}_exchange" 
                                       name="exchange" readonly>
                                <div class="form-text">仅用于验证股票代码</div>
                            </div>
                            
                            <div class="mb-3">
                                <label for="${this.modalId}_currency" class="form-label">货币</label>
                                <input type="text" class="form-control stock-currency-input" 
                                       id="${this.modalId}_currency" 
                                       name="currency" readonly 
                                       style="background-color: #f8f9fa;">
                                <div class="form-text">货币不可修改，由原始交易记录决定</div>
                            </div>

                            <div class="mb-3">
                                <label for="${this.modalId}_ipo_date" class="form-label">IPO日期</label>
                                <input type="date" class="form-control stock-ipo-input"
                                       id="${this.modalId}_ipo_date"
                                       name="first_trade_date"
                                       placeholder="YYYY-MM-DD">
                                <div class="form-text">验证时会尝试自动获取IPO日期，也可以手动输入</div>
                            </div>
                            
                            <div class="verification-result" style="display: none;">
                                <div class="alert alert-success" role="alert">
                                    <i class="fas fa-check-circle me-2"></i>
                                    <strong>验证成功！</strong>
                                    <div class="verification-details mt-2"></div>
                                </div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">取消</button>
                            <button type="submit" class="btn btn-primary">
                                <i class="fas fa-save me-2"></i>更新股票代码
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>`;
    }

    bindEvents() {
        // 验证按钮事件
        const verifyBtn = this.modal.querySelector('.verify-symbol-btn');
        verifyBtn.addEventListener('click', () => this.verifySymbol());

        // 表单提交事件
        this.form.addEventListener('submit', (e) => this.handleSubmit(e));
    }

    show(stockData = {}) {
        // 设置表单数据
        this.stockId = stockData.stockId || this.stockId;
        this.currentSymbol = stockData.symbol || this.currentSymbol;
        this.currentCurrency = stockData.currency || this.currentCurrency;

        // 填充表单
        this.modal.querySelector('.stock-id-input').value = this.stockId || '';
        this.modal.querySelector('.stock-symbol-input').value = this.currentSymbol;
        this.modal.querySelector('.stock-currency-input').value = this.currentCurrency;
        const ipoInput = this.modal.querySelector('.stock-ipo-input');
        if (stockData.firstTradeDate) {
            ipoInput.value = stockData.firstTradeDate;
        } else {
            ipoInput.value = '';
        }

        // 清空验证结果和只读字段
        this.modal.querySelector('.stock-name-input').value = '';
        this.modal.querySelector('.stock-exchange-input').value = '';
        this.modal.querySelector('.verification-result').style.display = 'none';

        // 显示模态框
        try {
            const bsModal = new bootstrap.Modal(this.modal);
            bsModal.show();
            console.log('模态框已显示');
        } catch (error) {
            console.error('显示模态框失败:', error);
            // 检查Bootstrap是否加载
            if (typeof bootstrap === 'undefined') {
                console.error('Bootstrap未加载');
                alert('Bootstrap未加载，无法显示对话框');
            } else {
                console.error('Bootstrap Modal初始化失败:', error);
                alert('无法显示对话框: ' + error.message);
            }
        }
    }

    async verifySymbol() {
        const symbolInput = this.modal.querySelector('.stock-symbol-input');
        const currencyInput = this.modal.querySelector('.stock-currency-input');
        const verifyBtn = this.modal.querySelector('.verify-symbol-btn');
        const resultDiv = this.modal.querySelector('.verification-result');
        const ipoInput = this.modal.querySelector('.stock-ipo-input');
        
        const symbol = symbolInput.value.trim().toUpperCase();
        const currency = currencyInput.value;
        
        if (!symbol) {
            alert('请先输入股票代码');
            return;
        }
        
        // 显示加载状态
        const originalText = verifyBtn.innerHTML;
        verifyBtn.disabled = true;
        verifyBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 验证中...';
        resultDiv.style.display = 'none';
        
        try {
            const response = await fetch('/api/v1/stocks/refresh-info', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify({
                    symbol: symbol,
                    currency: currency
                })
            });
            
            const data = await response.json();
            
            if (data.success && data.stock_info) {
                const info = data.stock_info;
                
                // 自动填充表单
                if (info.name) {
                    this.modal.querySelector('.stock-name-input').value = info.name;
                }
                if (info.exchange) {
                    this.modal.querySelector('.stock-exchange-input').value = info.exchange;
                }
                
                // 显示验证结果
                const details = [];
                if (info.name) details.push(`名称: ${info.name}`);
                if (info.exchange) details.push(`交易所: ${info.exchange}`);
                if (info.current_price) details.push(`当前价格: ${currency} $${info.current_price}`);
                if (info.first_trade_date) {
                    ipoInput.value = info.first_trade_date;
                    details.push(`IPO日期: ${info.first_trade_date}`);
                }
                
                this.modal.querySelector('.verification-details').innerHTML = details.join('<br>');
                resultDiv.style.display = 'block';
                
            } else {
                alert('无法从Yahoo Finance获取股票信息: ' + (data.error || '未知错误'));
            }
        } catch (error) {
            console.error('Error:', error);
            alert('验证股票信息时出错，请重试。');
        } finally {
            // 恢复按钮状态
            verifyBtn.disabled = false;
            verifyBtn.innerHTML = originalText;
        }
    }

    async handleSubmit(e) {
        e.preventDefault();
        
        const formData = new FormData(this.form);
        const data = {
            symbol: formData.get('symbol'),
            name: formData.get('name'),
            exchange: formData.get('exchange'),
            currency: formData.get('currency'),
            first_trade_date: formData.get('first_trade_date') || null
        };
        
        if (!this.stockId) {
            this.onError('未找到股票ID');
            return;
        }
        
        try {
            const response = await fetch(`/api/v1/stocks/${this.stockId}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify(data)
            });
            
            const result = await response.json();
            
            if (result.success) {
                // 构建成功消息
                let message = '股票代码更新成功！';
                
                if (result.updated_transactions > 0) {
                    message += `\n已更新 ${result.updated_transactions} 条交易记录`;
                }
                
                if (result.merged_duplicate) {
                    message += `\n合并重复股票，额外更新了 ${result.merged_transactions} 条交易记录`;
                }
                
                if (result.refreshed_info) {
                    message += '\n股票信息已从Yahoo Finance更新';
                    
                    if (result.stock_info && result.stock_info.current_price) {
                        message += `\n当前价格: ${result.stock_info.currency} $${result.stock_info.current_price}`;
                    }
                }
                if (result.stock_info && result.stock_info.first_trade_date) {
                    message += `\nIPO日期: ${result.stock_info.first_trade_date}`;
                }
                
                alert(message);
                
                // 隐藏模态框
                const bsModal = bootstrap.Modal.getInstance(this.modal);
                bsModal.hide();
                
                // 调用成功回调
                this.onSuccess(result);
                
            } else {
                this.onError(result.error || '更新失败');
            }
        } catch (error) {
            console.error('Error:', error);
            this.onError('更新股票代码时出错，请重试');
        }
    }

    // 静态方法：快速创建修正按钮
    static createCorrectionButton(options = {}) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = options.className || 'btn btn-warning btn-sm';
        button.innerHTML = options.innerHTML || '<i class="fas fa-edit me-2"></i>修正股票代码';
        
        button.addEventListener('click', () => {
            const correction = new StockSymbolCorrection(options);
            correction.show(options.stockData || {});
        });
        
        return button;
    }
}

// 导出到全局作用域以便在其他脚本中使用
window.StockSymbolCorrection = StockSymbolCorrection;
