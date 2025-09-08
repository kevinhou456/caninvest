/**
 * 加拿大家庭投资管理系统 - 主要JavaScript文件
 */

// 全局配置
window.FamilyInvestment = {
    apiBase: '/api/v1',
    chartColors: {
        primary: '#dc3545',
        success: '#28a745',
        info: '#17a2b8',
        warning: '#ffc107',
        danger: '#dc3545',
        secondary: '#6c757d'
    },
    formatters: {
        currency: new Intl.NumberFormat('en-CA', {
            style: 'currency',
            currency: 'CAD'
        }),
        currencyUSD: new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD'
        }),
        percent: new Intl.NumberFormat('en-CA', {
            style: 'percent',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }),
        number: new Intl.NumberFormat('en-CA')
    }
};

// 工具函数
const Utils = {
    // 格式化货币
    formatCurrency: function(amount, currency = 'CAD') {
        if (currency === 'USD') {
            return window.FamilyInvestment.formatters.currencyUSD.format(amount);
        }
        return window.FamilyInvestment.formatters.currency.format(amount);
    },

    // 格式化百分比
    formatPercent: function(value) {
        return window.FamilyInvestment.formatters.percent.format(value / 100);
    },

    // 格式化数字
    formatNumber: function(value) {
        return window.FamilyInvestment.formatters.number.format(value);
    },

    // 获取价格变化的CSS类
    getPriceChangeClass: function(change) {
        if (change > 0) return 'text-success positive';
        if (change < 0) return 'text-danger negative';
        return 'text-muted';
    },

    // 显示通知消息
    showNotification: function(message, type = 'info') {
        const alertHtml = `
            <div class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
        
        const container = document.querySelector('.container-fluid');
        const alertContainer = document.createElement('div');
        alertContainer.innerHTML = alertHtml;
        container.insertBefore(alertContainer.firstElementChild, container.firstElementChild);
        
        // 自动隐藏
        setTimeout(() => {
            const alert = alertContainer.firstElementChild;
            if (alert && alert.classList.contains('alert')) {
                const bsAlert = new bootstrap.Alert(alert);
                bsAlert.close();
            }
        }, 5000);
    },

    // 确认对话框
    confirm: function(message, callback) {
        if (confirm(message)) {
            callback();
        }
    },

    // 加载状态管理
    setLoading: function(element, loading = true) {
        if (loading) {
            element.disabled = true;
            const originalText = element.textContent;
            element.dataset.originalText = originalText;
            element.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Loading...';
        } else {
            element.disabled = false;
            element.textContent = element.dataset.originalText || 'Submit';
        }
    }
};

// API调用封装
const API = {
    // 基础请求方法
    request: async function(url, options = {}) {
        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            }
        };

        const config = { ...defaultOptions, ...options };
        
        try {
            const response = await fetch(window.FamilyInvestment.apiBase + url, config);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error('API request failed:', error);
            Utils.showNotification(`Request failed: ${error.message}`, 'danger');
            throw error;
        }
    },

    // GET请求
    get: function(url) {
        return this.request(url);
    },

    // POST请求
    post: function(url, data) {
        return this.request(url, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    },

    // PUT请求
    put: function(url, data) {
        return this.request(url, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    },

    // DELETE请求
    delete: function(url) {
        return this.request(url, {
            method: 'DELETE'
        });
    }
};

// 图表工具
const Charts = {
    // 默认配置
    defaultOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'bottom'
            }
        }
    },

    // 创建饼图
    createPieChart: function(ctx, data, options = {}) {
        const config = {
            type: 'pie',
            data: data,
            options: { ...this.defaultOptions, ...options }
        };
        
        return new Chart(ctx, config);
    },

    // 创建线图
    createLineChart: function(ctx, data, options = {}) {
        const config = {
            type: 'line',
            data: data,
            options: {
                ...this.defaultOptions,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                ...options
            }
        };
        
        return new Chart(ctx, config);
    },

    // 创建柱状图
    createBarChart: function(ctx, data, options = {}) {
        const config = {
            type: 'bar',
            data: data,
            options: {
                ...this.defaultOptions,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                ...options
            }
        };
        
        return new Chart(ctx, config);
    },

    // 生成图表颜色
    generateColors: function(count) {
        const colors = [
            '#dc3545', '#28a745', '#17a2b8', '#ffc107', 
            '#6f42c1', '#fd7e14', '#20c997', '#e83e8c'
        ];
        
        const result = [];
        for (let i = 0; i < count; i++) {
            result.push(colors[i % colors.length]);
        }
        
        return result;
    }
};

// 表格工具
const Tables = {
    // 初始化数据表格
    initDataTable: function(selector, options = {}) {
        const defaultOptions = {
            pageLength: 25,
            responsive: true,
            order: [[0, 'desc']],
            language: {
                search: 'Search:',
                lengthMenu: 'Show _MENU_ entries per page',
                info: 'Showing _START_ to _END_ of _TOTAL_ entries',
                paginate: {
                    first: 'First',
                    last: 'Last',
                    next: 'Next',
                    previous: 'Previous'
                }
            }
        };

        return $(selector).DataTable({ ...defaultOptions, ...options });
    },

    // 更新表格数据
    updateTable: function(table, data) {
        table.clear();
        table.rows.add(data);
        table.draw();
    }
};

// 表单工具
const Forms = {
    // 序列化表单数据为JSON
    serializeToJson: function(form) {
        const formData = new FormData(form);
        const data = {};
        
        for (let [key, value] of formData.entries()) {
            if (data[key]) {
                if (Array.isArray(data[key])) {
                    data[key].push(value);
                } else {
                    data[key] = [data[key], value];
                }
            } else {
                data[key] = value;
            }
        }
        
        return data;
    },

    // 验证表单
    validate: function(form) {
        const requiredFields = form.querySelectorAll('[required]');
        let isValid = true;

        requiredFields.forEach(field => {
            if (!field.value.trim()) {
                field.classList.add('is-invalid');
                isValid = false;
            } else {
                field.classList.remove('is-invalid');
            }
        });

        return isValid;
    },

    // 重置表单验证状态
    resetValidation: function(form) {
        const fields = form.querySelectorAll('.is-invalid, .is-valid');
        fields.forEach(field => {
            field.classList.remove('is-invalid', 'is-valid');
        });
    }
};

// 文件上传工具
const FileUpload = {
    // 初始化拖拽上传
    initDragAndDrop: function(dropZone, fileInput, callback) {
        dropZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropZone.classList.add('dragover');
        });

        dropZone.addEventListener('dragleave', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
        });

        dropZone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropZone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                fileInput.files = files;
                callback(files[0]);
            }
        });

        dropZone.addEventListener('click', () => {
            fileInput.click();
        });

        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                callback(e.target.files[0]);
            }
        });
    },

    // 上传文件
    uploadFile: function(file, url, progressCallback, successCallback, errorCallback) {
        const formData = new FormData();
        formData.append('file', file);

        const xhr = new XMLHttpRequest();

        xhr.upload.addEventListener('progress', (e) => {
            if (e.lengthComputable) {
                const percentComplete = (e.loaded / e.total) * 100;
                progressCallback(percentComplete);
            }
        });

        xhr.addEventListener('load', () => {
            if (xhr.status === 200) {
                const response = JSON.parse(xhr.responseText);
                successCallback(response);
            } else {
                errorCallback(new Error(`Upload failed with status ${xhr.status}`));
            }
        });

        xhr.addEventListener('error', () => {
            errorCallback(new Error('Upload failed'));
        });

        xhr.open('POST', url);
        xhr.send(formData);
    }
};

// 页面初始化
document.addEventListener('DOMContentLoaded', function() {
    // 初始化工具提示
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.map(function(tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // 初始化弹出框
    const popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    popoverTriggerList.map(function(popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });

    // 暂时禁用自动隐藏警告消息功能来调试
    console.log('页面已加载，Bootstrap alerts应该正常工作');
    
    // 检查 Bootstrap 是否正确加载
    if (typeof bootstrap === 'undefined') {
        console.error('Bootstrap JavaScript 没有正确加载');
    } else {
        console.log('Bootstrap 已正确加载');
    }

    // 表单自动保存草稿（可选功能）
    const forms = document.querySelectorAll('form[data-auto-save]');
    forms.forEach(form => {
        const formId = form.getAttribute('id') || form.getAttribute('name');
        if (formId) {
            // 加载草稿
            const draft = localStorage.getItem(`draft_${formId}`);
            if (draft) {
                const data = JSON.parse(draft);
                Object.keys(data).forEach(key => {
                    const field = form.querySelector(`[name="${key}"]`);
                    if (field) {
                        field.value = data[key];
                    }
                });
            }

            // 保存草稿
            form.addEventListener('input', Utils.debounce(() => {
                const data = Forms.serializeToJson(form);
                localStorage.setItem(`draft_${formId}`, JSON.stringify(data));
            }, 1000));
        }
    });
});

// 防抖函数
Utils.debounce = function(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
};

// 导出到全局作用域
window.Utils = Utils;
window.API = API;
window.Charts = Charts;
window.Tables = Tables;
window.Forms = Forms;
window.FileUpload = FileUpload;