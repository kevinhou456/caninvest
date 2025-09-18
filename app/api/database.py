"""
数据库备份和恢复API
"""

import os
import shutil
import tempfile
from datetime import datetime
from flask import request, jsonify, send_file, current_app
from werkzeug.utils import secure_filename
from app.api import bp
from app import db


@bp.route('/database/backup', methods=['GET'])
def backup_database():
    """
    备份数据库
    """
    try:
        # 获取数据库文件路径
        db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
        current_app.logger.info(f"Database URI: {db_uri}")

        if db_uri.startswith('sqlite:///'):
            db_path = db_uri[10:]  # 移除 'sqlite:///'
        elif db_uri.startswith('sqlite://'):
            db_path = db_uri[9:]   # 移除 'sqlite://'
        else:
            return jsonify({'success': False, 'error': 'Only SQLite databases are supported for backup'}), 400

        current_app.logger.info(f"Parsed database path: {db_path}")

        # 如果是相对路径，转换为绝对路径
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)

        current_app.logger.info(f"Final database path: {db_path}")
        current_app.logger.info(f"File exists: {os.path.exists(db_path)}")

        if not os.path.exists(db_path):
            return jsonify({'success': False, 'error': f'Database file not found at: {db_path}'}), 404

        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f'family_investment_backup_{timestamp}.db'

        # 检查文件是否可读
        if not os.access(db_path, os.R_OK):
            return jsonify({'success': False, 'error': 'Database file is not readable'}), 403

        # 直接返回原文件，而不是创建副本
        return send_file(
            db_path,
            as_attachment=True,
            download_name=backup_filename,
            mimetype='application/octet-stream'
        )

    except Exception as e:
        current_app.logger.error(f"Database backup error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/database/restore', methods=['POST'])
def restore_database():
    """
    恢复数据库
    """
    try:
        # 检查是否有上传的文件
        if 'backup_file' not in request.files:
            return jsonify({'success': False, 'error': 'No backup file provided'}), 400

        backup_file = request.files['backup_file']
        if backup_file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        # 验证文件扩展名
        if not backup_file.filename.lower().endswith('.db'):
            return jsonify({'success': False, 'error': 'Invalid file type. Please upload a .db file'}), 400

        # 获取当前数据库路径
        db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
        if db_uri.startswith('sqlite:///'):
            db_path = db_uri.replace('sqlite:///', '')
        elif db_uri.startswith('sqlite://'):
            db_path = db_uri.replace('sqlite://', '')
        else:
            return jsonify({'success': False, 'error': 'Only SQLite databases are supported for restore'}), 400

        # 如果是相对路径，转换为绝对路径
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)

        # 创建当前数据库的备份
        current_backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if os.path.exists(db_path):
            shutil.copy2(db_path, current_backup_path)

        # 保存上传的文件到临时位置
        temp_dir = tempfile.mkdtemp()
        temp_file_path = os.path.join(temp_dir, secure_filename(backup_file.filename))
        backup_file.save(temp_file_path)

        # 验证上传的文件是否是有效的SQLite数据库
        try:
            import sqlite3
            conn = sqlite3.connect(temp_file_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            conn.close()

            # 检查是否包含预期的表
            table_names = [table[0] for table in tables]
            required_tables = ['family_member', 'account', 'transaction']
            if not any(table in table_names for table in required_tables):
                return jsonify({
                    'success': False,
                    'error': 'Invalid database file. Missing required tables.'
                }), 400

        except Exception as e:
            return jsonify({
                'success': False,
                'error': f'Invalid database file: {str(e)}'
            }), 400

        # 关闭当前数据库连接
        db.session.close()
        db.engine.dispose()

        # 替换数据库文件
        if os.path.exists(db_path):
            os.remove(db_path)
        shutil.move(temp_file_path, db_path)

        # 重新创建数据库连接
        db.engine.dispose()

        # 清理临时目录
        shutil.rmtree(temp_dir)

        return jsonify({
            'success': True,
            'message': 'Database restored successfully',
            'backup_created': current_backup_path
        })

    except Exception as e:
        current_app.logger.error(f"Database restore error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/database/info', methods=['GET'])
def database_info():
    """
    获取数据库信息
    """
    try:
        # 获取数据库文件路径
        db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
        if db_uri.startswith('sqlite:///'):
            db_path = db_uri.replace('sqlite:///', '')
        elif db_uri.startswith('sqlite://'):
            db_path = db_uri.replace('sqlite://', '')
        else:
            return jsonify({'success': False, 'error': 'Only SQLite databases are supported'}), 400

        # 如果是相对路径，转换为绝对路径
        if not os.path.isabs(db_path):
            db_path = os.path.abspath(db_path)

        if not os.path.exists(db_path):
            return jsonify({'success': False, 'error': 'Database file not found'}), 404

        # 获取文件大小
        file_size = os.path.getsize(db_path)

        # 获取最后修改时间
        last_modified = datetime.fromtimestamp(os.path.getmtime(db_path))

        # 获取表统计信息
        try:
            from app.models import FamilyMember, Account, Transaction
            member_count = FamilyMember.query.count()
            account_count = Account.query.count()
            transaction_count = Transaction.query.count()

            return jsonify({
                'success': True,
                'info': {
                    'file_size': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'last_modified': last_modified.isoformat(),
                    'member_count': member_count,
                    'account_count': account_count,
                    'transaction_count': transaction_count,
                    'path': db_path
                }
            })
        except Exception as e:
            return jsonify({
                'success': True,
                'info': {
                    'file_size': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'last_modified': last_modified.isoformat(),
                    'path': db_path,
                    'error': f'Could not get table statistics: {str(e)}'
                }
            })

    except Exception as e:
        current_app.logger.error(f"Database info error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500