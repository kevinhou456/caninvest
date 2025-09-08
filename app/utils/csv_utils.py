"""
CSV处理工具函数
"""

import csv
import io


def detect_csv_delimiter(sample_text: str) -> str:
    """
    使用csv.Sniffer检测CSV分隔符
    
    Args:
        sample_text: CSV文件的样本文本
        
    Returns:
        检测到的分隔符字符
    """
    try:
        # 使用csv.Sniffer自动检测方言
        dialect = csv.Sniffer().sniff(sample_text, delimiters=',;\t|')
        return dialect.delimiter
    except csv.Error:
        # 如果自动检测失败，手动检测最常用的分隔符
        if ';' in sample_text and sample_text.count(';') > sample_text.count(','):
            return ';'
        elif '\t' in sample_text and sample_text.count('\t') > sample_text.count(','):
            return '\t'
        elif '|' in sample_text and sample_text.count('|') > sample_text.count(','):
            return '|'
        else:
            return ','


def detect_csv_delimiter_from_file(file_path: str, encoding: str = 'utf-8-sig') -> str:
    """
    从文件检测CSV分隔符
    
    Args:
        file_path: CSV文件路径
        encoding: 文件编码
        
    Returns:
        检测到的分隔符字符
    """
    with open(file_path, 'r', encoding=encoding) as f:
        sample = f.read(1024)
        return detect_csv_delimiter(sample)


def detect_csv_delimiter_from_fileobj(file_obj, encoding: str = 'utf-8-sig') -> str:
    """
    从文件对象检测CSV分隔符
    
    Args:
        file_obj: 文件对象
        encoding: 文件编码
        
    Returns:
        检测到的分隔符字符
    """
    file_obj.seek(0)
    sample = file_obj.read(1024)
    file_obj.seek(0)
    
    # 如果是字节数据，需要解码
    if isinstance(sample, bytes):
        sample = sample.decode(encoding)
    
    return detect_csv_delimiter(sample)