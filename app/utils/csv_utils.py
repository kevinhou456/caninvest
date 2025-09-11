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
    if not sample_text.strip():
        return ','
    
    print(f"DEBUG: Detecting delimiter from sample: {sample_text[:100]!r}")
    
    try:
        # 使用csv.Sniffer自动检测方言
        dialect = csv.Sniffer().sniff(sample_text, delimiters=',;\t|')
        detected = dialect.delimiter
        print(f"DEBUG: csv.Sniffer detected delimiter: {detected!r}")
        return detected
    except csv.Error as e:
        print(f"DEBUG: csv.Sniffer failed: {e}")
        # 如果自动检测失败，手动检测最常用的分隔符
        delimiters = [';', '\t', '|', ',']
        counts = {}
        
        for delimiter in delimiters:
            if delimiter in sample_text:
                counts[delimiter] = sample_text.count(delimiter)
        
        print(f"DEBUG: Delimiter counts: {counts}")
        
        if counts:
            # 返回出现次数最多的分隔符
            best_delimiter = max(counts.items(), key=lambda x: x[1])[0]
            print(f"DEBUG: Selected delimiter by count: {best_delimiter!r}")
            return best_delimiter
        else:
            print("DEBUG: No delimiters found, defaulting to comma")
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