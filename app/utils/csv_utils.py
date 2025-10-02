"""
CSV处理工具函数
"""

import csv
import io
from collections import Counter
from typing import Dict, Iterable


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


def analyze_csv_content(content: str,
                        delimiters: Iterable[str] = (',', ';', '\t', '|'),
                        max_lines: int = 500) -> Dict[str, int]:
    """
    Analyze CSV text content to determine header position and delimiter.

    Args:
        content: Full CSV text content.
        delimiters: Candidate delimiters to evaluate.
        max_lines: Maximum number of lines to inspect when inferring the header.

    Returns:
        Dictionary with detected delimiter, header_index (0-based) and field_count.
    """
    if not content or not content.strip():
        return {'delimiter': ',', 'header_index': 0, 'field_count': 0}

    best_match = None

    for delimiter in delimiters:
        reader = csv.reader(io.StringIO(content), delimiter=delimiter)
        candidate_rows = []

        try:
            for idx, row in enumerate(reader):
                candidate_rows.append((idx, row))
                if max_lines and idx + 1 >= max_lines:
                    break
        except csv.Error:
            continue

        # 只考虑包含多个字段的行，用于判断结构化数据
        structured_rows = [(idx, row) for idx, row in candidate_rows if len(row) > 1]
        if not structured_rows:
            continue

        length_counter = Counter(len(row) for _, row in structured_rows)
        if not length_counter:
            continue

        common_length, _ = length_counter.most_common(1)[0]

        header_index = None
        for idx, row in structured_rows:
            if len(row) != common_length:
                continue

            tokens = [token.strip() for token in row if token and token.strip()]
            if not tokens:
                continue

            alpha_tokens = sum(1 for token in tokens if any(ch.isalpha() for ch in token))
            if alpha_tokens >= max(1, len(tokens) // 2):
                header_index = idx
                break

        if header_index is None:
            header_index = next(idx for idx, row in structured_rows if len(row) == common_length)

        consistent_rows = sum(1 for idx, row in structured_rows if idx >= header_index and len(row) == common_length)
        quality_score = (consistent_rows, -header_index)

        match_info = {
            'delimiter': delimiter,
            'header_index': header_index,
            'field_count': common_length
        }

        if best_match is None or quality_score > best_match[0]:
            best_match = (quality_score, match_info)

    if best_match:
        return best_match[1]

    return {'delimiter': ',', 'header_index': 0, 'field_count': 0}
