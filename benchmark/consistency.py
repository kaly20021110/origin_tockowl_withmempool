import re
import os

def extract_committed_digests(log_file):
    """
    提取给定 log 文件中所有形如 'Committed [digest]' 的字符串，并按顺序返回 digest 列表。
    """
    pattern = re.compile(r'commit Block node (\d+) batch_id (\d+)')
    digests = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            digests.extend(pattern.findall(line))
    return digests

def compare_digest_sequences(log_files):
    """
    比较多个 log 文件中的 Committed digest 序列是否完全一致。
    """
    digest_sequences = {}
    for log_file in log_files:
        digests = extract_committed_digests(log_file)
        digest_sequences[log_file] = digests

    # 获取第一个文件的 digest 序列作为参考
    ref_file, ref_digests = next(iter(digest_sequences.items()))

    # 逐个比较其余文件的 digest 序列
    for file, digests in digest_sequences.items():
        if digests != ref_digests:
            print(f"文件 {file} 与 {ref_file} 的 committed digest 序列不一致。")
            return False
    print("所有文件的 committed digest 序列完全一致。")
    return True

if __name__ == '__main__':
    # 指定 logs 目录下的文件
    log_dir = 'logs/2025-05-16v15-37-34'
    log_files = [
        os.path.join(log_dir, 'node-info-0.log'),
        os.path.join(log_dir, 'node-info-1.log'),
        os.path.join(log_dir, 'node-info-2.log'),
        os.path.join(log_dir, 'node-info-3.log'),
    ]
    compare_digest_sequences(log_files)