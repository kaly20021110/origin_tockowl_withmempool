import os
import re
from datetime import datetime
from collections import defaultdict

# 日志时间格式
TIME_FORMAT = "%Y/%m/%d %H:%M:%S.%f"

def parse_log_file(log_file):
    """
    解析单个日志文件，提取 batch 创建时间和接收时间
    返回两个 dict：
        - create_times[(creator_node, batch_id)] = datetime
        - receive_times[(creator_node, batch_id)] = [datetime, ...]
    """
    create_pattern = re.compile(r'create Block node (\d+) batch_id (\d+)')
    recv_pattern = re.compile(r'recieve payload from (\d+) and batchid is (\d+)')
    timestamp_pattern = re.compile(r'\[WARN\] ([\d/:.\s]+)')

    create_times = {}
    receive_times = defaultdict(list)

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 提取时间戳
            time_match = timestamp_pattern.match(line)
            if not time_match:
                continue
            timestamp = datetime.strptime(time_match.group(1).strip(), TIME_FORMAT)

            # 匹配 create Block
            create_match = create_pattern.search(line)
            if create_match:
                node_id = int(create_match.group(1))
                batch_id = int(create_match.group(2))
                create_times[(node_id, batch_id)] = timestamp

            # 匹配 recieve payload
            recv_match = recv_pattern.search(line)
            if recv_match:
                node_id = int(recv_match.group(1))
                batch_id = int(recv_match.group(2))
                receive_times[(node_id, batch_id)].append(timestamp)

    return create_times, receive_times

def merge_batch_data(log_files):
    """
    从多个日志文件中合并 batch 创建和接收时间
    """
    all_create_times = {}
    all_receive_times = defaultdict(list)

    for log_file in log_files:
        create_times, receive_times = parse_log_file(log_file)
        all_create_times.update(create_times)
        for key, times in receive_times.items():
            all_receive_times[key].extend(times)

    return all_create_times, all_receive_times

def compute_delays(create_times, receive_times):
    """
    计算每个 batch 的平均接收延迟以及整体平均延迟
    """
    total_delay = 0.0
    total_count = 0

    print("📊 每个 Batch 的平均接收延迟：")
    print("Batch (creator, id) | Avg Delay (s) | Num Received")
    print("--------------------|----------------|---------------")

    for key in sorted(create_times.keys()):
        create_time = create_times[key]
        recv_list = receive_times.get(key, [])
        batch_str = f"({key[0]}, {key[1]})"
        if recv_list:
            delays = [(recv_time - create_time).total_seconds() for recv_time in recv_list]
            avg_delay = sum(delays) / len(delays)
            total_delay += sum(delays)
            total_count += len(delays)
           # print(f"{batch_str:<20} | {avg_delay:<14.6f} | {len(recv_list)}")
       # else:
         #   print(f"{batch_str:<20} | No receive data  | 0")

    if total_count > 0:
        overall_avg = total_delay / total_count
        print("\n✅ 全部接收事件的平均延迟：")
        print(f"总接收次数: {total_count}")
        print(f"平均延迟: {overall_avg:.6f} 秒")
    else:
        print("\n⚠️ 无任何接收事件，无法计算平均延迟。")

if __name__ == '__main__':
    log_dir = 'logs/2025-05-29v15-00-55'
    log_files = [
        os.path.join(log_dir, f'node-warn-{i}.log') for i in range(4)
    ]

    print("🔍 正在解析所有日志文件...\n")
    create_times, receive_times = merge_batch_data(log_files)
    compute_delays(create_times, receive_times)
