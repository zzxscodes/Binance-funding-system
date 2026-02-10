#!/usr/bin/env python3
"""
分析数据层到alpha完成计算的完整流程时间
从日志中提取关键时间点并计算各阶段耗时
"""
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import statistics

# 日志文件路径
LOG_DIR = Path("/data/long-short-infra/logs")

# 关键日志模式
PATTERNS = {
    'data_complete_notified': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - data_layer - INFO - \[data_layer\.py:\d+\] - '
        r'Notified data complete.*?at ([\d\-:\.\+TZ]+)'
    ),
    'data_complete_received': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - event_coordinator - INFO - \[event_coordinator\.py:\d+\] - '
        r'Data complete event received: (\d+) symbols at ([\d\-:\.\+TZ]+)'
    ),
    'strategy_trigger_received': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - strategy_process - INFO - \[strategy_process\.py:\d+\] - '
        r'Strategy trigger received: data_complete'
    ),
    'strategy_execution_start': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - strategy_process - INFO - \[strategy_process\.py:\d+\] - '
        r'Starting strategy execution for (\d+) symbols'
    ),
    'alpha_fetching_snapshot': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - alpha - INFO - \[alpha\.py:\d+\] - '
        r'Fetching snapshot: (\d+)d, mode=(\w+), ([\d\-]+) -> ([\d\-]+)'
    ),
    'alpha_calculators_completed': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - alpha - INFO - \[alpha\.py:\d+\] - '
        r'Alpha calculators completed: (\d+) calcs in ([\d\.]+)s'
    ),
    'alpha_completed': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - strategy_process - INFO - \[strategy_process\.py:\d+\] - '
        r'Alpha completed: non-zero weights=(\d+) \(from (\d+) calculators\)'
    ),
    'strategy_execution_completed': (
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - strategy_process - INFO - \[strategy_process\.py:\d+\] - '
        r'Strategy execution completed successfully in ([\d\.]+) seconds\. Generated positions for (\d+) accounts'
    ),
}

# 日志文件到模式的映射
LOG_FILE_PATTERNS = {
    'data_layer.log': ['data_complete_notified'],
    'event_coordinator.log': ['data_complete_received'],
    'strategy_process.log': ['strategy_trigger_received', 'strategy_execution_start', 'alpha_completed', 'strategy_execution_completed'],
    'alpha.log': ['alpha_fetching_snapshot', 'alpha_calculators_completed'],
}


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """解析时间戳字符串"""
    # 处理多种格式
    timestamp_str = timestamp_str.strip()
    
    # 移除时区信息中的Z，替换为+00:00
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1] + '+00:00'
    
    # 尝试解析ISO格式
    try:
        # 格式1: 2026-02-10 03:29:44 (简单格式，假设UTC)
        if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', timestamp_str):
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return dt.replace(tzinfo=timezone.utc)
        
        # 格式2: 2026-02-10 03:29:44.526777+00:00 (带微秒和时区)
        if '+' in timestamp_str or timestamp_str.endswith('+00:00'):
            # 带时区的格式
            if '+' in timestamp_str:
                dt_str, tz_str = timestamp_str.rsplit('+', 1)
                dt = datetime.fromisoformat(dt_str)
                return dt.replace(tzinfo=timezone.utc)
            else:
                return datetime.fromisoformat(timestamp_str)
        else:
            # 假设是UTC时间
            dt = datetime.fromisoformat(timestamp_str)
            return dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        print(f"Warning: Failed to parse timestamp '{timestamp_str}': {e}")
        return None


def parse_log_line(line: str, pattern_name: str) -> Optional[Dict]:
    """解析单行日志"""
    pattern = PATTERNS.get(pattern_name)
    if not pattern:
        return None
    
    match = re.search(pattern, line)
    if not match:
        return None
    
    groups = match.groups()
    log_time_str = groups[0]
    log_time = parse_timestamp(log_time_str)
    
    if log_time is None:
        return None
    
    result = {
        'log_time': log_time,
        'log_time_str': log_time_str,
        'pattern': pattern_name,
    }
    
    # 根据不同的模式提取额外信息
    if pattern_name == 'data_complete_notified':
        result['notify_time'] = parse_timestamp(groups[1])
    elif pattern_name == 'data_complete_received':
        result['symbols_count'] = int(groups[1])
        result['event_time'] = parse_timestamp(groups[2])
    elif pattern_name == 'strategy_trigger_received':
        pass
    elif pattern_name == 'strategy_execution_start':
        result['symbols_count'] = int(groups[1])
    elif pattern_name == 'alpha_fetching_snapshot':
        result['history_days'] = int(groups[1])
        result['mode'] = groups[2]
    elif pattern_name == 'alpha_calculators_completed':
        result['calc_count'] = int(groups[1])
        result['calc_duration'] = float(groups[2])
    elif pattern_name == 'alpha_completed':
        result['non_zero_weights'] = int(groups[1])
        result['calc_count'] = int(groups[2])
    elif pattern_name == 'strategy_execution_completed':
        result['total_duration'] = float(groups[1])
        result['accounts_count'] = int(groups[2])
    
    return result


def read_log_file(file_path: Path) -> List[str]:
    """读取日志文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.readlines()
    except Exception as e:
        print(f"Warning: Failed to read {file_path}: {e}")
        return []


def extract_events() -> Dict[str, List[Dict]]:
    """从日志文件中提取所有事件"""
    events = defaultdict(list)
    
    # 用于去重：记录已处理的事件（基于时间戳和关键信息）
    seen_events = defaultdict(set)
    
    # 读取各个日志文件
    # 优先读取日期日志文件（更完整），如果没有则读取主日志文件
    from datetime import date
    today = date.today().strftime('%Y%m%d')
    
    log_files_to_check = [
        ('data_layer.log', ['data_complete_notified']),
        ('event_coordinator.log', ['data_complete_received']),
        ('strategy_process.log', ['strategy_trigger_received', 'strategy_execution_start', 'alpha_fetching_snapshot', 'alpha_calculators_completed', 'alpha_completed', 'strategy_execution_completed']),
        ('alpha.log', ['alpha_fetching_snapshot', 'alpha_calculators_completed']),
    ]
    
    for base_filename, pattern_names in log_files_to_check:
        # 优先读取日期日志文件
        date_file = LOG_DIR / f"{base_filename.replace('.log', '')}_{today}.log"
        base_file = LOG_DIR / base_filename
        
        # 决定读取哪个文件：如果日期文件存在且较新，优先读取日期文件
        file_to_read = None
        if date_file.exists() and base_file.exists():
            # 如果两个文件都存在，比较修改时间，读取较新的
            if date_file.stat().st_mtime >= base_file.stat().st_mtime:
                file_to_read = date_file
            else:
                file_to_read = base_file
        elif date_file.exists():
            file_to_read = date_file
        elif base_file.exists():
            file_to_read = base_file
        
        if file_to_read:
            lines = read_log_file(file_to_read)
            for line in lines:
                for pattern_name in pattern_names:
                    event = parse_log_line(line, pattern_name)
                    if event:
                        # 去重：基于时间戳和事件类型
                        event_key = (event['log_time'], pattern_name)
                        if event_key not in seen_events[pattern_name]:
                            seen_events[pattern_name].add(event_key)
                            events[pattern_name].append(event)
    
    # 处理alpha的日期日志文件（如果存在）
    alpha_date_file = LOG_DIR / f'alpha_{today}.log'
    if alpha_date_file.exists():
        lines = read_log_file(alpha_date_file)
        for line in lines:
            for pattern_name in ['alpha_fetching_snapshot', 'alpha_calculators_completed']:
                event = parse_log_line(line, pattern_name)
                if event:
                    event_key = (event['log_time'], pattern_name)
                    if event_key not in seen_events[pattern_name]:
                        seen_events[pattern_name].add(event_key)
                        events[pattern_name].append(event)
    
    return events


def match_rounds(events: Dict[str, List[Dict]]) -> List[Dict]:
    """匹配同一轮的事件"""
    rounds = []
    
    # 获取所有数据完成通知的时间
    data_complete_notified = sorted(events.get('data_complete_notified', []), key=lambda x: x['log_time'])
    
    # 为每个事件类型创建已使用标记
    used_events = {event_type: set() for event_type in events.keys()}
    
    for notify_event in data_complete_notified:
        notify_time = notify_event['log_time']
        
        # 查找同一轮的其他事件（在通知时间后的合理时间窗口内，比如60秒内）
        window_seconds = 60  # 60秒窗口，因为有些轮次可能需要更长时间
        
        round_data = {
            'round_id': len(rounds) + 1,
            'data_complete_notified': notify_event,
        }
        
        # 查找事件协调器接收事件（应该几乎同时发生，在1秒内）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('data_complete_received', []):
            if id(event) in used_events.get('data_complete_received', set()):
                continue
            time_diff = (event['log_time'] - notify_time).total_seconds()
            if 0 <= time_diff < 5.0:  # 应该在5秒内
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['data_complete_received'] = best_match
            used_events['data_complete_received'].add(id(best_match))
        
        # 查找策略触发接收（应该在通知后很快发生）
        prev_time = round_data.get('data_complete_received', {}).get('log_time', notify_time)
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('strategy_trigger_received', []):
            if id(event) in used_events.get('strategy_trigger_received', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['strategy_trigger_received'] = best_match
            used_events['strategy_trigger_received'].add(id(best_match))
            prev_time = best_match['log_time']
        
        # 查找策略执行开始（应该在触发后很快发生，通常在同一秒）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('strategy_execution_start', []):
            if id(event) in used_events.get('strategy_execution_start', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['strategy_execution_start'] = best_match
            used_events['strategy_execution_start'].add(id(best_match))
            prev_time = best_match['log_time']
        
        # 查找alpha获取快照（应该在策略开始后发生，通常在同一秒或稍后）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('alpha_fetching_snapshot', []):
            if id(event) in used_events.get('alpha_fetching_snapshot', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['alpha_fetching_snapshot'] = best_match
            used_events['alpha_fetching_snapshot'].add(id(best_match))
            prev_time = best_match['log_time']
        
        # 查找alpha计算器完成（应该在获取快照后发生，可能需要几秒到几十秒）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('alpha_calculators_completed', []):
            if id(event) in used_events.get('alpha_calculators_completed', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['alpha_calculators_completed'] = best_match
            used_events['alpha_calculators_completed'].add(id(best_match))
            prev_time = best_match['log_time']
        
        # 查找alpha完成（应该在计算器完成后很快发生，通常在同一秒）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('alpha_completed', []):
            if id(event) in used_events.get('alpha_completed', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['alpha_completed'] = best_match
            used_events['alpha_completed'].add(id(best_match))
            prev_time = best_match['log_time']
        
        # 查找策略执行完成（应该在alpha完成后很快发生，通常在同一秒）
        best_match = None
        best_time_diff = float('inf')
        for event in events.get('strategy_execution_completed', []):
            if id(event) in used_events.get('strategy_execution_completed', set()):
                continue
            time_diff = (event['log_time'] - prev_time).total_seconds()
            if 0 <= time_diff < window_seconds:
                if time_diff < best_time_diff:
                    best_time_diff = time_diff
                    best_match = event
        if best_match:
            round_data['strategy_execution_completed'] = best_match
            used_events['strategy_execution_completed'].add(id(best_match))
        
        rounds.append(round_data)
    
    return rounds


def calculate_timings(rounds: List[Dict]) -> List[Dict]:
    """计算各阶段的时间"""
    results = []
    
    for round_data in rounds:
        result = {
            'round_id': round_data['round_id'],
            'timings': {},
            'durations': {},
        }
        
        # 获取起始时间（数据层通知时间）
        start_time = round_data.get('data_complete_notified', {}).get('log_time')
        if not start_time:
            continue
        
        result['start_time'] = start_time
        
        # 计算各阶段时间
        stages = [
            ('data_complete_received', 'Data Layer → Event Coordinator'),
            ('strategy_trigger_received', 'Event Coordinator → Strategy Trigger'),
            ('strategy_execution_start', 'Strategy Trigger → Strategy Start'),
            ('alpha_fetching_snapshot', 'Strategy Start → Alpha Fetch Start'),
            ('alpha_calculators_completed', 'Alpha Fetch → Calculators Done'),
            ('alpha_completed', 'Calculators Done → Alpha Done'),
            ('strategy_execution_completed', 'Alpha Done → Strategy Complete'),
        ]
        
        prev_time = start_time
        for stage_key, stage_name in stages:
            event = round_data.get(stage_key)
            if event:
                event_time = event.get('log_time')
                if event_time:
                    duration = (event_time - prev_time).total_seconds() * 1000  # 转换为毫秒
                    result['timings'][stage_name] = event_time
                    result['durations'][stage_name] = duration
                    prev_time = event_time
        
        # 计算总时间
        end_event = round_data.get('strategy_execution_completed')
        if end_event:
            end_time = end_event.get('log_time')
            if end_time:
                total_duration = (end_time - start_time).total_seconds() * 1000
                result['total_duration_ms'] = total_duration
                result['end_time'] = end_time
        
        # 添加额外信息
        if 'alpha_calculators_completed' in round_data:
            calc_event = round_data['alpha_calculators_completed']
            result['calc_duration'] = calc_event.get('calc_duration', 0) * 1000
        
        # 使用日志中记录的实际耗时来改进时间计算
        # 如果"Alpha Fetch → Calculators Done"阶段有calc_duration，说明大部分时间在数据获取
        if 'alpha_fetching_snapshot' in round_data and 'alpha_calculators_completed' in round_data:
            fetch_time = round_data['alpha_fetching_snapshot'].get('log_time')
            calc_complete_time = round_data['alpha_calculators_completed'].get('log_time')
            calc_duration = round_data['alpha_calculators_completed'].get('calc_duration', 0) * 1000
            
            if fetch_time and calc_complete_time:
                total_fetch_to_calc = (calc_complete_time - fetch_time).total_seconds() * 1000
                # 如果总时间远大于calc_duration，说明大部分时间在数据获取
                if total_fetch_to_calc > calc_duration * 2:
                    # 估算数据获取时间 = 总时间 - calc_duration
                    data_fetch_duration = total_fetch_to_calc - calc_duration
                    # 更新durations
                    if 'Alpha Fetch → Calculators Done' in result['durations']:
                        # 将时间分解为数据获取和计算两部分
                        result['durations']['Alpha Fetch Data'] = data_fetch_duration
                        result['durations']['Alpha Calculators Execution'] = calc_duration
                        # 保留原来的总时间
                        result['durations']['Alpha Fetch → Calculators Done'] = total_fetch_to_calc
        
        # 使用strategy_execution_completed中记录的总耗时来验证
        if 'strategy_execution_completed' in round_data:
            reported_duration = round_data['strategy_execution_completed'].get('total_duration', 0) * 1000
            result['reported_total_duration_ms'] = reported_duration
        
        results.append(result)
    
    return results


def calculate_statistics(results: List[Dict]) -> Dict:
    """计算统计指标"""
    stats = {}
    
    # 收集所有阶段的时间数据
    stage_durations = defaultdict(list)
    total_durations = []
    
    for result in results:
        if 'total_duration_ms' in result:
            total_durations.append(result['total_duration_ms'])
        
        for stage_name, duration in result.get('durations', {}).items():
            stage_durations[stage_name].append(duration)
    
    # 计算总时间的统计
    if total_durations:
        stats['total'] = {
            'count': len(total_durations),
            'mean': statistics.mean(total_durations),
            'median': statistics.median(total_durations),
            'min': min(total_durations),
            'max': max(total_durations),
            'stdev': statistics.stdev(total_durations) if len(total_durations) > 1 else 0,
            'p95': sorted(total_durations)[int(len(total_durations) * 0.95)] if total_durations else 0,
            'p99': sorted(total_durations)[int(len(total_durations) * 0.99)] if total_durations else 0,
        }
    
    # 计算各阶段的统计
    for stage_name, durations in stage_durations.items():
        if durations:
            stats[stage_name] = {
                'count': len(durations),
                'mean': statistics.mean(durations),
                'median': statistics.median(durations),
                'min': min(durations),
                'max': max(durations),
                'stdev': statistics.stdev(durations) if len(durations) > 1 else 0,
                'p95': sorted(durations)[int(len(durations) * 0.95)] if durations else 0,
                'p99': sorted(durations)[int(len(durations) * 0.99)] if durations else 0,
            }
    
    return stats


def print_results(results: List[Dict], stats: Dict):
    """打印结果"""
    print("=" * 100)
    print("数据层到Alpha完成计算的流程时间分析")
    print("=" * 100)
    print()
    
    # 打印每轮的详细信息
    print("各轮次详细时间（毫秒）:")
    print("-" * 100)
    print(f"{'轮次':<6} {'总时间(ms)':<12} {'Data→Coord':<12} {'Coord→Trigger':<12} {'Trigger→Start':<12} "
          f"{'Start→Fetch':<12} {'Fetch→Calc':<12} {'Calc→Alpha':<12} {'Alpha→Complete':<12}")
    print("-" * 100)
    
    for result in results:
        if 'total_duration_ms' not in result:
            continue
        
        durations = result.get('durations', {})
        print(f"{result['round_id']:<6} "
              f"{result.get('total_duration_ms', 0):<12.2f} "
              f"{durations.get('Data Layer → Event Coordinator', 0):<12.2f} "
              f"{durations.get('Event Coordinator → Strategy Trigger', 0):<12.2f} "
              f"{durations.get('Strategy Trigger → Strategy Start', 0):<12.2f} "
              f"{durations.get('Strategy Start → Alpha Fetch Start', 0):<12.2f} "
              f"{durations.get('Alpha Fetch → Calculators Done', 0):<12.2f} "
              f"{durations.get('Calculators Done → Alpha Done', 0):<12.2f} "
              f"{durations.get('Alpha Done → Strategy Complete', 0):<12.2f}")
    
    print()
    print("=" * 100)
    print("统计指标（毫秒）:")
    print("=" * 100)
    print()
    
    # 打印总时间统计
    if 'total' in stats:
        print("总时间（从数据层通知到策略完成）:")
        print(f"  样本数: {stats['total']['count']}")
        print(f"  平均值: {stats['total']['mean']:.2f} ms")
        print(f"  中位数: {stats['total']['median']:.2f} ms")
        print(f"  最小值: {stats['total']['min']:.2f} ms")
        print(f"  最大值: {stats['total']['max']:.2f} ms")
        print(f"  标准差: {stats['total']['stdev']:.2f} ms")
        print(f"  P95:    {stats['total']['p95']:.2f} ms")
        print(f"  P99:    {stats['total']['p99']:.2f} ms")
        print()
    
    # 打印各阶段统计
    stage_order = [
        'Data Layer → Event Coordinator',
        'Event Coordinator → Strategy Trigger',
        'Strategy Trigger → Strategy Start',
        'Strategy Start → Alpha Fetch Start',
        'Alpha Fetch Data',
        'Alpha Calculators Execution',
        'Alpha Fetch → Calculators Done',
        'Calculators Done → Alpha Done',
        'Alpha Done → Strategy Complete',
    ]
    
    for stage_name in stage_order:
        if stage_name in stats:
            stage_stats = stats[stage_name]
            print(f"{stage_name}:")
            print(f"  样本数: {stage_stats['count']}")
            print(f"  平均值: {stage_stats['mean']:.2f} ms")
            print(f"  中位数: {stage_stats['median']:.2f} ms")
            print(f"  最小值: {stage_stats['min']:.2f} ms")
            print(f"  最大值: {stage_stats['max']:.2f} ms")
            print(f"  标准差: {stage_stats['stdev']:.2f} ms")
            print(f"  P95:    {stage_stats['p95']:.2f} ms")
            print(f"  P99:    {stage_stats['p99']:.2f} ms")
            print()


def main():
    """主函数"""
    print("正在读取日志文件...")
    events = extract_events()
    
    print(f"提取到的事件数量:")
    for pattern_name, event_list in events.items():
        print(f"  {pattern_name}: {len(event_list)}")
    
    print("\n正在匹配轮次...")
    rounds = match_rounds(events)
    print(f"匹配到 {len(rounds)} 轮")
    
    print("\n正在计算时间...")
    results = calculate_timings(rounds)
    print(f"计算出 {len(results)} 轮的时间")
    
    print("\n正在计算统计指标...")
    stats = calculate_statistics(results)
    
    print_results(results, stats)
    
    # 保存结果到JSON文件
    output_file = Path("/data/long-short-infra/pipeline_timing_analysis.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'results': [
                {
                    **r,
                    'start_time': r['start_time'].isoformat() if 'start_time' in r else None,
                    'end_time': r['end_time'].isoformat() if 'end_time' in r else None,
                    'timings': {
                        k: v.isoformat() if isinstance(v, datetime) else str(v)
                        for k, v in r.get('timings', {}).items()
                    }
                }
                for r in results
            ],
            'statistics': stats
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n详细结果已保存到: {output_file}")


if __name__ == '__main__':
    main()
