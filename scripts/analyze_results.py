import os
import glob
import datetime

LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

def get_latest_task_dirs(limit=10):
    task_dirs = glob.glob(os.path.join(LOGS_DIR, "task_*"))
    task_dirs.sort(key=os.path.getmtime, reverse=True)
    return task_dirs[:limit]

def parse_brief(brief_path):
    metrics = {}
    if not os.path.exists(brief_path):
        return metrics
    with open(brief_path, "r", encoding="utf-8") as f:
        for line in f:
            if ":" in line:
                key, val = line.split(":", 1)
                metrics[key.strip()] = val.strip()
    return metrics

def analyze():
    print("="*60)
    print(f"OBS Java Bench - Automated Result Analysis ({datetime.datetime.now()})")
    print("="*60)
    
    latest_dirs = get_latest_task_dirs()
    if not latest_dirs:
        print("No log directories found in logs/")
        return

    print(f"{'Task Name':<25} | {'TPS':>10} | {'Success%':>10} | {'P99 Latency':>12}")
    print("-" * 65)
    
    for task_dir in latest_dirs:
        brief_path = os.path.join(task_dir, "brief.txt")
        metrics = parse_brief(brief_path)
        
        task_name = os.path.basename(task_dir)
        tps = metrics.get("Average TPS", "N/A")
        success = metrics.get("Success Rate", "N/A")
        p99 = metrics.get("P99 Latency (ms)", "N/A")
        
        print(f"{task_name:<25} | {tps:>10} | {success:>10} | {p99:>12}")

if __name__ == "__main__":
    analyze()
