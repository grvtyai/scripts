#!/usr/bin/env python3

import argparse
import csv
import time
import subprocess
import psutil
from datetime import datetime
import sys

# ANSI Farben
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def banner():
    print(r"""
======================================
     Disk & NFS MONITORING TOOL
======================================
    """)

# ---------------------------------------------------------
# Disk Monitoring
# ---------------------------------------------------------
def monitor_disks(disks, interval, max_runtime, log_file=None, verbose=False):
    if not disks:
        print("Please state the Disk(s) you want to monitor. Multiple Disks must be separated by comma")
        return

    watched_disks = [d.strip() for d in disks]

    last_disk_stats = {}
    start_time = time.time()

    # CSV Setup
    if log_file:
        csvfile = open(log_file, "w", newline="")
        fieldnames = [
            "timestamp", "ram_percent",
            "cpu_user_percent", "cpu_system_percent", "cpu_iowait_percent", "cpu_idle_percent",
            "disk_device", "rps", "wps", "rKB", "wKB",
            "await_read", "await_write", "util"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
    else:
        writer = None

    def read_cpu_stats():
        with open("/proc/stat", "r") as f:
            for line in f:
                if line.startswith("cpu "):
                    parts = line.split()
                    values = list(map(int, parts[1:]))
                    total = sum(values)
                    idle = values[3]
                    iowait = values[4]
                    user = values[0]
                    system = values[2]
                    return {"total": total, "idle": idle, "iowait": iowait, "user": user, "system": system}
        return None

    prev_cpu = read_cpu_stats()
    time.sleep(0.1)

    while (time.time() - start_time) < max_runtime:
        timestamp = datetime.now().replace(microsecond=0).isoformat()
        remaining_time = int(max_runtime - (time.time() - start_time))

        # CPU
        cpu = read_cpu_stats()
        total_diff = cpu["total"] - prev_cpu["total"]
        idle_diff = cpu["idle"] - prev_cpu["idle"]
        iowait_diff = cpu["iowait"] - prev_cpu["iowait"]
        user_diff = cpu["user"] - prev_cpu["user"]
        system_diff = cpu["system"] - prev_cpu["system"]

        cpu_user = round(100 * user_diff / total_diff, 2)
        cpu_system = round(100 * system_diff / total_diff, 2)
        cpu_iowait = round(100 * iowait_diff / total_diff, 2)
        cpu_idle = round(100 * idle_diff / total_diff, 2)
        prev_cpu = cpu

        # RAM
        ram = round(psutil.virtual_memory().percent, 2)

        # Disk stats
        disk_data = {}
        now = time.time()
        with open("/proc/diskstats", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 14:
                    continue
                dev = parts[2]
                if dev not in watched_disks:
                    continue

                reads_completed = int(parts[3])
                sectors_read = int(parts[5])
                time_reading = int(parts[6])
                writes_completed = int(parts[7])
                sectors_written = int(parts[9])
                time_writing = int(parts[10])
                weighted_io_time = int(parts[13])

                prev = last_disk_stats.get(dev, None)
                if prev:
                    dt = now - prev["timestamp"]
                    dt = max(dt, 0.0001)

                    r_ops = reads_completed - prev["reads_completed"]
                    w_ops = writes_completed - prev["writes_completed"]

                    rps = r_ops / dt
                    wps = w_ops / dt

                    rKB = ((sectors_read - prev["sectors_read"]) * 512 / 1024) / dt
                    wKB = ((sectors_written - prev["sectors_written"]) * 512 / 1024) / dt

                    await_read = ((time_reading - prev["time_reading"]) / r_ops / 1000) if r_ops > 0 else 0
                    await_write = ((time_writing - prev["time_writing"]) / w_ops / 1000) if w_ops > 0 else 0

                    util = ((weighted_io_time - prev["weighted_io_time"]) / (dt * 1000)) * 100
                else:
                    rps = wps = rKB = wKB = await_read = await_write = util = 0

                # replace decimal point with comma for Excel
                def fmt(v, digits=2):
                    return str(round(v, digits)).replace('.', ',')

                disk_data[dev] = {
                    "rps": fmt(rps),
                    "wps": fmt(wps),
                    "rKB": fmt(rKB),
                    "wKB": fmt(wKB),
                    "await_read": fmt(await_read, 3),
                    "await_write": fmt(await_write, 3),
                    "util": fmt(util)
                }

                last_disk_stats[dev] = {
                    "reads_completed": reads_completed,
                    "sectors_read": sectors_read,
                    "time_reading": time_reading,
                    "writes_completed": writes_completed,
                    "sectors_written": sectors_written,
                    "time_writing": time_writing,
                    "weighted_io_time": weighted_io_time,
                    "timestamp": now
                }

        for dev, data in disk_data.items():
            if writer:
                writer.writerow({
                    "timestamp": timestamp,
                    "ram_percent": str(ram).replace('.', ','),
                    "cpu_user_percent": str(cpu_user).replace('.', ','),
                    "cpu_system_percent": str(cpu_system).replace('.', ','),
                    "cpu_iowait_percent": str(cpu_iowait).replace('.', ','),
                    "cpu_idle_percent": str(cpu_idle).replace('.', ','),
                    "disk_device": dev,
                    "rps": data["rps"],
                    "wps": data["wps"],
                    "rKB": data["rKB"],
                    "wKB": data["wKB"],
                    "await_read": data["await_read"],
                    "await_write": data["await_write"],
                    "util": data["util"]
                })
            else:
                print(f"[Disk {dev}] {timestamp} | RAM: {ram}% CPU User: {cpu_user}% System: {cpu_system}% IOWait: {cpu_iowait}% Idle: {cpu_idle}% rps: {data['rps']} wps: {data['wps']} rKB: {data['rKB']} wKB: {data['wKB']} await_r: {data['await_read']} await_w: {data['await_write']} util: {data['util']}%")

        if writer:
            print(f"{GREEN}[OK]{RESET} {timestamp} Measurement taken for Disk Utilisation | Time to go: {remaining_time} sec")

        if writer:
            csvfile.flush()
        time.sleep(interval)

    if writer:
        csvfile.close()

# ---------------------------------------------------------
# NFS Monitoring
# ---------------------------------------------------------
def monitor_nfs(interval, max_runtime, log_file=None, verbose=False):
    start_time = time.time()

    if log_file:
        csvfile = open(log_file, "w", newline="")
        fieldnames = [
            "timestamp",
            "read_ops_s", "read_kb_s", "read_kb_op", "read_rtt_ms", "read_exe_ms", "read_queue_ms",
            "write_ops_s", "write_kb_s", "write_kb_op", "write_rtt_ms", "write_exe_ms", "write_queue_ms"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    else:
        writer = None

    while (time.time() - start_time) < max_runtime:
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        remaining_time = int(max_runtime - (time.time() - start_time))

        try:
            result = subprocess.run(["nfsiostat", "1", "2"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            output = result.stdout.splitlines()

            read_blocks = [i for i, l in enumerate(output) if l.strip().startswith("read:")]
            if len(read_blocks) < 2:
                raise ValueError("Less than 2 read blocks found.")

            start = read_blocks[1]

            def clean_parts(parts):
                return [p.replace(',','.') for p in parts if not p.startswith('(') and not '%' in p]

            def parse_stats(line):
                parts = clean_parts(line.split())
                return {
                    "ops_s": float(parts[0]),
                    "kb_s": float(parts[1]),
                    "kb_op": float(parts[2]),
                    "retrans": int(parts[3]),
                    "rtt_ms": float(parts[4]),
                    "exe_ms": float(parts[5]),
                    "queue_ms": float(parts[6]),
                }

            read_stats = parse_stats(output[start+1])
            write_stats = parse_stats(output[start+3])

            if writer:
                writer.writerow({
                    "timestamp": timestamp,
                    "read_ops_s": read_stats["ops_s"],
                    "read_kb_s": read_stats["kb_s"],
                    "read_kb_op": read_stats["kb_op"],
                    "read_rtt_ms": read_stats["rtt_ms"],
                    "read_exe_ms": read_stats["exe_ms"],
                    "read_queue_ms": read_stats["queue_ms"],
                    "write_ops_s": write_stats["ops_s"],
                    "write_kb_s": write_stats["kb_s"],
                    "write_kb_op": write_stats["kb_op"],
                    "write_rtt_ms": write_stats["rtt_ms"],
                    "write_exe_ms": write_stats["exe_ms"],
                    "write_queue_ms": write_stats["queue_ms"],
                })
            else:
                print(f"[NFS] {timestamp} | Read: ops/s={read_stats['ops_s']} kb/s={read_stats['kb_s']} kb/op={read_stats['kb_op']} RTT={read_stats['rtt_ms']}ms exe={read_stats['exe_ms']}ms queue={read_stats['queue_ms']}ms")
                print(f"[NFS] {timestamp} | Write: ops/s={write_stats['ops_s']} kb/s={write_stats['kb_s']} kb/op={write_stats['kb_op']} RTT={write_stats['rtt_ms']}ms exe={write_stats['exe_ms']}ms queue={write_stats['queue_ms']}ms")

            if writer:
                print(f"{GREEN}[OK]{RESET} {timestamp} Measurement taken for NFS | Time to go: {remaining_time} sec")

        except Exception as e:
            print(f"{RED}[FAIL]{RESET} {timestamp} An Error occurred during the Measurement: {e}")

        if writer:
            csvfile.flush()
        time.sleep(interval)

    if writer:
        csvfile.close()

# ---------------------------------------------------------
# CLI Handling
# ---------------------------------------------------------
def main():
    banner()
    parser = argparse.ArgumentParser(description="Disk & NFS Monitoring Tool - by grvtyai")
    parser.add_argument("-d", "--disk", type=str, help="Run Disk Monitoring. Specify disks comma-separated.")
    parser.add_argument("-n", "--nfs", action="store_true", help="Run NFS Monitoring")
    parser.add_argument("-l", "--log", type=str, nargs='?', const='', help="Log output to file")
    parser.add_argument("-i", "--interval", type=int, default=5, help="Interval in seconds")
    parser.add_argument("-m", "--max_runtime", type=int, default=30, help="Maximum runtime in seconds")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    # Falls keine Argumente übergeben wurden, Help anzeigen
    if len(sys.argv)==1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    args = parser.parse_args()

    disk_log = None
    nfs_log = None

    timestamp_name = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.log is not None:
        if args.log == '':
            if args.disk:
                disk_log = f"Disk-{timestamp_name}.csv"
            if args.nfs:
                nfs_log = f"NFS-{timestamp_name}.csv"
        else:
            if args.disk:
                disk_log = f"Disk-{args.log}"
            if args.nfs:
                nfs_log = f"NFS-{args.log}"

    if args.disk and not args.disk.strip():
        print("Please state the Disk(s) you want to monitor. Multiple Disks must be separated by comma")
        sys.exit(1)

    # Start Monitoring
    if args.disk:
        monitor_disks(args.disk.split(','), args.interval, args.max_runtime, disk_log, args.verbose)

    if args.nfs:
        monitor_nfs(args.interval, args.max_runtime, nfs_log, args.verbose)


if __name__ == "__main__":
    main()
