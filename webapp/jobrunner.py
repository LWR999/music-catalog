import subprocess
import threading
import time
from collections import deque

# Simple in-process job registry (no external deps)
_jobs = deque(maxlen=20)
_jobs_lock = threading.Lock()

def _run_job(cmd: list[str]):
    start = time.strftime("%Y-%m-%d %H:%M:%S")
    rec = {"id": int(time.time()*1000), "cmd": " ".join(cmd), "start": start, "end": None, "rc": None}
    with _jobs_lock:
        _jobs.appendleft(rec)
    try:
        rc = subprocess.call(cmd)
    except Exception as e:
        rc = -1
    finally:
        rec["end"] = time.strftime("%Y-%m-%d %H:%M:%S")
        rec["rc"] = rc

def start_job(cmd: list[str]) -> int:
    t = threading.Thread(target=_run_job, args=(cmd,), daemon=True)
    t.start()
    # latest job id
    with _jobs_lock:
        return _jobs[0]["id"] if _jobs else -1

def job_status():
    with _jobs_lock:
        return list(_jobs)
