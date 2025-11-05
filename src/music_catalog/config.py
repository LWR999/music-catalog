from __future__ import annotations
import os, sys, yaml
from dataclasses import dataclass
from typing import Any, Dict, List

DEFAULT_CONFIG_ENV = "MUSICCATALOG_CONFIG"

@dataclass
class Config:
    env: str
    db_path: str
    state_dir: str
    report_dir: str
    roots: List[Dict[str, Any]]
    scan: Dict[str, Any]
    audits: Dict[str, Any]
    normalization: Dict[str, Any]

def load_config(path: str | None) -> Config:
    cfg_path = path or os.environ.get(DEFAULT_CONFIG_ENV)
    if not cfg_path:
        print("Config path not provided. Use --config or set MUSICCATALOG_CONFIG.", file=sys.stderr)
        sys.exit(2)
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Config(
        env=raw.get("env", "dev"),
        db_path=raw["catalog"]["db_path"],
        state_dir=raw["catalog"]["state_dir"],
        report_dir=raw["report"]["out_dir"],
        roots=raw["roots"],
        scan=raw.get("scan", {}),
        audits=raw.get("audits", {}),
        normalization=raw.get("normalization", {}),
    )
