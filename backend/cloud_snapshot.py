"""Build a Pages snapshot from public canonical history in an explicit scratch directory.

The transient DuckDB exists only to reuse the established export calculations. Neither
the source history nor an existing site is changed; no application server is started.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
ROOT_ASSETS = frozenset({"index.html", "eye.html", "world.html", ".nojekyll"})
ASSET_SUFFIXES = frozenset({".js", ".css", ".svg", ".png", ".jpg", ".jpeg", ".webp",
                           ".gif", ".ico", ".woff", ".woff2", ".ttf", ".otf", ".json"})
PRIVATE_PARTS = frozenset({".git", ".env", "raw", "docs", "handoff", "transcripts",
                           "__pycache__", "node_modules"})
LOCAL_PATH = re.compile(r"(?<![A-Za-z])[A-Za-z]:[\\/]|/Users/|/home/")
SECRET = re.compile(r"github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{32,}")


def protected_path(path: Path) -> Path:
    """Resolve without opening input; reject the protected production data tree."""
    path = Path(path).resolve()
    protected = [ROOT / "backend" / "data"]
    if os.getenv("LOCALAPPDATA"):
        protected.append(Path(os.environ["LOCALAPPDATA"]) / "EagleEye" / "data")
    normalized = path.as_posix().lower()
    if "/appdata/local/eagleeye/data" in normalized:
        raise ValueError("Production data is outside the cloud preparation boundary")
    for item in protected:
        item = item.resolve()
        if path == item or item in path.parents:
            raise ValueError("Production data is outside the cloud preparation boundary")
    return path


def require_new_directory(path: Path) -> Path:
    path = protected_path(path)
    for shared in (ROOT / "web-publish", ROOT / "public", ROOT / "web-dist"):
        shared = shared.resolve()
        if path == shared or shared in path.parents:
            raise ValueError("Cloud preparation must not write into the publish clone or shared build directories")
    if path.exists():
        raise ValueError("Output and scratch directories must not already exist")
    return path


def disjoint_paths(*paths: Path):
    resolved = [protected_path(p) for p in paths]
    for i, left in enumerate(resolved):
        for right in resolved[i + 1:]:
            if left == right or left in right.parents or right in left.parents:
                raise ValueError("Input, output and scratch directories must be disjoint")


def safe_file(root: Path, relative: str) -> Path:
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts or "\\" in relative:
        raise ValueError("Unsafe relative public path")
    path = root / relative_path
    if path.is_symlink() or any(p.is_symlink() for p in path.parents if p != root.parent):
        raise ValueError("Symlinks are not allowed in public inputs")
    if not path.is_file() or root.resolve() not in path.resolve().parents:
        raise ValueError("Missing or escaping public input")
    return path


def scan_public_text(blob: bytes, label: str):
    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError:
        return
    if LOCAL_PATH.search(text) or SECRET.search(text):
        raise ValueError(f"Private path or credential-shaped content in {label}")


def asset_files(assets_dir: Path) -> list[str]:
    """Select built browser assets and the existing license inventory, never snapshot."""
    root = protected_path(assets_dir)
    if not root.is_dir():
        raise ValueError("An explicit built site directory is required")
    selected = set()
    for name in ROOT_ASSETS:
        if (root / name).exists():
            safe_file(root, name)
            selected.add(name)
    if "index.html" not in selected:
        raise ValueError("Built site is missing index.html")
    asset_root = root / "assets"
    if asset_root.is_symlink():
        raise ValueError("Symlink asset directories are not allowed")
    if asset_root.exists():
        for path in asset_root.rglob("*"):
            if path.is_symlink():
                raise ValueError("Symlink assets are not allowed")
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if path.suffix.lower() not in ASSET_SUFFIXES or any(p.lower() in PRIVATE_PARTS for p in path.parts):
                raise ValueError("Unexpected file in browser assets")
            safe_file(root, rel)
            selected.add(rel)
    inventory_path = safe_file(root, "licenses/inventory.json")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if not isinstance(inventory, list):
        raise ValueError("Malformed web runtime license inventory")
    selected.add("licenses/inventory.json")
    for item in inventory:
        if not isinstance(item, dict) or not isinstance(item.get("files"), list):
            raise ValueError("Malformed web runtime license record")
        for rel in item["files"]:
            if not isinstance(rel, str):
                raise ValueError("Malformed web runtime license file")
            target = "licenses/" + rel
            safe_file(root, target)
            selected.add(target)
    return sorted(selected)


def copy_assets(assets_dir: Path, destination: Path) -> dict:
    files = asset_files(assets_dir)
    for rel in files:
        source = safe_file(assets_dir, rel)
        blob = source.read_bytes()
        scan_public_text(blob, rel)
        target = destination / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
    (destination / ".nojekyll").write_text("", encoding="utf-8")
    return {"asset_files": len(files), "snapshot_copied_from_assets": False}


def run_checked(arguments: list[str], *, cwd: Path, scratch: Path) -> dict:
    env = os.environ.copy()
    # Override inherited local configuration before importing any application code.
    temporary = scratch / "temporary"
    temporary.mkdir(parents=True, exist_ok=True)
    env.update(EAGLEEYE_DATA_DIR=str(scratch / "unused-default"),
               EAGLEEYE_TWSE_WEBSITE="off", PYTHONDONTWRITEBYTECODE="1", PYTHONUTF8="1",
               TMPDIR=str(temporary), TEMP=str(temporary), TMP=str(temporary))
    result = subprocess.run([sys.executable, "-B", *arguments], cwd=cwd, env=env,
                            capture_output=True, text=True, encoding="utf-8",
                            errors="replace", timeout=300)
    if result.returncode:
        raise RuntimeError("Offline snapshot command failed: " + result.stderr[-2500:])
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else {}


def build_snapshot(history_dir: Path, assets_dir: Path, output_site: Path,
                   scratch_dir: Path, *, code_root: Path | None = None) -> dict:
    from .cloud_history import CanonicalHistory
    from .store import Store

    history_dir = protected_path(history_dir)
    assets_dir = protected_path(assets_dir)
    output_site = require_new_directory(output_site)
    scratch_dir = require_new_directory(scratch_dir)
    disjoint_paths(history_dir, assets_dir, output_site, scratch_dir)
    history = CanonicalHistory(history_dir)
    validated = history.validate()
    asset_files(assets_dir)  # Fail before creating outputs if input is incomplete.
    scratch_dir.mkdir(parents=True)
    data = scratch_dir / "data"
    hydration = history.hydrate(Store(data))
    staging = scratch_dir / "site"
    staging.mkdir()
    assets = copy_assets(assets_dir, staging)
    code_root = Path(code_root or ROOT).resolve()
    exported = run_checked(["-m", "backend.web_export", "--data-dir", str(data),
                            "--output", str(staging / "snapshot")],
                           cwd=code_root, scratch=scratch_dir)
    verified = run_checked([str(code_root / "scripts" / "verify_web_snapshot.py"), str(staging)],
                           cwd=code_root, scratch=scratch_dir)
    # The final output appears only after successful export and verification.
    output_site.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(staging, output_site)
    return {"result": "passed", "history": validated, "hydrated": hydration,
            "export": exported, "verification": verified, **assets}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", required=True, type=Path)
    parser.add_argument("--assets-dir", required=True, type=Path)
    parser.add_argument("--output-site", required=True, type=Path)
    parser.add_argument("--scratch-dir", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build_snapshot(**vars(args)), ensure_ascii=False))


if __name__ == "__main__":
    main()
