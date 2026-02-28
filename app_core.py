import sqlite3
import os
import sys
import shutil
import subprocess
import platform
import datetime
import threading
import queue
import time
import json
import re
import io
import csv
import zipfile
import hashlib
import textwrap
import tempfile
import importlib
import urllib.request
import urllib.error
from urllib.parse import urlparse, urljoin, parse_qs, parse_qsl, urlencode, urlunparse

# --- External Libraries for Scraper (lazy-loaded for faster app startup) ---
requests = None
BeautifulSoup = None
Image = None
genai = None
webdriver = None
FirefoxService = None
FirefoxOptions = None
By = None
WebDriverWait = None
Select = None
EC = None
TimeoutException = None
WebDriverException = None
StaleElementReferenceException = None
NoSuchElementException = None
GeckoDriverManager = None
SCRAPER_AVAILABLE = False
SCRAPER_IMPORT_ERROR = ""
_SCRAPER_IMPORT_ATTEMPTED = False

# --- CONFIGURATION ---
APP_NAME = "BidManager"
DEFAULT_EXE_NAME = "BidManager.exe"
UPDATE_MANIFEST_FILE = "build_version.json"

try:
    from app_version import APP_VERSION, BUILD_UTC
except Exception:
    APP_VERSION = "dev"
    BUILD_UTC = ""

# --- Optional PDF support (PyMuPDF) ---
fitz = None
PDF_SUPPORT = False
try:
    import fitz as _fitz  # PyMuPDF

    fitz = _fitz
    PDF_SUPPORT = True
except Exception:
    fitz = None
    PDF_SUPPORT = False

def _get_app_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

def _safe_getcwd():
    try:
        return os.getcwd()
    except Exception:
        return _get_app_base_dir()

def _resolve_path(raw, fallback=""):
    txt = str(raw or "").strip()
    if not txt:
        txt = str(fallback or "").strip()
    if not txt:
        return _get_app_base_dir()
    if os.path.isabs(txt):
        return os.path.normpath(txt)
    return os.path.normpath(os.path.join(_get_app_base_dir(), txt))

APP_CONFIG_DIR = _get_app_base_dir()
APP_PATHS_FILE = os.path.join(APP_CONFIG_DIR, "app_paths.json")
USER_SETTINGS_FILE = os.path.join(APP_CONFIG_DIR, "user_settings.json")
LEGACY_LOCALAPPDATA_DIR = os.path.join(
    os.getenv("LOCALAPPDATA") or os.path.expanduser("~"),
    APP_NAME
)
LEGACY_LOCALAPPDATA_USER_SETTINGS_FILE = os.path.join(LEGACY_LOCALAPPDATA_DIR, "user_settings.json")
DB_FILE = "tender_manager.db"
ROOT_FOLDER = "My_Tender_Projects"
BASE_DOWNLOAD_DIRECTORY = "Tender_Downloads"
TEMPLATE_LIBRARY_FOLDER = "Checklist_Templates"
GOOGLE_API_KEY = "AIzaSyD78VcTNJCh3qlSYN9ZcLl4MdA3Q88TXQU"  # API Key from tender_scraper.py

def _new_scraper_session():
    if requests is None:
        return None
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWeb7Kit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    })
    return s


def ensure_scraper_dependencies():
    global requests, BeautifulSoup, Image, genai
    global webdriver, FirefoxService, FirefoxOptions, By, WebDriverWait, Select, EC
    global TimeoutException, WebDriverException, StaleElementReferenceException, NoSuchElementException
    global GeckoDriverManager, SCRAPER_AVAILABLE, SCRAPER_IMPORT_ERROR, _SCRAPER_IMPORT_ATTEMPTED
    if SCRAPER_AVAILABLE:
        return True
    if _SCRAPER_IMPORT_ATTEMPTED and not SCRAPER_AVAILABLE:
        return False
    _SCRAPER_IMPORT_ATTEMPTED = True
    try:
        _requests = importlib.import_module("requests")
        _BeautifulSoup = getattr(importlib.import_module("bs4"), "BeautifulSoup")
        _Image = importlib.import_module("PIL.Image")
        _genai = importlib.import_module("google.generativeai")
        _webdriver = importlib.import_module("selenium.webdriver")
        _FirefoxService = getattr(importlib.import_module("selenium.webdriver.firefox.service"), "Service")
        _FirefoxOptions = getattr(importlib.import_module("selenium.webdriver.firefox.options"), "Options")
        _By = getattr(importlib.import_module("selenium.webdriver.common.by"), "By")
        _ui_mod = importlib.import_module("selenium.webdriver.support.ui")
        _WebDriverWait = getattr(_ui_mod, "WebDriverWait")
        _Select = getattr(_ui_mod, "Select")
        _EC = importlib.import_module("selenium.webdriver.support.expected_conditions")
        _exc_mod = importlib.import_module("selenium.common.exceptions")
        _TimeoutException = getattr(_exc_mod, "TimeoutException")
        _WebDriverException = getattr(_exc_mod, "WebDriverException")
        _StaleElementReferenceException = getattr(_exc_mod, "StaleElementReferenceException")
        _NoSuchElementException = getattr(_exc_mod, "NoSuchElementException")
        _GeckoDriverManager = getattr(importlib.import_module("webdriver_manager.firefox"), "GeckoDriverManager")
    except Exception as e:
        SCRAPER_IMPORT_ERROR = str(e)
        SCRAPER_AVAILABLE = False
        print(f"Scraper dependencies missing: {e}")
        return False
    requests = _requests
    BeautifulSoup = _BeautifulSoup
    Image = _Image
    genai = _genai
    webdriver = _webdriver
    FirefoxService = _FirefoxService
    FirefoxOptions = _FirefoxOptions
    By = _By
    WebDriverWait = _WebDriverWait
    Select = _Select
    EC = _EC
    TimeoutException = _TimeoutException
    WebDriverException = _WebDriverException
    StaleElementReferenceException = _StaleElementReferenceException
    NoSuchElementException = _NoSuchElementException
    GeckoDriverManager = _GeckoDriverManager
    SCRAPER_AVAILABLE = True
    SCRAPER_IMPORT_ERROR = ""
    return True

def _normalize_db_file(raw):
    txt = str(raw or "").strip()
    if not txt:
        return _resolve_path("tender_manager.db")
    if not txt.lower().endswith(".db"):
        return _resolve_path(os.path.join(txt, "tender_manager.db"))
    return _resolve_path(txt)

def load_app_paths_config():
    defaults = {
        "db_file": _normalize_db_file(DB_FILE),
        "root_folder": _resolve_path(ROOT_FOLDER),
        "download_folder": _resolve_path(BASE_DOWNLOAD_DIRECTORY),
        "template_folder": _resolve_path(TEMPLATE_LIBRARY_FOLDER),
    }
    if not os.path.exists(APP_PATHS_FILE):
        return defaults
    try:
        with open(APP_PATHS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {
            "db_file": _normalize_db_file(raw.get("db_file", defaults["db_file"])),
            "root_folder": _resolve_path(raw.get("root_folder", defaults["root_folder"])),
            "download_folder": _resolve_path(raw.get("download_folder", defaults["download_folder"])),
            "template_folder": _resolve_path(raw.get("template_folder", defaults["template_folder"])),
        }
    except Exception:
        return defaults

def save_app_paths_config(db_file, root_folder, download_folder, template_folder=None):
    payload = {
        "db_file": _normalize_db_file(db_file),
        "root_folder": _resolve_path(root_folder),
        "download_folder": _resolve_path(download_folder),
        "template_folder": _resolve_path(template_folder or TEMPLATE_LIBRARY_FOLDER),
    }
    os.makedirs(APP_CONFIG_DIR, exist_ok=True)
    with open(APP_PATHS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def _load_user_settings():
    candidates = [USER_SETTINGS_FILE, LEGACY_LOCALAPPDATA_USER_SETTINGS_FILE]
    for cand in candidates:
        if not os.path.exists(cand):
            continue
        try:
            with open(cand, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                return raw
        except Exception:
            continue
    return {}

def _save_user_settings(settings):
    os.makedirs(APP_CONFIG_DIR, exist_ok=True)
    tmp_file = USER_SETTINGS_FILE + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)
    os.replace(tmp_file, USER_SETTINGS_FILE)

_user_settings_lock = threading.Lock()
_user_settings_cache = _load_user_settings()

def get_user_setting(key, default=None):
    with _user_settings_lock:
        return _user_settings_cache.get(key, default)

def set_user_setting(key, value):
    with _user_settings_lock:
        _user_settings_cache[key] = value
        try:
            _save_user_settings(_user_settings_cache)
        except Exception:
            pass


def _version_tuple(raw):
    parts = re.findall(r"\d+", str(raw or ""))
    return tuple(int(p) for p in parts) if parts else (0,)


def is_newer_version(candidate, current=None):
    return _version_tuple(candidate) > _version_tuple(current or APP_VERSION)


def get_local_update_info(update_dir):
    folder = str(update_dir or "").strip()
    if not folder:
        return {"ok": False, "message": "Update folder is not configured."}
    if not os.path.isdir(folder):
        return {"ok": False, "message": f"Update folder not found: {folder}"}

    manifest_path = os.path.join(folder, UPDATE_MANIFEST_FILE)
    if not os.path.exists(manifest_path):
        return {
            "ok": False,
            "message": f"Missing {UPDATE_MANIFEST_FILE} in update folder.",
        }

    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f) or {}
    except Exception as e:
        return {"ok": False, "message": f"Invalid update manifest: {e}"}

    version = str(manifest.get("version", "") or "").strip()
    exe_name = str(manifest.get("exe_name", DEFAULT_EXE_NAME) or DEFAULT_EXE_NAME).strip() or DEFAULT_EXE_NAME
    exe_path = os.path.join(folder, exe_name)
    if not version:
        return {"ok": False, "message": "Update manifest does not contain a version."}
    if not os.path.exists(exe_path):
        return {"ok": False, "message": f"Update EXE not found: {exe_path}"}

    newer = is_newer_version(version, APP_VERSION)
    return {
        "ok": True,
        "source": "local",
        "newer": newer,
        "current_version": APP_VERSION,
        "available_version": version,
        "exe_path": exe_path,
        "manifest_path": manifest_path,
        "built_at_utc": str(manifest.get("built_at_utc", "") or "").strip(),
        "notes": str(manifest.get("notes", "") or "").strip(),
    }


def get_remote_update_info(manifest_url):
    url = str(manifest_url or "").strip()
    if not url:
        return {"ok": False, "message": "Update manifest URL is not configured."}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BidManager-Updater/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        manifest = json.loads(raw) or {}
    except urllib.error.HTTPError as e:
        return {"ok": False, "message": f"Manifest request failed: HTTP {e.code}"}
    except urllib.error.URLError as e:
        return {"ok": False, "message": f"Manifest request failed: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Invalid remote manifest: {e}"}

    version = str(manifest.get("version", "") or "").strip()
    exe_url = str(manifest.get("exe_url", "") or "").strip()
    installer_url = str(manifest.get("installer_url", "") or "").strip()
    if not version:
        return {"ok": False, "message": "Remote update manifest does not contain a version."}
    if not exe_url and not installer_url:
        return {"ok": False, "message": "Remote update manifest missing exe_url/installer_url."}

    newer = is_newer_version(version, APP_VERSION)
    return {
        "ok": True,
        "source": "remote",
        "newer": newer,
        "current_version": APP_VERSION,
        "available_version": version,
        "exe_url": exe_url,
        "installer_url": installer_url,
        "manifest_url": url,
        "built_at_utc": str(manifest.get("built_at_utc", "") or "").strip(),
        "notes": str(manifest.get("notes", "") or "").strip(),
    }


def get_update_info(update_dir=None, manifest_url=None):
    remote_url = str(manifest_url or "").strip()
    if remote_url:
        info = get_remote_update_info(remote_url)
        if info.get("ok"):
            return info
        local_info = get_local_update_info(update_dir)
        if local_info.get("ok"):
            local_info["message"] = f"Remote update check failed, using local folder. ({info.get('message', '')})"
            return local_info
        return info
    return get_local_update_info(update_dir)


def download_remote_update_binary(binary_url, suffix=".exe"):
    url = str(binary_url or "").strip()
    if not url:
        return {"ok": False, "message": "Update URL is empty."}
    try:
        tmp_dir = tempfile.mkdtemp(prefix="bidmanager_remote_update_")
        out_path = os.path.join(tmp_dir, f"update{suffix}")
        req = urllib.request.Request(url, headers={"User-Agent": "BidManager-Updater/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp, open(out_path, "wb") as out:
            shutil.copyfileobj(resp, out)
        return {"ok": True, "path": out_path}
    except urllib.error.HTTPError as e:
        return {"ok": False, "message": f"Download failed: HTTP {e.code}"}
    except urllib.error.URLError as e:
        return {"ok": False, "message": f"Download failed: {e}"}
    except Exception as e:
        return {"ok": False, "message": f"Download failed: {e}"}


def _write_self_updater_script(source_exe, target_exe):
    updater_dir = tempfile.mkdtemp(prefix="bidmanager_updater_")
    script_path = os.path.join(updater_dir, "update_and_restart.cmd")
    script = "\n".join([
        "@echo off",
        "setlocal",
        f'set "SRC={source_exe}"',
        f'set "DST={target_exe}"',
        ":retry_copy",
        'copy /Y "%SRC%" "%DST%" >nul',
        "if errorlevel 1 (",
        "  timeout /t 1 /nobreak >nul",
        "  goto retry_copy",
        ")",
        'start "" "%DST%" --updated',
        'del "%~f0"',
    ])
    with open(script_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(script)
    return script_path


def launch_self_update(update_exe_path):
    if not getattr(sys, "frozen", False):
        return False, "Self-update is available only in packaged EXE mode."
    current_exe = os.path.abspath(sys.executable)
    source_exe = os.path.abspath(str(update_exe_path or "").strip())
    if not os.path.exists(source_exe):
        return False, f"Update EXE not found: {source_exe}"
    try:
        script_path = _write_self_updater_script(source_exe, current_exe)
        flags = 0
        if platform.system().lower().startswith("win"):
            flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
        subprocess.Popen(["cmd", "/c", script_path], creationflags=flags)
        return True, "Updater started. Closing app..."
    except Exception as e:
        return False, f"Failed to launch updater: {e}"


def launch_installer_update(installer_path):
    source_installer = os.path.abspath(str(installer_path or "").strip())
    if not os.path.exists(source_installer):
        return False, f"Installer not found: {source_installer}"
    try:
        flags = 0
        if platform.system().lower().startswith("win"):
            flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
        subprocess.Popen([source_installer], creationflags=flags)
        return True, "Installer launched. Closing app..."
    except Exception as e:
        return False, f"Failed to launch installer update: {e}"


def install_runtime_exe_if_needed():
    """
    First-run installer for onefile EXE:
    - Copies the EXE into LocalAppData\\BidManager\\installed.
    - Copies local config/data files if present.
    - Relaunches the installed EXE once.
    Returns True when current process should exit immediately.
    """
    if not getattr(sys, "frozen", False):
        return False
    if not platform.system().lower().startswith("win"):
        return False

    current_exe = os.path.abspath(sys.executable)
    current_dir = os.path.dirname(current_exe)
    # Onedir/installer layout keeps required runtime files in a local "_internal" folder.
    # Re-copying only the EXE to a different folder breaks startup (missing python*.dll).
    if os.path.isdir(os.path.join(current_dir, "_internal")):
        return False
    # If app is already running from LocalAppData app root, skip legacy self-install.
    try:
        local_root = os.path.normcase(os.path.abspath(LEGACY_LOCALAPPDATA_DIR))
        if os.path.normcase(current_exe).startswith(local_root + os.sep):
            return False
    except Exception:
        pass

    install_dir = os.path.join(LEGACY_LOCALAPPDATA_DIR, "installed")
    install_exe = os.path.join(install_dir, DEFAULT_EXE_NAME)
    if os.path.normcase(current_exe) == os.path.normcase(install_exe):
        return False
    if "--installed-launch" in sys.argv:
        return False

    try:
        os.makedirs(install_dir, exist_ok=True)
        src_stat = os.stat(current_exe)
        needs_copy = True
        if os.path.exists(install_exe):
            dst_stat = os.stat(install_exe)
            needs_copy = (dst_stat.st_size != src_stat.st_size) or (int(dst_stat.st_mtime) != int(src_stat.st_mtime))
        if needs_copy:
            shutil.copy2(current_exe, install_exe)
            src_internal = os.path.join(current_dir, "_internal")
            dst_internal = os.path.join(install_dir, "_internal")
            if os.path.isdir(src_internal):
                shutil.copytree(src_internal, dst_internal, dirs_exist_ok=True)

        # Migrate sidecar files from launch directory once.
        launch_dir = os.path.dirname(current_exe)
        for fname in ("app_paths.json", "user_settings.json", "tender_manager.db"):
            src = os.path.join(launch_dir, fname)
            dst = os.path.join(install_dir, fname)
            if os.path.exists(src) and not os.path.exists(dst):
                shutil.copy2(src, dst)

        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
        subprocess.Popen([install_exe, "--installed-launch"], creationflags=flags)
        return True
    except Exception as e:
        log_to_gui(f"Runtime install skipped: {e}")
        return False


def ensure_uninstall_files():
    if not getattr(sys, "frozen", False):
        return
    if not platform.system().lower().startswith("win"):
        return
    app_dir = _get_app_base_dir()
    ps1_path = os.path.join(app_dir, "uninstall.ps1")
    cmd_path = os.path.join(app_dir, "uninstall.cmd")
    ps1_lines = [
        "$ErrorActionPreference = 'SilentlyContinue'",
        "$AppName = 'BidManager'",
        "$AppDir = Split-Path -Parent $MyInvocation.MyCommand.Path",
        "$Desktop = [Environment]::GetFolderPath('Desktop')",
        "$Programs = Join-Path $env:APPDATA 'Microsoft\\Windows\\Start Menu\\Programs'",
        "$links = @(",
        "  (Join-Path $Desktop \"$AppName.lnk\"),",
        "  (Join-Path $Programs \"$AppName.lnk\"),",
        "  (Join-Path (Join-Path $Programs $AppName) \"$AppName.lnk\")",
        ")",
        "foreach ($lnk in $links) { if (Test-Path $lnk) { Remove-Item $lnk -Force } }",
        "$legacy = Join-Path $env:LOCALAPPDATA $AppName",
        "if ((Test-Path $legacy) -and ($legacy -ne $AppDir)) {",
        "  Remove-Item $legacy -Recurse -Force",
        "}",
        "Start-Sleep -Milliseconds 500",
        "Remove-Item $AppDir -Recurse -Force",
    ]
    cmd_lines = [
        "@echo off",
        "setlocal",
        "powershell -NoProfile -ExecutionPolicy Bypass -File \"%~dp0uninstall.ps1\"",
        "echo Uninstall started. You can close this window.",
        "timeout /t 2 /nobreak >nul",
    ]
    try:
        with open(ps1_path, "w", encoding="utf-8", newline="\r\n") as f:
            f.write("\r\n".join(ps1_lines) + "\r\n")
        with open(cmd_path, "w", encoding="utf-8", newline="\r\n") as f:
            f.write("\r\n".join(cmd_lines) + "\r\n")
    except Exception:
        pass

_path_cfg = load_app_paths_config()
DB_FILE = _path_cfg["db_file"]
ROOT_FOLDER = _path_cfg["root_folder"]
BASE_DOWNLOAD_DIRECTORY = _path_cfg["download_folder"]
TEMPLATE_LIBRARY_FOLDER = _path_cfg["template_folder"]

try:
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)
except Exception:
    DB_FILE = _resolve_path("tender_manager.db")
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)

try:
    os.makedirs(ROOT_FOLDER, exist_ok=True)
except Exception:
    ROOT_FOLDER = _resolve_path("My_Tender_Projects")
    os.makedirs(ROOT_FOLDER, exist_ok=True)

try:
    os.makedirs(BASE_DOWNLOAD_DIRECTORY, exist_ok=True)
except Exception:
    BASE_DOWNLOAD_DIRECTORY = _resolve_path("Tender_Downloads")
    os.makedirs(BASE_DOWNLOAD_DIRECTORY, exist_ok=True)

try:
    os.makedirs(TEMPLATE_LIBRARY_FOLDER, exist_ok=True)
except Exception:
    TEMPLATE_LIBRARY_FOLDER = _resolve_path("Checklist_Templates")
    os.makedirs(TEMPLATE_LIBRARY_FOLDER, exist_ok=True)


def sanitize_name(name, fallback="item"):
    txt = str(name or "").strip()
    txt = re.sub(r'[<>:"/\\|?*]+', "_", txt)
    txt = re.sub(r"\s+", " ", txt).strip(" .")
    return txt or fallback


def ensure_template_storage_folder(organization, template_name, template_id):
    org = sanitize_name(organization, "Organization")
    tname = sanitize_name(template_name, "Template")
    tid = int(template_id or 0)
    folder = os.path.join(TEMPLATE_LIBRARY_FOLDER, org, f"{tname}_{tid}")
    os.makedirs(folder, exist_ok=True)
    return folder

# --- THREAD COMMUNICATION ---
log_queue = queue.Queue()
captcha_req_queue = queue.Queue()
captcha_res_queue = queue.Queue()

def log_to_gui(message):
    """Puts a message into the queue for the GUI to display."""
    log_queue.put(message)
    print(message)

def resolve_project_folder_path(saved_path, project_title=""):
    raw = str(saved_path or "").strip()
    title = str(project_title or "").strip()
    if not raw:
        base = re.sub(r'[<>:"/\\|?*]+', "_", title).strip(" .") or "Project"
        return os.path.join(ROOT_FOLDER, base)
    drive, _ = os.path.splitdrive(raw)
    if drive and not os.path.exists(drive + os.sep):
        leaf = os.path.basename(raw.rstrip("\\/")) or title or "Project"
        leaf = re.sub(r'[<>:"/\\|?*]+', "_", str(leaf)).strip(" .") or "Project"
        return os.path.join(ROOT_FOLDER, leaf)
    return _resolve_path(raw)

def ensure_project_standard_folders(project_root):
    root = resolve_project_folder_path(project_root, os.path.basename(str(project_root or "").strip()))
    try:
        os.makedirs(root, exist_ok=True)
    except Exception:
        leaf = os.path.basename(str(root or "").rstrip("\\/")) or "Project"
        leaf = re.sub(r'[<>:"/\\|?*]+', "_", leaf).strip(" .") or "Project"
        root = os.path.join(ROOT_FOLDER, leaf)
        os.makedirs(root, exist_ok=True)
    ready = os.path.join(root, "Ready Docs")
    tender = os.path.join(root, "Tender Docs")
    working = os.path.join(root, "Working Docs")
    for p in (ready, tender, working):
        os.makedirs(p, exist_ok=True)
    return {
        "project_root": root,
        "ready_docs": ready,
        "tender_docs": tender,
        "working_docs": working,
    }

def copy_tree_contents(src_dir, dest_dir, exclude_names=None):
    if not (src_dir and os.path.isdir(src_dir)):
        return 0
    os.makedirs(dest_dir, exist_ok=True)
    excluded = {str(x).strip().lower() for x in (exclude_names or []) if str(x).strip()}
    copied = 0
    for name in os.listdir(src_dir):
        if str(name).strip().lower() in excluded:
            continue
        src = os.path.join(src_dir, name)
        dst = os.path.join(dest_dir, name)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
            copied += 1
        elif os.path.isfile(src):
            shutil.copy2(src, dst)
            copied += 1
    return copied

# --- DATABASE LAYER ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Existing tables
    c.execute('''CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, client_name TEXT, deadline TEXT, description TEXT, folder_path TEXT, project_value TEXT, prebid TEXT, status TEXT DEFAULT 'Active', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS checklist_items (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, sr_no INTEGER, req_file_name TEXT, description TEXT, subfolder TEXT DEFAULT 'Main', linked_file_path TEXT, status TEXT DEFAULT 'Pending', FOREIGN KEY(project_id) REFERENCES projects(id))''')
    
    # New tables for Scraper
    c.execute('''CREATE TABLE IF NOT EXISTS websites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        url TEXT,
        status_url TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS organizations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        website_id INTEGER,
        name TEXT,
        tender_count INTEGER,
        tenders_url TEXT,
        is_selected INTEGER DEFAULT 0,
        FOREIGN KEY(website_id) REFERENCES websites(id),
        UNIQUE(website_id, name)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS tenders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        website_id INTEGER,
        org_chain TEXT,
        tender_ref_no TEXT,
        tender_id TEXT UNIQUE,
        title TEXT,
        tender_value TEXT,
        emd TEXT,
        closing_date TEXT,
        opening_date TEXT,
        tender_url TEXT,
        status TEXT,
        is_archived INTEGER DEFAULT 0,
        is_downloaded INTEGER DEFAULT 0,
        is_bookmarked INTEGER DEFAULT 0,
        folder_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        location TEXT,
        tender_category TEXT,
        pre_bid_meeting_date TEXT,
        work_description TEXT,
        normalized_tender_url TEXT,
        FOREIGN KEY(website_id) REFERENCES websites(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS downloaded_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tender_id TEXT,
        file_name TEXT,
        file_type TEXT DEFAULT 'document',
        source_url TEXT,
        local_path TEXT,
        downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(tender_id) REFERENCES tenders(tender_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS checklist_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_no INTEGER,
        organization TEXT NOT NULL,
        template_name TEXT NOT NULL,
        description TEXT,
        notes TEXT,
        folder_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS checklist_template_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_id INTEGER NOT NULL,
        sr_no INTEGER DEFAULT 0,
        req_file_name TEXT,
        description TEXT,
        subfolder TEXT DEFAULT 'Main',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(template_id) REFERENCES checklist_templates(id) ON DELETE CASCADE
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS checklist_template_item_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        template_item_id INTEGER NOT NULL,
        file_name TEXT,
        source_name TEXT,
        stored_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(template_item_id) REFERENCES checklist_template_items(id) ON DELETE CASCADE
    )''')
    # Template migrations for existing DBs.
    for col, ddl in [
        ("template_no", "INTEGER"),
        ("description", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE checklist_templates ADD COLUMN {col} {ddl}")
        except sqlite3.OperationalError:
            pass
    c.execute("CREATE INDEX IF NOT EXISTS idx_tpl_org_name ON checklist_templates(organization, template_name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tpl_no ON checklist_templates(template_no)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tpl_items_tid ON checklist_template_items(template_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tpl_files_item ON checklist_template_item_files(template_item_id)")

    # Seed Websites if empty
    c.execute("SELECT count(*) FROM websites")
    if c.fetchone()[0] == 0:
        websites = [
            ("MahaTenders", "https://mahatenders.gov.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page", "https://mahatenders.gov.in/nicgep/app?page=WebTenderStatusLists&service=page"),
            ("ETenders", "https://etenders.gov.in/eprocure/app?page=FrontEndTendersByOrganisation&service=page", "https://etenders.gov.in/eprocure/app?page=WebTenderStatusLists&service=page"),
            ("Eprocure", "https://eprocure.gov.in/eprocure/app?page=FrontEndTendersByOrganisation&service=page", "https://eprocure.gov.in/eprocure/app?page=WebTenderStatusLists&service=page")
        ]
        c.executemany("INSERT INTO websites (name, url, status_url) VALUES (?, ?, ?)", websites)

    # Migrations for existing databases
    migrations = ["location", "tender_category", "pre_bid_meeting_date", "last_downloaded_at", "normalized_tender_url", "work_description"]
    for col in migrations:
        try:
            c.execute(f"ALTER TABLE tenders ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass
    try:
        c.execute("ALTER TABLE tenders ADD COLUMN is_archived INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # Backfill legacy archive marker stored in status.
    c.execute("UPDATE tenders SET is_archived=1 WHERE COALESCE(status,'')='Archived'")
    c.execute("UPDATE tenders SET status='' WHERE COALESCE(status,'')='Archived'")
    c.execute("UPDATE tenders SET status='' WHERE COALESCE(status,'')='Active'")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tenders_site_normurl ON tenders(website_id, normalized_tender_url)")
    download_migrations = [
        ("file_type", "TEXT DEFAULT 'document'"),
        ("source_url", "TEXT"),
        ("local_path", "TEXT"),
    ]
    for col, ddl in download_migrations:
        try:
            c.execute(f"ALTER TABLE downloaded_files ADD COLUMN {col} {ddl}")
        except sqlite3.OperationalError:
            pass
    try:
        c.execute("ALTER TABLE projects ADD COLUMN source_tender_id TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE projects ADD COLUMN project_value TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        c.execute("ALTER TABLE projects ADD COLUMN prebid TEXT")
    except sqlite3.OperationalError:
        pass
    c.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_source_tender_unique
           ON projects(source_tender_id)
           WHERE source_tender_id IS NOT NULL AND TRIM(source_tender_id) != ''"""
    )
    c.execute("DROP INDEX IF EXISTS idx_downloaded_file_unique")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_downloaded_file_unique ON downloaded_files(tender_id, file_name, file_type)")
    c.execute('''CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS auto_archive_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_at_utc TEXT NOT NULL,
        status TEXT NOT NULL,
        archived_count INTEGER DEFAULT 0,
        archived_status_updated INTEGER DEFAULT 0,
        websites_count INTEGER DEFAULT 0,
        notes TEXT
    )''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_auto_archive_runs_ts ON auto_archive_runs(run_at_utc)")

    conn.commit()
    conn.close()

# --- SCRAPER BACKEND ---
class ScraperBackend:
    captcha_solved_in_session = False
    gemini_model = None
    gemini_model_name = None
    session = None

    @staticmethod
    def add_website_logic(name, url, status_url):
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute("INSERT INTO websites (name, url, status_url) VALUES (?, ?, ?)", (name, url, status_url))
            conn.commit()
            return True
        except Exception as e:
            log_to_gui(f"Error adding website: {e}")
            return False
        finally:
            conn.close()

    @staticmethod
    def delete_website_logic(website_id):
        conn = sqlite3.connect(DB_FILE)
        try:
            conn.execute("DELETE FROM downloaded_files WHERE tender_id IN (SELECT tender_id FROM tenders WHERE website_id=?)", (website_id,))
            conn.execute("DELETE FROM tenders WHERE website_id=?", (website_id,))
            conn.execute("DELETE FROM organizations WHERE website_id=?", (website_id,))
            conn.execute("DELETE FROM websites WHERE id=?", (website_id,))
            conn.commit()
            return True
        except Exception as e:
            log_to_gui(f"Error deleting website: {e}")
            return False
        finally:
            conn.close()

    @staticmethod
    def clear_saved_scraper_details_logic(clear_orgs=False, clear_active=False, clear_archived=False):
        """
        Clears selected scraper data while preserving configured websites.
        - clear_orgs: organizations list/selections
        - clear_active: non-archived tenders (+ their download logs)
        - clear_archived: archived tenders (+ their download logs)
        """
        conn = sqlite3.connect(DB_FILE)
        try:
            c = conn.cursor()
            result = {
                "organizations": 0,
                "active_tenders": 0,
                "archived_tenders": 0,
                "downloaded_files": 0,
            }

            if clear_orgs:
                result["organizations"] = c.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
                c.execute("DELETE FROM organizations")

            if clear_active:
                active_ids = [r[0] for r in c.execute(
                    "SELECT tender_id FROM tenders WHERE COALESCE(is_archived,0)=0"
                ).fetchall() if r and r[0]]
                result["active_tenders"] = c.execute(
                    "SELECT COUNT(*) FROM tenders WHERE COALESCE(is_archived,0)=0"
                ).fetchone()[0]
                if active_ids:
                    q = ",".join("?" for _ in active_ids)
                    result["downloaded_files"] += c.execute(
                        f"SELECT COUNT(*) FROM downloaded_files WHERE tender_id IN ({q})",
                        tuple(active_ids)
                    ).fetchone()[0]
                    c.execute(f"DELETE FROM downloaded_files WHERE tender_id IN ({q})", tuple(active_ids))
                c.execute("DELETE FROM tenders WHERE COALESCE(is_archived,0)=0")

            if clear_archived:
                arch_ids = [r[0] for r in c.execute(
                    "SELECT tender_id FROM tenders WHERE COALESCE(is_archived,0)=1"
                ).fetchall() if r and r[0]]
                result["archived_tenders"] = c.execute(
                    "SELECT COUNT(*) FROM tenders WHERE COALESCE(is_archived,0)=1"
                ).fetchone()[0]
                if arch_ids:
                    q = ",".join("?" for _ in arch_ids)
                    result["downloaded_files"] += c.execute(
                        f"SELECT COUNT(*) FROM downloaded_files WHERE tender_id IN ({q})",
                        tuple(arch_ids)
                    ).fetchone()[0]
                    c.execute(f"DELETE FROM downloaded_files WHERE tender_id IN ({q})", tuple(arch_ids))
                c.execute("DELETE FROM tenders WHERE COALESCE(is_archived,0)=1")

            conn.commit()
            return result
        except Exception as e:
            log_to_gui(f"Error clearing saved scraper details: {e}")
            return None
        finally:
            conn.close()

    @staticmethod
    def get_websites():
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id, name, url, status_url FROM websites")
        rows = c.fetchall()
        conn.close()
        return {row[0]: {"name": row[1], "url": row[2], "status_url": row[3]} for row in rows}

    @staticmethod
    def safe_request(url):
        """Performs a request with automatic session refreshing if stale."""
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return None
        if ScraperBackend.session is None:
            ScraperBackend.session = _new_scraper_session()
        try:
            response = ScraperBackend.session.get(url, timeout=30)
            
            # Check for stale session using BeautifulSoup to be specific (like tender_scraper.py)
            is_stale = False
            try:
                soup = BeautifulSoup(response.text, 'html.parser')
                if soup.title and ("Stale Session" in soup.title.text or "Error" in soup.title.text):
                    is_stale = True
            except:
                if "Stale Session" in response.text:
                    is_stale = True

            if is_stale:
                log_to_gui("Stale session detected. Refreshing...")
                # Attempt to refresh session by hitting the specific page that resets the session correctly
                if 'app?' in url:
                    base_part = url.split('app?')[0]
                    refresh_url = base_part + 'app?page=FrontEndTendersByOrganisation&service=page'
                else:
                    parsed = urlparse(url)
                    refresh_url = f"{parsed.scheme}://{parsed.netloc}/nicgep/app?page=FrontEndTendersByOrganisation&service=page"
                
                ScraperBackend.session.get(refresh_url, timeout=30)
                time.sleep(2)
                # Retry original request
                response = ScraperBackend.session.get(url, timeout=30)
            return response
        except Exception as e:
            log_to_gui(f"Request failed: {e}")
            # Reset session on connection error to recover for subsequent requests
            ScraperBackend.session = _new_scraper_session()
            return None

    @staticmethod
    def get_working_gemini_model():
        if not ensure_scraper_dependencies():
            return None
        if ScraperBackend.gemini_model is not None:
            return ScraperBackend.gemini_model
        try:
            genai.configure(api_key=GOOGLE_API_KEY)
        except Exception as e:
            log_to_gui(f"Gemini config error: {e}")
            return None

        discovered = []
        try:
            for m in genai.list_models():
                methods = getattr(m, "supported_generation_methods", []) or []
                if "generateContent" in methods:
                    mname = getattr(m, "name", None)
                    if mname:
                        discovered.append(mname)
        except Exception as e:
            log_to_gui(f"Gemini list_models warning: {e}")

        def norm(name):
            return name.split("/", 1)[1] if name.startswith("models/") else name

        discovered_norm = [norm(x) for x in discovered]
        discovered_norm.sort(key=lambda n: (0 if "flash" in n.lower() else 1, 0 if "pro" in n.lower() else 1, n))
        fallback = [
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-1.5-flash-latest",
            "gemini-1.5-pro-latest",
            "gemini-1.5-flash",
            "gemini-pro",
        ]
        candidates = []
        seen = set()
        for name in discovered_norm + fallback:
            if name and name not in seen:
                seen.add(name)
                candidates.append(name)

        last_error = None
        for name in candidates:
            try:
                model = genai.GenerativeModel(name)
                # lightweight capability probe
                _ = model.generate_content("ok", generation_config={"temperature": 0})
                ScraperBackend.gemini_model = model
                ScraperBackend.gemini_model_name = name
                log_to_gui(f"Gemini model selected: {name}")
                return model
            except Exception as e:
                last_error = e
                continue

        if last_error:
            log_to_gui(f"Gemini model detection failed: {last_error}")
        return None

    @staticmethod
    def solve_captcha_with_gemini(image_data):
        if not ensure_scraper_dependencies():
            return None
        try:
            model = ScraperBackend.get_working_gemini_model()
            if model is None:
                return None
            img = Image.open(io.BytesIO(image_data))
            response = model.generate_content(["Extract the 6 alphanumeric characters from this CAPTCHA image. Return ONLY the text.", img])
            text = re.sub(r'[^a-zA-Z0-9]', '', response.text.strip())
            return text if len(text) == 6 else None
        except Exception as e:
            log_to_gui(f"Gemini Error: {e}")
            return None

    @staticmethod
    def handle_captcha_interaction(driver, context, submit_id="Submit"):
        if not ensure_scraper_dependencies():
            return False
        def _status_table_visible():
            try:
                table = driver.find_element(By.ID, "tabList")
                if not table.is_displayed():
                    return False
                rows = table.find_elements(By.XPATH, ".//tr[contains(@class, 'even') or contains(@class, 'odd')]")
                return len(rows) > 0
            except Exception:
                return False

        # If session CAPTCHA was solved already, try submitting directly first.
        if ScraperBackend.captcha_solved_in_session:
            try:
                btn = driver.find_element(By.ID, submit_id)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(2)
                if _status_table_visible():
                    return True
                # Session may have expired; fall through to solve CAPTCHA again.
                if len(driver.find_elements(By.ID, "captchaImage")) > 0:
                    ScraperBackend.captcha_solved_in_session = False
            except Exception:
                ScraperBackend.captcha_solved_in_session = False

        # If result table is already visible, treat as solved for this session.
        if _status_table_visible():
            ScraperBackend.captcha_solved_in_session = True
            return True
        try:
            captcha_img = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "captchaImage")))
        except:
            log_to_gui(f"No CAPTCHA found for {context}.")
            return True

        max_retries = 3
        for attempt in range(max_retries):
            try:
                captcha_input = driver.find_element(By.ID, "captchaText")
                btn = driver.find_element(By.ID, submit_id)
                img_data = captcha_img.screenshot_as_png

                # Try Gemini first (auto attempts).
                solution = ScraperBackend.solve_captcha_with_gemini(img_data)
                if not solution:
                    log_to_gui(f"CAPTCHA auto-solve failed for {context}. Retrying ({attempt + 1}/{max_retries})...")
                    try:
                        captcha_img = driver.find_element(By.ID, "captchaImage")
                    except Exception:
                        pass
                    continue

                captcha_input.clear()
                captcha_input.send_keys(solution)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3)
                
                # Check success: either result table loaded or CAPTCHA image disappeared.
                if _status_table_visible() or len(driver.find_elements(By.ID, "captchaImage")) == 0:
                    log_to_gui("CAPTCHA Solved!")
                    ScraperBackend.captcha_solved_in_session = True
                    return True
                else:
                    log_to_gui("CAPTCHA Failed. Retrying...")
                    captcha_img = driver.find_element(By.ID, "captchaImage") # Re-find
            except Exception as e:
                log_to_gui(f"Captcha Error: {e}")
        # Manual fallback after 3 failed auto attempts.
        try:
            for manual_try in range(3):
                try:
                    captcha_input = driver.find_element(By.ID, "captchaText")
                    btn = driver.find_element(By.ID, submit_id)
                    img_data = driver.find_element(By.ID, "captchaImage").screenshot_as_png
                except Exception:
                    log_to_gui(f"Could not load CAPTCHA image for manual input ({context}).")
                    return False

                log_to_gui(f"Requesting Manual CAPTCHA for {context} (manual try {manual_try + 1}/3)...")
                captcha_req_queue.put(img_data)
                solution = captcha_res_queue.get()
                if not solution:
                    return False  # user cancelled

                captcha_input.clear()
                captcha_input.send_keys(solution)
                driver.execute_script("arguments[0].click();", btn)
                time.sleep(3)
                if _status_table_visible() or len(driver.find_elements(By.ID, "captchaImage")) == 0:
                    log_to_gui("CAPTCHA Solved (manual)!")
                    ScraperBackend.captcha_solved_in_session = True
                    return True
                log_to_gui("Manual CAPTCHA incorrect. Retrying manual input...")
            return False
        except Exception as e:
            log_to_gui(f"Captcha manual fallback error: {e}")
            return False

    @staticmethod
    def ensure_download_tables():
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS downloaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tender_id TEXT,
            file_name TEXT,
            file_type TEXT DEFAULT 'document',
            source_url TEXT,
            local_path TEXT,
            downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(tender_id) REFERENCES tenders(tender_id)
        )''')
        download_migrations = [
            ("file_type", "TEXT DEFAULT 'document'"),
            ("source_url", "TEXT"),
            ("local_path", "TEXT"),
        ]
        for col, ddl in download_migrations:
            try:
                c.execute(f"ALTER TABLE downloaded_files ADD COLUMN {col} {ddl}")
            except sqlite3.OperationalError:
                pass
        c.execute("DROP INDEX IF EXISTS idx_downloaded_file_unique")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_downloaded_file_unique ON downloaded_files(tender_id, file_name, file_type)")
        conn.commit()
        conn.close()

    @staticmethod
    def get_downloaded_file_log(tender_id):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT file_name FROM downloaded_files WHERE tender_id=?", (str(tender_id),))
        rows = c.fetchall()
        conn.close()
        return {r[0] for r in rows}

    @staticmethod
    def log_downloaded_file(tender_id, file_name, file_type="document", source_url=None, local_path=None):
        conn = sqlite3.connect(DB_FILE)
        conn.execute(
            "INSERT OR IGNORE INTO downloaded_files (tender_id, file_name, file_type, source_url, local_path) VALUES (?, ?, ?, ?, ?)",
            (str(tender_id), str(file_name), str(file_type), source_url, local_path)
        )
        conn.commit()
        conn.close()

    @staticmethod
    def should_skip_file(tender_id, file_name, file_path):
        existing_log = ScraperBackend.get_downloaded_file_log(tender_id)
        file_name = str(file_name)
        if os.path.exists(file_path) and file_name not in existing_log:
            # Backfill legacy files into DB log on first encounter.
            ScraperBackend.log_downloaded_file(tender_id, file_name)
            existing_log.add(file_name)
        return os.path.exists(file_path) and file_name in existing_log

    @staticmethod
    def refresh_selenium_session(driver, init_url, tender_url):
        if not ensure_scraper_dependencies():
            return False
        try:
            driver.get(init_url)
            time.sleep(2)
            driver.get(tender_url)
            time.sleep(2)
            title = (driver.title or "").lower()
            return not ("stale session" in title or title.strip() == "error")
        except Exception:
            return False

    @staticmethod
    def open_tender_page_with_recovery(driver, init_url, tender_url):
        if not ensure_scraper_dependencies():
            return False
        driver.get(tender_url)
        time.sleep(2)
        title = (driver.title or "").lower()
        if "stale session" in title or title.strip() == "error":
            log_to_gui("Stale session detected. Reinitializing Selenium session...")
            if not ScraperBackend.refresh_selenium_session(driver, init_url, tender_url):
                return False
        return True

    @staticmethod
    def fetch_organisations_logic(website_id):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return False
        websites = ScraperBackend.get_websites()
        if website_id not in websites: return
        site_data = websites[website_id]
        url = site_data['url']
        
        log_to_gui(f"Fetching organizations from {url}...")
        try:
            response = ScraperBackend.safe_request(url)
            if not response: return False
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Logic from tender_scraper.py
            org_name_header = soup.find('td', string='Organisation Name')
            if not org_name_header:
                log_to_gui("Could not find Organisation Name header.")
                return

            table = org_name_header.find_parent('table')
            rows = table.find_all('tr')
            
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            
            count = 0
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 2 and cols[0].text.strip().isdigit():
                    org_name = cols[1].text.strip()
                    tender_count = cols[2].text.strip()
                    link = cols[2].find('a')['href'] if cols[2].find('a') else ""
                    full_link = urljoin(url, link)
                    
                    # Insert or Ignore (to preserve selection status if exists)
                    # We use INSERT OR IGNORE then UPDATE to update details but keep selection
                    c.execute("SELECT id FROM organizations WHERE website_id=? AND name=?", (website_id, org_name))
                    exists = c.fetchone()
                    
                    if exists:
                        c.execute("UPDATE organizations SET tender_count=?, tenders_url=? WHERE id=?", (tender_count, full_link, exists[0]))
                    else:
                        c.execute("INSERT INTO organizations (website_id, name, tender_count, tenders_url) VALUES (?, ?, ?, ?)", 
                                  (website_id, org_name, tender_count, full_link))
                    count += 1
            
            conn.commit()
            conn.close()
            log_to_gui(f"Updated {count} organizations for {site_data['name']}")
            return True
        except Exception as e:
            log_to_gui(f"Error fetching orgs: {e}")
            return False

    @staticmethod
    def get_detail_by_label(soup_obj, label_variations, allow_contains=True):
        def norm(s):
            return " ".join((s or "").replace(":", " ").split()).lower()

        targets = [norm(v) for v in label_variations]
        caption_cells = soup_obj.find_all('td', class_=lambda c: c and 'td_caption' in c)

        # Primary path: exact label match in caption td -> immediate field sibling td.
        for label_td in caption_cells:
            label_text = norm(label_td.get_text(" ", strip=True))
            if label_text in targets:
                value_td = label_td.find_next_sibling('td')
                if value_td:
                    val = value_td.get_text(" ", strip=True)
                    if val:
                        return val

        # Secondary path: exact label match in explicit <b>/<strong> label nodes.
        for tag in soup_obj.find_all(['b', 'strong']):
            text = norm(tag.get_text(" ", strip=True))
            if text in targets:
                label_td = tag.find_parent('td')
                if label_td:
                    value_td = label_td.find_next_sibling('td')
                    if value_td:
                        val = value_td.get_text(" ", strip=True)
                        if val:
                            return val

        if allow_contains:
            # Contains-based fallback for portals that append extra caption text.
            for label_td in caption_cells:
                label_text = norm(label_td.get_text(" ", strip=True))
                if any(t in label_text for t in targets):
                    value_td = label_td.find_next_sibling('td')
                    if value_td:
                        val = value_td.get_text(" ", strip=True)
                        if val:
                            return val
            # Generic td fallback as last resort.
            for td in soup_obj.find_all('td'):
                text = norm(td.get_text(" ", strip=True))
                if any(t in text for t in targets):
                    value_td = td.find_next_sibling('td')
                    if value_td:
                        val = value_td.get_text(" ", strip=True)
                        if val:
                            return val
        return None

    @staticmethod
    def normalize_tender_id(raw_text):
        if not raw_text:
            return None
        txt = " ".join(str(raw_text).split())

        # 1) Explicit bracket format: [ID]
        m = re.search(r'\[([A-Z0-9\-_/.]+)\]', txt, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

        # 2) Label format: Tender ID: <id> / Ref No: <id>
        label_patterns = [
            r'(?i)tender\s*id\s*[:\-]?\s*([A-Z0-9\-_/.]+)',
            r'(?i)ref\.?\s*no\.?\s*[:\-]?\s*([A-Z0-9\-_/.]+)',
        ]
        for pat in label_patterns:
            m = re.search(pat, txt)
            if m:
                return m.group(1).strip()

        # 3) Generic token extraction fallback
        tokens = re.findall(r'[A-Z0-9][A-Z0-9\-_/.]{3,}', txt.upper())
        banned = {"TENDER", "TENDERID", "TENDERNO", "REF", "NO", "NIT", "ID"}
        ranked = []
        for t in tokens:
            if t in banned:
                continue
            score = 0
            if any(ch.isdigit() for ch in t):
                score += 2
            if any(ch.isalpha() for ch in t):
                score += 2
            if "/" in t or "-" in t or "_" in t or "." in t:
                score += 1
            ranked.append((score, len(t), t))
        if ranked:
            ranked.sort(reverse=True)
            return ranked[0][2]
        return None

    @staticmethod
    def derive_tender_id(title_text, detail_soup, tender_url):
        # Priority 1: Tender ID from detail page label (same intent as tender_scraper.py)
        if detail_soup is not None:
            raw_detail_id = ScraperBackend.get_detail_by_label(detail_soup, ["Tender ID", "Tender Id"])
            t_id = ScraperBackend.normalize_tender_id(raw_detail_id)
            if t_id:
                return t_id

        # Priority 2: parse from listing title/ref text
        t_id = ScraperBackend.normalize_tender_id(title_text)
        if t_id:
            return t_id

        # Priority 3: try URL query keys commonly used by portals
        try:
            qs = parse_qs(urlparse(tender_url).query)
            for key in ("tenderId", "tenderid", "tid", "id", "refNo"):
                if key in qs and qs[key]:
                    t_id = ScraperBackend.normalize_tender_id(qs[key][0])
                    if t_id:
                        return t_id
        except Exception:
            pass

        # Final fallback: deterministic URL-based id to avoid collisions/overwrites.
        return f"URL_{hashlib.md5(str(tender_url).encode('utf-8')).hexdigest()[:12].upper()}"

    @staticmethod
    def derive_tender_title(listing_title_text, detail_soup):
        """
        Resolve Tender Title from detail page first (same intent as tender_scraper.py),
        then fallback to listing title.
        """
        def clean_title(raw):
            txt = " ".join(str(raw or "").split())
            if not txt:
                return "N/A"
            # Remove trailing ref/id bracket blocks: [...][...]
            prev = None
            while prev != txt:
                prev = txt
                txt = re.sub(r"\s*\[[^\]]{3,}\]\s*$", "", txt).strip()
            if txt.startswith("[") and txt.endswith("]") and len(txt) > 2:
                txt = txt[1:-1].strip()
            return txt or "N/A"

        if detail_soup is not None:
            try:
                # Prefer exact "Title" in Work Item Details.
                for td in detail_soup.find_all("td", class_=lambda c: c and "td_caption" in c):
                    label = " ".join(td.get_text(" ", strip=True).replace(":", " ").split()).lower()
                    if label == "title":
                        value_td = td.find_next_sibling("td", class_="td_field") or td.find_next_sibling("td")
                        if value_td:
                            v = value_td.get_text(" ", strip=True)
                            if v:
                                return clean_title(v)
            except Exception:
                pass
            try:
                # Fallback to broader labels seen on NIC pages.
                v = ScraperBackend.get_detail_by_label(
                    detail_soup,
                    ["Title and Ref.No./Tender ID", "Title and Ref.No.", "Title"]
                )
                if v:
                    return clean_title(v)
            except Exception:
                pass
            try:
                if detail_soup.title and detail_soup.title.string:
                    v = detail_soup.title.string.strip().replace("E-Procurement System :: ", "")
                    if v and len(v) < 220:
                        return clean_title(v)
            except Exception:
                pass
        return clean_title(listing_title_text)

    @staticmethod
    def normalize_tender_url(raw_url):
        if not raw_url:
            return ""
        try:
            parsed = urlparse(raw_url)
            path = parsed.path or ""
            # Remove path params like ;jsessionid=...
            if ";" in path:
                path = path.split(";", 1)[0]
            skip_keys = {
                "session", "sessionid", "jsessionid", "sid", "phpsessid", "_", "ts", "timestamp"
            }
            q_items = []
            for k, v in parse_qsl(parsed.query, keep_blank_values=True):
                if k.lower() in skip_keys:
                    continue
                q_items.append((k, v))
            q_items.sort(key=lambda x: (x[0].lower(), x[1]))
            clean_query = urlencode(q_items, doseq=True)
            return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", clean_query, ""))
        except Exception:
            return str(raw_url)

    @staticmethod
    def upsert_tender_row(conn, tender_row):
        (
            website_id, org_chain, tender_id, title, tender_value, emd, closing_date, opening_date,
            tender_url, location, tender_category, pre_bid_meeting_date, work_description
        ) = tender_row
        norm_url = ScraperBackend.normalize_tender_url(tender_url)
        c = conn.cursor()

        existing = c.execute(
            """SELECT id, is_downloaded, is_bookmarked, folder_path, last_downloaded_at
               FROM tenders
               WHERE website_id=? AND normalized_tender_url=?
               ORDER BY id DESC LIMIT 1""",
            (website_id, norm_url)
        ).fetchone()
        if not existing and tender_id:
            existing = c.execute(
                """SELECT id, is_downloaded, is_bookmarked, folder_path, last_downloaded_at
                   FROM tenders
                   WHERE website_id=? AND tender_id=?
                   ORDER BY id DESC LIMIT 1""",
                (website_id, tender_id)
            ).fetchone()
        if not existing:
            existing = c.execute(
                """SELECT id, is_downloaded, is_bookmarked, folder_path, last_downloaded_at
                   FROM tenders
                   WHERE website_id=? AND org_chain=? AND title=? AND closing_date=?
                   ORDER BY id DESC LIMIT 1""",
                (website_id, org_chain, title, closing_date)
            ).fetchone()

        if existing:
            row_id, is_downloaded, is_bookmarked, folder_path, last_downloaded_at = existing
            c.execute(
                """UPDATE tenders
                   SET org_chain=?, tender_id=?, title=?, tender_value=?, emd=?, closing_date=?, opening_date=?,
                       tender_url=?, location=?, tender_category=?, pre_bid_meeting_date=?, normalized_tender_url=?,
                       work_description=?, status=CASE WHEN COALESCE(status,'')='Archived' THEN '' ELSE COALESCE(status,'') END,
                       is_archived=0, is_downloaded=?, is_bookmarked=?, folder_path=?, last_downloaded_at=?
                   WHERE id=?""",
                (
                    org_chain, tender_id, title, tender_value, emd, closing_date, opening_date,
                    tender_url, location, tender_category, pre_bid_meeting_date, norm_url, work_description,
                    is_downloaded, is_bookmarked, folder_path, last_downloaded_at, row_id
                )
            )
            return "updated"

        try:
            c.execute(
                """INSERT INTO tenders
                   (website_id, org_chain, tender_id, title, tender_value, emd, closing_date, opening_date,
                    tender_url, location, tender_category, pre_bid_meeting_date, work_description, status, is_archived, normalized_tender_url)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', 0, ?)""",
                (
                    website_id, org_chain, tender_id, title, tender_value, emd, closing_date, opening_date,
                    tender_url, location, tender_category, pre_bid_meeting_date, work_description, norm_url
                )
            )
            return "inserted"
        except sqlite3.IntegrityError:
            # Unique tender_id collisions: update the row for same website+tender_id if present.
            row = c.execute(
                "SELECT id FROM tenders WHERE website_id=? AND tender_id=? ORDER BY id DESC LIMIT 1",
                (website_id, tender_id)
            ).fetchone()
            if row:
                c.execute(
                    """UPDATE tenders
                       SET org_chain=?, title=?, tender_value=?, emd=?, closing_date=?, opening_date=?,
                           tender_url=?, location=?, tender_category=?, pre_bid_meeting_date=?,
                           normalized_tender_url=?, work_description=?,
                           status=CASE WHEN COALESCE(status,'')='Archived' THEN '' ELSE COALESCE(status,'') END,
                           is_archived=0
                       WHERE id=?""",
                    (
                        org_chain, title, tender_value, emd, closing_date, opening_date,
                        tender_url, location, tender_category, pre_bid_meeting_date, norm_url, work_description, row[0]
                    )
                )
                return "updated"
            raise

    @staticmethod
    def dedupe_tenders_for_website(conn, website_id):
        c = conn.cursor()
        rows = c.execute(
            """SELECT id, tender_id, tender_url, normalized_tender_url, title, closing_date,
                      is_downloaded, is_bookmarked, folder_path, last_downloaded_at
               FROM tenders
               WHERE website_id=?
               ORDER BY id DESC""",
            (website_id,)
        ).fetchall()

        keep_for_key = {}
        to_delete = []

        def key_of(r):
            # Prefer normalized URL (most stable for same tender across runs)
            if r[3]:
                return ("url", r[3])
            if r[1]:
                return ("id", r[1])
            return ("meta", f"{r[4]}|{r[5]}")

        for r in rows:
            rid = r[0]
            key = key_of(r)
            if key not in keep_for_key:
                keep_for_key[key] = r
            else:
                keep = keep_for_key[key]
                # Preserve user state on kept row.
                if (keep[6] or 0) == 0 and (r[6] or 0) == 1:
                    c.execute("UPDATE tenders SET is_downloaded=1 WHERE id=?", (keep[0],))
                if (keep[7] or 0) == 0 and (r[7] or 0) == 1:
                    c.execute("UPDATE tenders SET is_bookmarked=1 WHERE id=?", (keep[0],))
                if (not keep[8]) and r[8]:
                    c.execute("UPDATE tenders SET folder_path=? WHERE id=?", (r[8], keep[0]))
                if (not keep[9]) and r[9]:
                    c.execute("UPDATE tenders SET last_downloaded_at=? WHERE id=?", (r[9], keep[0]))
                to_delete.append(rid)

        if to_delete:
            c.executemany("DELETE FROM tenders WHERE id=?", [(x,) for x in to_delete])
        return len(to_delete)

    @staticmethod
    def fetch_tenders_logic(website_id):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT name, tenders_url FROM organizations WHERE website_id=? AND is_selected=1", (website_id,))
        selected_orgs = c.fetchall()
        conn.close()

        if not selected_orgs:
            log_to_gui("No organizations selected. Please select organizations first.")
            return

        scraped_org_seen_ids = {}
        failed_orgs = set()

        for org_name, url in selected_orgs:
            log_to_gui(f"Scraping tenders for: {org_name}")
            current_url = url
            org_seen_ids = set()
            org_scrape_ok = False
            
            while current_url:
                try:
                    res = ScraperBackend.safe_request(current_url)
                    if not res: break
                    soup = BeautifulSoup(res.text, 'html.parser')
                    
                    # Enhanced table finding logic
                    table = soup.find('table', {'id': 'table'})
                    if not table:
                        table = soup.find('table', {'class': 'list_table'})
                    if not table:
                        header_tds = soup.find_all('td', string=lambda s: s and 'S.No' in s)
                        for td in header_tds:
                            potential_table = td.find_parent('table')
                            if potential_table and potential_table.find('td', string=lambda s: s and 'e-Published Date' in s):
                                table = potential_table
                                break
                    if not table: 
                        log_to_gui(f"No tender table found on {current_url}")
                        break
                    org_scrape_ok = True
                    
                    rows = table.find_all('tr')
                    tenders_to_save = []
                    
                    for tr in rows:
                        cols = tr.find_all('td')
                        if len(cols) > 4:
                            # Assuming standard NIC structure
                            # Col 4 is usually Title/Ref No
                            title_col = cols[4]
                            link_tag = title_col.find('a')
                            if link_tag:
                                full_link = urljoin(current_url, link_tag['href'])
                                listing_title_text = title_col.get_text(" ", strip=True)
                                closing_date = cols[2].text.strip()
                                opening_date = cols[3].text.strip()
                                t_id = None

                                # Fetch Details
                                emd = "N/A"
                                val = "N/A"
                                loc = "N/A"
                                cat = "N/A"
                                prebid = "N/A"
                                work_desc = "N/A"
                                d_soup = None
                                
                                try:
                                    d_res = ScraperBackend.safe_request(full_link)
                                    if d_res:
                                        d_soup = BeautifulSoup(d_res.text, 'html.parser')
                                        # Keep mappings aligned with tender_scraper.py label strategy.
                                        emd = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["EMD Amount In â‚¹", "EMD Amount (in Rs.)", "EMD Amount In", "EMD Amount", "EMD"]
                                        ) or "N/A"
                                        val = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["Tender Value In â‚¹", "Tender Value In Rs.", "Tender Value In", "Tender Value"]
                                        ) or "N/A"
                                        loc = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["Location", "Work Location", "Place of Work"]
                                        ) or "N/A"
                                        # Strict: Tender Category should come from Tender Category label only.
                                        cat = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["Tender Category"],
                                            allow_contains=False
                                        ) or "N/A"
                                        prebid = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["Pre Bid Meeting Date", "Pre-Bid Meeting Date"]
                                        ) or "N/A"
                                        work_desc = ScraperBackend.get_detail_by_label(
                                            d_soup,
                                            ["Work Description", "Description of Work", "Work Desc"]
                                        ) or "N/A"
                                except: pass

                                title_text = ScraperBackend.derive_tender_title(listing_title_text, d_soup)
                                t_id = ScraperBackend.derive_tender_id(listing_title_text, d_soup, full_link)
                                log_to_gui(f"  > {t_id}")
                                t_id_norm = str(t_id or "").strip()
                                if t_id_norm and t_id_norm.upper() != "N/A":
                                    org_seen_ids.add(t_id_norm)
                                
                                tenders_to_save.append((website_id, org_name, t_id, title_text, val, emd, closing_date, opening_date, full_link, loc, cat, prebid, work_desc))

                    # Save batch
                    if tenders_to_save:
                        conn = sqlite3.connect(DB_FILE)
                        inserted_count = 0
                        updated_count = 0
                        for t in tenders_to_save:
                            try:
                                result = ScraperBackend.upsert_tender_row(conn, t)
                                if result == "inserted":
                                    inserted_count += 1
                                else:
                                    updated_count += 1
                            except Exception as e:
                                log_to_gui(f"Upsert failed for tender '{t[2]}': {e}")
                        removed = ScraperBackend.dedupe_tenders_for_website(conn, website_id)
                        conn.commit()
                        conn.close()
                        log_to_gui(f"Saved page: inserted={inserted_count}, updated={updated_count}, deduped={removed}")
                    else:
                        log_to_gui("No tenders found in table rows.")
                    
                    # Pagination
                    next_link = soup.find('a', string=lambda t: t and 'Next' in t)
                    current_url = urljoin(current_url, next_link['href']) if next_link else None
                    time.sleep(1)
                    
                except Exception as e:
                    log_to_gui(f"Error scraping {org_name}: {e}")
                    break
            if org_scrape_ok:
                scraped_org_seen_ids[org_name] = org_seen_ids
            else:
                failed_orgs.add(org_name)
        try:
            conn = sqlite3.connect(DB_FILE)
            removed = ScraperBackend.dedupe_tenders_for_website(conn, website_id)
            archived_missing = 0
            for org_name, seen_ids in scraped_org_seen_ids.items():
                archived_missing += ScraperBackend.archive_missing_tenders_for_org(
                    conn, website_id, org_name, seen_ids
                )
            conn.commit()
            conn.close()
            if removed:
                log_to_gui(f"Post-scrape dedupe removed {removed} duplicate rows.")
            if archived_missing:
                log_to_gui(f"Auto-archived {archived_missing} stale tenders no longer present in latest scrape.")
            log_to_gui(
                f"Stale-archive check completed for selected orgs: total={len(selected_orgs)}, "
                f"processed={len(scraped_org_seen_ids)}, failed={len(failed_orgs)}."
            )
            if failed_orgs:
                log_to_gui(f"Skipped stale-archive for failed org scrape(s): {', '.join(sorted(failed_orgs))}")
        except Exception as e:
            log_to_gui(f"Post-scrape dedupe error: {e}")
        log_to_gui("Tender fetching complete.")
        return True

    @staticmethod
    def archive_missing_tenders_for_org(conn, website_id, org_name, seen_tender_ids):
        c = conn.cursor()
        clean_ids = sorted({str(x).strip() for x in (seen_tender_ids or set()) if str(x).strip()})
        sql = (
            "UPDATE tenders SET is_archived=1 "
            "WHERE website_id=? AND org_chain=? AND COALESCE(is_archived,0)=0"
        )
        params = [website_id, org_name]
        if clean_ids:
            placeholders = ",".join("?" for _ in clean_ids)
            sql += f" AND TRIM(COALESCE(tender_id,'')) NOT IN ({placeholders})"
            params.extend(clean_ids)
        c.execute(sql, tuple(params))
        return int(c.rowcount or 0)

    @staticmethod
    def parse_closing_datetime(value):
        txt = " ".join(str(value or "").replace(",", " ").split()).strip()
        if not txt or txt.upper() in {"N/A", "NA", "NONE", "-"}:
            return None
        formats = (
            "%d-%b-%Y %I:%M %p",
            "%d-%b-%Y %H:%M",
            "%d-%m-%Y %I:%M %p",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %I:%M %p",
            "%d/%m/%Y %H:%M",
            "%d-%b-%Y",
            "%d-%m-%Y",
            "%d/%m/%Y",
        )
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(txt, fmt)
                if "%H:%M" not in fmt and "%I:%M" not in fmt:
                    dt = dt.replace(hour=23, minute=59, second=59)
                return dt
            except Exception:
                continue
        m = re.match(r"^(\d{1,2}-[A-Za-z]{3}-\d{4})\s+(\d{1,2}:\d{2}\s*[APMapm]{2})$", txt)
        if m:
            try:
                return datetime.datetime.strptime(
                    f"{m.group(1)} {m.group(2).upper().replace(' ', '')}",
                    "%d-%b-%Y %I:%M%p"
                )
            except Exception:
                return None
        return None

    @staticmethod
    def download_file_with_requests(url, file_path, cookies, tender_id=None, file_type="document"):
        if not ensure_scraper_dependencies():
            return False
        try:
            s = requests.Session()
            for c in cookies:
                s.cookies.set(c['name'], c['value'], domain=c.get('domain'), path=c.get('path'))
            s.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWeb7Kit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36'
            })
            r = s.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            if tender_id:
                ScraperBackend.log_downloaded_file(
                    tender_id,
                    os.path.basename(file_path),
                    file_type=file_type,
                    source_url=url,
                    local_path=file_path
                )
            return True
        except Exception as e:
            log_to_gui(f"Download failed for {os.path.basename(file_path)}: {e}")
            return False

    @staticmethod
    def _extract_status_and_row(driver):
        if not ensure_scraper_dependencies():
            return None, None
        try:
            table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "tabList")))
        except Exception:
            return None, None
        headers = [c.text.strip() for c in table.find_elements(By.CSS_SELECTOR, "tr.list_header td")]
        if not headers:
            headers = [c.text.strip() for c in table.find_elements(By.XPATH, ".//tbody/tr[1]/td")]
        idx = headers.index("Tender Stage") if "Tender Stage" in headers else -1
        if idx < 0:
            return None, None
        rows = table.find_elements(By.XPATH, ".//tbody/tr[contains(@class, 'even') or contains(@class, 'odd')]")
        if not rows:
            return None, None
        first_row = rows[0]
        cells = first_row.find_elements(By.TAG_NAME, "td")
        if idx >= len(cells):
            return None, first_row
        return cells[idx].text.strip(), first_row

    @staticmethod
    def _download_result_docs_from_popup(driver, tender_id, base_folder):
        if not ensure_scraper_dependencies():
            return False
        wait = WebDriverWait(driver, 20)
        main_window = driver.current_window_handle
        downloaded_any = False
        result_folder = os.path.join(base_folder, "Financial Result")
        os.makedirs(result_folder, exist_ok=True)
        try:
            summary_link = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "summary details")))
            driver.execute_script("arguments[0].click();", summary_link)
            wait.until(EC.number_of_windows_to_be(2))
            popup = None
            for wh in driver.window_handles:
                if wh != main_window:
                    popup = wh
                    break
            if not popup:
                return False
            driver.switch_to.window(popup)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute("href")
                if not href:
                    continue
                low = href.lower()
                if ".pdf" not in low and ".xlsx" not in low and ".zip" not in low:
                    continue
                name = (link.text or "").strip()
                if not name:
                    name = os.path.basename(urlparse(href).path) or f"result_{int(time.time())}"
                safe_name = re.sub(r'[\\/*?:"<>|]', "", name)
                fpath = os.path.join(result_folder, safe_name)
                if ScraperBackend.should_skip_file(tender_id, safe_name, fpath):
                    continue
                if ScraperBackend.download_file_with_requests(href, fpath, driver.get_cookies(), tender_id=tender_id, file_type="result"):
                    downloaded_any = True
                    log_to_gui(f"    Downloaded Result File: {safe_name}")
            driver.close()
            driver.switch_to.window(main_window)
        except Exception as e:
            log_to_gui(f"    Result popup processing error: {e}")
            try:
                if driver.current_window_handle != main_window:
                    driver.close()
                driver.switch_to.window(main_window)
            except Exception:
                pass
        return downloaded_any

    @staticmethod
    def check_tender_status_logic(website_id, archived_only=False):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return 0
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        # Get status URL
        site_url = c.execute("SELECT status_url FROM websites WHERE id=?", (website_id,)).fetchone()
        if not site_url:
            conn.close()
            return 0
        status_url = site_url[0]

        if archived_only:
            tenders = c.execute(
                """SELECT id, tender_id
                   FROM tenders
                   WHERE website_id=?
                     AND COALESCE(is_archived,0)=1
                     AND COALESCE(is_downloaded,0)=1
                     AND TRIM(COALESCE(tender_id,''))<>''""",
                (website_id,)
            ).fetchall()
        else:
            # Keep status blank for unselected active tenders, and check only selected active tenders.
            c.execute(
                "UPDATE tenders SET status='' WHERE website_id=? AND COALESCE(is_archived,0)=0 AND COALESCE(is_downloaded,0)=0",
                (website_id,)
            )
            conn.commit()
            tenders = c.execute(
                """SELECT id, tender_id
                   FROM tenders
                   WHERE website_id=?
                     AND COALESCE(is_archived,0)=0
                     AND COALESCE(is_downloaded,0)=1
                     AND TRIM(COALESCE(tender_id,''))<>''""",
                (website_id,)
            ).fetchall()
        conn.close()

        if not tenders:
            if archived_only:
                log_to_gui("No selected archived tenders to check status.")
            else:
                log_to_gui("No selected tenders to check status.")
            return 0

        mode_txt = "selected archived" if archived_only else "selected"
        log_to_gui(f"Checking status for {len(tenders)} {mode_txt} tenders...")
        
        options = FirefoxOptions()
        # options.add_argument("--headless") 
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        # Solve once per Selenium session; retry only if portal asks again.
        ScraperBackend.captcha_solved_in_session = False

        updated_count = 0
        try:
            for db_id, tid in tenders:
                log_to_gui(f"Checking: {tid}")
                driver.get(status_url)
                time.sleep(2)
                
                try:
                    # Input Tender ID
                    tid_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "tenderId")))
                    tid_input.clear()
                    tid_input.send_keys(tid)
                    
                    # Solve Captcha
                    if ScraperBackend.handle_captcha_interaction(driver, f"Status {tid}", "Search"):
                        new_status, _ = ScraperBackend._extract_status_and_row(driver)
                        if new_status:
                            conn = sqlite3.connect(DB_FILE)
                            conn.execute("UPDATE tenders SET status=? WHERE id=?", (new_status, db_id))
                            conn.commit()
                            conn.close()
                            updated_count += 1
                            log_to_gui(f"Updated status for {tid}: {new_status}")
                        else:
                            log_to_gui(f"No Tender Stage found for {tid}")
                    else:
                        log_to_gui(f"CAPTCHA failed for {tid}. Skipping this tender.")
                except Exception as e:
                    log_to_gui(f"Error checking {tid}: {e}")
        finally:
            driver.quit()
        log_to_gui("Status check complete.")
        return updated_count

    @staticmethod
    def download_tender_results_logic(website_id):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        site_row = c.execute("SELECT status_url FROM websites WHERE id=?", (website_id,)).fetchone()
        if not site_row:
            conn.close()
            log_to_gui("Status URL not configured for this website.")
            return
        status_url = site_row[0]
        c.execute("SELECT id, tender_id, folder_path FROM tenders WHERE website_id=? AND is_downloaded=1", (website_id,))
        targets = c.fetchall()
        conn.close()
        if not targets:
            log_to_gui("No tenders marked for download/result check.")
            return

        log_to_gui(f"Starting result file checks for {len(targets)} tenders...")
        options = FirefoxOptions()
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        ScraperBackend.captcha_solved_in_session = False
        target_statuses = {"Financial Bid Opening", "Financial Evaluation", "AOC", "Concluded"}
        try:
            for db_id, tender_id, folder_path in targets:
                try:
                    log_to_gui(f"Checking result status: {tender_id}")
                    driver.get(status_url)
                    time.sleep(2)
                    inp = WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.ID, "tenderId")))
                    inp.clear()
                    inp.send_keys(tender_id)
                    if not ScraperBackend.handle_captcha_interaction(driver, f"Result Check {tender_id}", submit_id="Search"):
                        log_to_gui(f"  CAPTCHA failed for {tender_id}.")
                        continue
                    status, row = ScraperBackend._extract_status_and_row(driver)
                    if status:
                        conn = sqlite3.connect(DB_FILE)
                        conn.execute("UPDATE tenders SET status=? WHERE id=?", (status, db_id))
                        conn.commit()
                        conn.close()
                    if status not in target_statuses:
                        log_to_gui(f"  Status '{status}' not eligible for result docs.")
                        continue

                    view_links = row.find_elements(By.XPATH, ".//a[img[contains(@src, 'view.png')]]") if row else []
                    if not view_links and row:
                        view_links = row.find_elements(By.TAG_NAME, "a")
                    if not view_links:
                        log_to_gui("  Result view link not found.")
                        continue
                    driver.execute_script("arguments[0].click();", view_links[0])
                    time.sleep(2)

                    safe_id = re.sub(r'[\\/*?:"<>|]', "", str(tender_id))
                    existing_folder = (folder_path or "").strip()
                    if existing_folder and os.path.isdir(existing_folder):
                        tender_folder = existing_folder
                    else:
                        tender_folder = os.path.join(BASE_DOWNLOAD_DIRECTORY, safe_id)
                        os.makedirs(tender_folder, exist_ok=True)
                    conn = sqlite3.connect(DB_FILE)
                    conn.execute("UPDATE tenders SET folder_path=? WHERE id=?", (tender_folder, db_id))
                    conn.commit()
                    conn.close()
                    got = ScraperBackend._download_result_docs_from_popup(driver, tender_id, tender_folder)
                    if got:
                        conn = sqlite3.connect(DB_FILE)
                        conn.execute("UPDATE tenders SET folder_path=?, last_downloaded_at=CURRENT_TIMESTAMP WHERE id=?", (tender_folder, db_id))
                        conn.commit()
                        conn.close()
                except Exception as e:
                    log_to_gui(f"  Result download error for {tender_id}: {e}")
        finally:
            driver.quit()
        log_to_gui("Result file check complete.")

    @staticmethod
    def download_tenders_logic(website_id, target_db_ids=None, forced_mode=None):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return
        ScraperBackend.ensure_download_tables()
        ids = []
        if target_db_ids:
            try:
                ids = [int(x) for x in target_db_ids]
            except Exception:
                ids = []
        mode_override = str(forced_mode or "").strip().lower()
        if mode_override not in {"full", "update"}:
            mode_override = ""
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        where_sql = "website_id=? AND COALESCE(is_downloaded,0)=1 AND COALESCE(is_archived,0)=0"
        params = [website_id]
        if ids:
            placeholders = ",".join("?" for _ in ids)
            where_sql += f" AND id IN ({placeholders})"
            params.extend(ids)
        c.execute(
            f"SELECT id, tender_id, title, tender_url, last_downloaded_at, folder_path "
            f"FROM tenders WHERE {where_sql}",
            tuple(params)
        )
        to_download = c.fetchall()
        conn.close()
        
        if not to_download:
            log_to_gui("No tenders marked for download.")
            return
        
        # Get base URL for session refresh
        conn = sqlite3.connect(DB_FILE)
        site_url_row = conn.execute("SELECT url FROM websites WHERE id=?", (website_id,)).fetchone()
        conn.close()
        base_url = site_url_row[0] if site_url_row else "https://mahatenders.gov.in/nicgep/app?page=FrontEndTendersByOrganisation&service=page"

        log_to_gui(f"Starting download for {len(to_download)} tenders...")
        
        options = FirefoxOptions()
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        wait = WebDriverWait(driver, 20)
        ScraperBackend.captcha_solved_in_session = False
        # Establish Selenium session once (same pattern as tender_scraper.py).
        try:
            driver.get(base_url)
            time.sleep(4)
        except Exception as e:
            log_to_gui(f"Failed to initialize Selenium session: {e}")
            driver.quit()
            return
        
        for db_id, t_id, title, url, last_dl, existing_folder in to_download:
            download_mode = mode_override if mode_override else ('update' if last_dl else 'full')
            log_to_gui(f"Processing: {t_id} (Mode: {download_mode})...")
            
            safe_id = re.sub(r'[\\/*?:"<>|]',"", t_id)
            preferred_dir = os.path.join(BASE_DOWNLOAD_DIRECTORY, safe_id)
            existing = str(existing_folder or "").strip()
            if existing:
                try:
                    ex_abs = os.path.normcase(os.path.abspath(existing))
                    pref_abs = os.path.normcase(os.path.abspath(preferred_dir))
                    save_dir = existing if ex_abs == pref_abs else preferred_dir
                except Exception:
                    save_dir = preferred_dir
            else:
                save_dir = preferred_dir
            if not os.path.exists(save_dir): os.makedirs(save_dir)
            
            try:
                if not ScraperBackend.open_tender_page_with_recovery(driver, base_url, url):
                    log_to_gui(f"Could not recover session for {t_id}. Skipping.")
                    continue

                # --- 1. Tender Notice (Full Mode Only) ---
                if download_mode == 'full':
                    try:
                        notice_filename = f"Tendernotice_{safe_id}.pdf"
                        notice_path = os.path.join(save_dir, notice_filename)
                        if not ScraperBackend.should_skip_file(t_id, notice_filename, notice_path):
                            log_to_gui("  Checking Tender Notice...")
                            if not ScraperBackend.open_tender_page_with_recovery(driver, base_url, url):
                                log_to_gui("  Could not open tender page for Tender Notice.")
                            else:
                                downloaded_notice = False
                                # If captcha already solved in this session, final link is often directly available.
                                if ScraperBackend.captcha_solved_in_session:
                                    try:
                                        final_link = wait.until(EC.presence_of_element_located((By.ID, "DirectLink_0")))
                                        href = final_link.get_attribute('href')
                                        if href and ScraperBackend.download_file_with_requests(href, notice_path, driver.get_cookies(), t_id, file_type="notice"):
                                            log_to_gui("  Downloaded Tender Notice.")
                                            downloaded_notice = True
                                    except Exception:
                                        pass
                                if not downloaded_notice:
                                    trigger = None
                                    try:
                                        trigger = wait.until(EC.element_to_be_clickable((By.ID, "docDownload")))
                                    except Exception:
                                        try:
                                            trigger = wait.until(EC.element_to_be_clickable((By.ID, "DirectLink_8")))
                                        except Exception:
                                            pass
                                    if trigger:
                                        driver.execute_script("arguments[0].click();", trigger)
                                        if ScraperBackend.handle_captcha_interaction(driver, "Tender Notice"):
                                            try:
                                                final_link = wait.until(EC.presence_of_element_located((By.ID, "DirectLink_0")))
                                                href = final_link.get_attribute('href')
                                                if href and ScraperBackend.download_file_with_requests(href, notice_path, driver.get_cookies(), t_id, file_type="notice"):
                                                    log_to_gui("  Downloaded Tender Notice.")
                                                    downloaded_notice = True
                                                else:
                                                    log_to_gui("  Final Tender Notice link missing/invalid.")
                                            except Exception:
                                                log_to_gui("  Could not find final Notice link.")
                                if not downloaded_notice:
                                    log_to_gui("  Tender Notice not downloaded.")
                        else:
                            log_to_gui("  Skipping Tender Notice (already logged and file exists).")
                    except Exception as e:
                        log_to_gui(f"  Notice download error: {e}")

                # --- 2. Zip File (Full Mode Only) ---
                if download_mode == 'full':
                    try:
                        zip_filename = f"{safe_id}.zip"
                        zip_path = os.path.join(save_dir, zip_filename)
                        if not ScraperBackend.should_skip_file(t_id, zip_filename, zip_path):
                            log_to_gui("  Checking Zip File...")
                            if ScraperBackend.open_tender_page_with_recovery(driver, base_url, url):
                                zip_href = None
                                try:
                                    zip_elem = wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Download as zip file")))
                                    zip_href = zip_elem.get_attribute('href')
                                except Exception:
                                    try:
                                        zip_elem = wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, "Download as zip")))
                                        zip_href = zip_elem.get_attribute('href')
                                    except Exception:
                                        try:
                                            zip_elem = wait.until(EC.presence_of_element_located((By.ID, "DirectLink_7")))
                                            zip_href = zip_elem.get_attribute('href')
                                        except Exception:
                                            try:
                                                zip_elem = wait.until(EC.presence_of_element_located((By.ID, "DirectLink_8")))
                                                zip_href = zip_elem.get_attribute('href')
                                            except Exception:
                                                pass
                                if zip_href and ScraperBackend.download_file_with_requests(zip_href, zip_path, driver.get_cookies(), t_id, file_type="zip"):
                                    log_to_gui("  Downloaded Zip File.")
                                else:
                                    log_to_gui("  Zip link not found.")
                        else:
                            log_to_gui("  Skipping Zip file (already logged and file exists).")
                    except Exception as e:
                        log_to_gui(f"  Zip download error: {e}")

                # --- 3. Pre-Bid Meeting (Always Check) ---
                try:
                    prebid_filename = f"PreBid_Meeting_{safe_id}.pdf"
                    prebid_path = os.path.join(save_dir, prebid_filename)
                    if not ScraperBackend.should_skip_file(t_id, prebid_filename, prebid_path):
                        if ScraperBackend.open_tender_page_with_recovery(driver, base_url, url):
                            try:
                                pb_link = wait.until(EC.presence_of_element_located((By.ID, "DirectLink_2")))
                                href = pb_link.get_attribute('href')
                                if href and ScraperBackend.download_file_with_requests(href, prebid_path, driver.get_cookies(), t_id, file_type="prebid"):
                                    log_to_gui("  Downloaded Pre-Bid File.")
                                else:
                                    log_to_gui("  Pre-Bid file link not found.")
                            except Exception:
                                log_to_gui("  No Pre-Bid file found.")
                    else:
                        log_to_gui("  Skipping Pre-Bid file (already logged and file exists).")
                except Exception as e:
                    log_to_gui(f"  Pre-bid error: {e}")

                # --- 4. Corrigendums (Always Check) ---
                try:
                    log_to_gui("  Checking Corrigendums...")
                    if ScraperBackend.open_tender_page_with_recovery(driver, base_url, url):
                        main_window = driver.current_window_handle
                        corr_links = driver.find_elements(By.XPATH, "//a[contains(@title, 'View Corrigendum History')]")
                        if corr_links:
                            driver.execute_script("arguments[0].click();", corr_links[0])
                            wait.until(EC.number_of_windows_to_be(2))
                            for handle in driver.window_handles:
                                if handle != main_window:
                                    driver.switch_to.window(handle)
                                    break

                            docs = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a[contains(@id, 'DirectLink_')]")))
                            for doc in docs:
                                href = doc.get_attribute('href')
                                name = (doc.text or "").strip()
                                if href and name:
                                    safe_name = re.sub(r'[\\/*?:"<>|]', "", name)
                                    fpath = os.path.join(save_dir, safe_name)
                                    if ScraperBackend.should_skip_file(t_id, safe_name, fpath):
                                        continue
                                    if ScraperBackend.download_file_with_requests(href, fpath, driver.get_cookies(), t_id, file_type="corrigendum"):
                                        log_to_gui(f"    Downloaded Corrigendum: {safe_name}")
                            driver.close()
                            driver.switch_to.window(main_window)
                except Exception as e:
                    log_to_gui(f"  Corrigendum error: {e}")
                    try:
                        if 'main_window' in locals():
                            driver.switch_to.window(main_window)
                    except Exception:
                        pass

                # Update DB
                conn = sqlite3.connect(DB_FILE)
                conn.execute("UPDATE tenders SET folder_path=?, last_downloaded_at=CURRENT_TIMESTAMP WHERE id=?", (save_dir, db_id))
                conn.commit()
                conn.close()

            except Exception as e:
                log_to_gui(f"Error accessing {url}: {e}")
        
        driver.quit()
        log_to_gui("Download process finished.")

    @staticmethod
    def download_docs_for_tender_to_folder(source_tender_id, destination_folder):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return False
        tender_id = str(source_tender_id or "").strip()
        dest = str(destination_folder or "").strip()
        if not tender_id:
            log_to_gui("Download Docs: missing source tender id.")
            return False
        if not dest:
            log_to_gui("Download Docs: missing destination folder.")
            return False
        os.makedirs(dest, exist_ok=True)

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        row = c.execute(
            "SELECT id, website_id FROM tenders WHERE TRIM(COALESCE(tender_id,''))=? LIMIT 1",
            (tender_id,)
        ).fetchone()
        if not row:
            conn.close()
            log_to_gui(f"Download Docs: tender not found for id '{tender_id}'.")
            return False
        target_db_id, website_id = row
        backups = c.execute(
            "SELECT id, COALESCE(is_downloaded,0), COALESCE(folder_path,'') FROM tenders WHERE website_id=?",
            (website_id,)
        ).fetchall()
        conn.close()

        success = False
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("UPDATE tenders SET is_downloaded=0 WHERE website_id=?", (website_id,))
            c.execute(
                "UPDATE tenders SET is_downloaded=1, folder_path=? WHERE id=?",
                (dest, target_db_id)
            )
            conn.commit()
            conn.close()

            ScraperBackend.download_tenders_logic(website_id)
            success = True
        finally:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            for tid, was_selected, old_folder in backups:
                c.execute(
                    "UPDATE tenders SET is_downloaded=?, folder_path=? WHERE id=?",
                    (int(was_selected or 0), str(old_folder or "").strip(), tid)
                )
            conn.commit()
            conn.close()
        return success

    @staticmethod
    def download_updates_for_tender_to_folder(source_tender_id, destination_folder):
        if not ensure_scraper_dependencies():
            log_to_gui("Scraper dependencies are missing. Install requirements and rebuild.")
            return False
        tender_id = str(source_tender_id or "").strip()
        dest = str(destination_folder or "").strip()
        if not tender_id:
            log_to_gui("Update Docs: missing source tender id.")
            return False
        if not dest:
            log_to_gui("Update Docs: missing destination folder.")
            return False
        os.makedirs(dest, exist_ok=True)

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        row = c.execute(
            "SELECT id, website_id, COALESCE(is_downloaded,0), COALESCE(folder_path,'') "
            "FROM tenders WHERE TRIM(COALESCE(tender_id,''))=? LIMIT 1",
            (tender_id,)
        ).fetchone()
        if not row:
            conn.close()
            log_to_gui(f"Update Docs: tender not found for id '{tender_id}'.")
            return False
        target_db_id, website_id, old_selected, old_folder = row
        conn.close()

        success = False
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                "UPDATE tenders SET is_downloaded=1, folder_path=? WHERE id=?",
                (dest, target_db_id)
            )
            conn.commit()
            conn.close()

            ScraperBackend.download_tenders_logic(
                website_id,
                target_db_ids=[target_db_id],
                forced_mode="update",
            )
            success = True
        finally:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                "UPDATE tenders SET is_downloaded=?, folder_path=? WHERE id=?",
                (int(old_selected or 0), str(old_folder or "").strip(), target_db_id)
            )
            conn.commit()
            conn.close()
        return success

    @staticmethod
    def download_single_tender_logic(tender_db_id, mode):
        try:
            target_id = int(tender_db_id)
        except Exception:
            log_to_gui("Download: invalid tender selection.")
            return False
        mode_txt = str(mode or "").strip().lower()
        if mode_txt not in {"full", "update"}:
            mode_txt = ""
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute(
            "SELECT website_id, COALESCE(is_archived,0) FROM tenders WHERE id=?",
            (target_id,)
        ).fetchone()
        conn.close()
        if not row:
            log_to_gui("Download: selected tender not found.")
            return False
        website_id, is_archived = row
        if int(is_archived or 0) != 0:
            log_to_gui("Download: selected tender is archived; only active tenders are allowed.")
            return False
        ScraperBackend.download_tenders_logic(website_id, target_db_ids=[target_id], forced_mode=mode_txt)
        return True

    @staticmethod
    def archive_completed_tenders_logic(website_id):
        completed = ("AOC", "Concluded", "Cancelled", "Withdrawn", "Terminated")
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in completed)
        c.execute(
            f"UPDATE tenders SET is_archived=1 WHERE website_id=? AND status IN ({placeholders})",
            (website_id, *completed)
        )
        changed_by_status = int(c.rowcount or 0)
        now_dt = datetime.datetime.now()
        overdue_ids = []
        rows = c.execute(
            "SELECT id, closing_date FROM tenders WHERE website_id=? AND COALESCE(is_archived,0)=0",
            (website_id,)
        ).fetchall()
        for tid, closing_raw in rows:
            closing_dt = ScraperBackend.parse_closing_datetime(closing_raw)
            if closing_dt and closing_dt < now_dt:
                overdue_ids.append(tid)
        changed_by_due = 0
        if overdue_ids:
            c.executemany("UPDATE tenders SET is_archived=1 WHERE id=?", [(tid,) for tid in overdue_ids])
            changed_by_due = int(c.rowcount or 0)
        changed = changed_by_status + changed_by_due
        conn.commit()
        conn.close()
        log_to_gui(
            f"Archived {changed} tenders (status-based={changed_by_status}, overdue={changed_by_due})."
        )
        return changed

    @staticmethod
    def get_download_log_rows(website_id=None, limit=500):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        if website_id:
            c.execute(
                """SELECT d.tender_id, d.file_name, COALESCE(d.file_type,'document'), d.downloaded_at
                   FROM downloaded_files d
                   JOIN tenders t ON t.tender_id = d.tender_id
                   WHERE t.website_id=?
                   ORDER BY d.downloaded_at DESC LIMIT ?""",
                (website_id, limit)
            )
        else:
            c.execute(
                """SELECT tender_id, file_name, COALESCE(file_type,'document'), downloaded_at
                   FROM downloaded_files
                   ORDER BY downloaded_at DESC LIMIT ?""",
                (limit,)
            )
        rows = c.fetchall()
        conn.close()
        return rows

    @staticmethod
    def archive_tender_logic(tender_db_id):
        conn = sqlite3.connect(DB_FILE)
        conn.execute("UPDATE tenders SET is_archived=1 WHERE id=?", (tender_db_id,))
        conn.commit()
        conn.close()
        log_to_gui("Tender archived.")

    @staticmethod
    def get_setting(key, default=None):
        conn = sqlite3.connect(DB_FILE)
        row = conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row[0] if row else default

    @staticmethod
    def set_setting(key, value):
        conn = sqlite3.connect(DB_FILE)
        conn.execute(
            "INSERT INTO app_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value))
        )
        conn.commit()
        conn.close()

    @staticmethod
    def log_auto_archive_run(status, archived_count=0, archived_status_updated=0, websites_count=0, notes=""):
        conn = sqlite3.connect(DB_FILE)
        conn.execute(
            """INSERT INTO auto_archive_runs
               (run_at_utc, status, archived_count, archived_status_updated, websites_count, notes)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                datetime.datetime.now(datetime.UTC).isoformat(),
                str(status or ""),
                int(archived_count or 0),
                int(archived_status_updated or 0),
                int(websites_count or 0),
                str(notes or ""),
            )
        )
        conn.commit()
        conn.close()

# --- VIEW 3: ONLINE TENDERS (NEW INTEGRATION) ---

