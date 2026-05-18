#!/usr/bin/env python3
"""
Test SSH jump-host access and MySQL connectivity.

This script does not inspect business tables. It only:
  1. opens an SSH local tunnel;
  2. connects to MySQL through that tunnel;
  3. runs a tiny read-only health query.
"""

import argparse
import getpass
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

try:
    import pymysql
except ImportError:
    print("Missing dependency: pymysql. Install with: python3 -m pip install pymysql", file=sys.stderr)
    raise


def load_env_file(path):
    env_path = Path(path).expanduser()
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_config(args):
    mysql_password = os.environ.get("MARKET_MYSQL_PASSWORD")
    if args.mysql_password:
        mysql_password = args.mysql_password
    elif args.prompt_mysql_password:
        mysql_password = getpass.getpass("MySQL password: ")

    if not mysql_password:
        raise RuntimeError("Missing required MySQL password. Set MARKET_MYSQL_PASSWORD or use --prompt-mysql-password.")

    return {
        "ssh_host": require_env("MARKET_SSH_HOST"),
        "ssh_user": os.environ.get("MARKET_SSH_USER", "ubuntu"),
        "ssh_key_path": str(Path(require_env("MARKET_SSH_KEY_PATH")).expanduser()),
        "mysql_host": args.mysql_host or require_env("MARKET_MYSQL_HOST"),
        "mysql_port": int(args.mysql_port or os.environ.get("MARKET_MYSQL_PORT", "3306")),
        "mysql_user": args.mysql_user or require_env("MARKET_MYSQL_USER"),
        "mysql_password": mysql_password,
        "mysql_db": args.mysql_db if args.mysql_db is not None else os.environ.get("MARKET_MYSQL_DB", ""),
        "batch_mode": os.environ.get("MARKET_SSH_BATCH_MODE", "yes").lower() not in ("0", "false", "no"),
    }


def find_free_port():
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def wait_for_local_port(port, process, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read().decode("utf-8", "replace") if process.stderr else ""
            raise RuntimeError(f"SSH tunnel exited early:\n{stderr}")

        sock = socket.socket()
        sock.settimeout(0.3)
        try:
            sock.connect(("127.0.0.1", port))
            return
        except OSError:
            time.sleep(0.3)
        finally:
            sock.close()

    raise TimeoutError(f"SSH tunnel did not become ready on local port {port}")


def start_tunnel(config):
    local_port = find_free_port()
    forward = f"127.0.0.1:{local_port}:{config['mysql_host']}:{config['mysql_port']}"
    target = f"{config['ssh_user']}@{config['ssh_host']}"

    cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=30",
        "-i",
        config["ssh_key_path"],
        "-L",
        forward,
        "-N",
        target,
    ]
    if config["batch_mode"]:
        cmd[1:1] = ["-o", "BatchMode=yes"]

    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        wait_for_local_port(local_port, process)
    except Exception:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        raise
    return local_port, process


def test_mysql(config, local_port):
    conn = pymysql.connect(
        host="127.0.0.1",
        port=local_port,
        user=config["mysql_user"],
        password=config["mysql_password"],
        database=config["mysql_db"] or None,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=20,
        write_timeout=20,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    VERSION() AS mysql_version,
                    DATABASE() AS current_database,
                    CURRENT_USER() AS connected_user,
                    NOW() AS server_time
                """
            )
            return cur.fetchone()
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Test SSH tunnel and MySQL connection.")
    parser.add_argument("--env-file", default=".env.market", help="Path to env file. Default: .env.market")
    parser.add_argument("--mysql-host", help="Override MySQL host from env file.")
    parser.add_argument("--mysql-port", type=int, help="Override MySQL port from env file.")
    parser.add_argument("--mysql-user", help="Override MySQL user from env file.")
    parser.add_argument("--mysql-password", help="Override MySQL password from env file. Prefer --prompt-mysql-password.")
    parser.add_argument("--prompt-mysql-password", action="store_true", help="Prompt for the MySQL password without echoing it.")
    parser.add_argument("--mysql-db", help="Override MySQL database from env file. Use an empty string to connect without a default DB.")
    return parser.parse_args()


def main():
    args = parse_args()
    load_env_file(args.env_file)
    config = get_config(args)

    tunnel_process = None
    try:
        print("Opening SSH tunnel...")
        local_port, tunnel_process = start_tunnel(config)
        print(f"SSH tunnel OK: 127.0.0.1:{local_port} -> MySQL")

        print("Connecting to MySQL...")
        result = test_mysql(config, local_port)
        print("MySQL connection OK")
        print(f"mysql_version={result['mysql_version']}")
        print(f"current_database={result['current_database']}")
        print(f"connected_user={result['connected_user']}")
        print(f"server_time={result['server_time']}")
    finally:
        if tunnel_process:
            tunnel_process.terminate()
            try:
                tunnel_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel_process.kill()


if __name__ == "__main__":
    main()
