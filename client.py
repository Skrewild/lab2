#!/usr/bin/env python3
"""Lab 2 Starter Client (standard library only)."""

from urllib import request, parse
import argparse
import json
import sys

def http_post_json(url: str, payload: dict, timeout_s: float = 2.0):
    """POST JSON and return (status_code, json_body)."""
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout_s) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))

def http_get_json(url: str, timeout_s: float = 2.0):
    """GET and return (status_code, json_body)."""
    with request.urlopen(url, timeout=timeout_s) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", required=True, help="Base URL, e.g. http://10.0.1.12:8000")
    ap.add_argument("cmd", choices=["put", "get", "status"])
    ap.add_argument("key", nargs="?")
    ap.add_argument("value", nargs="?")
    args = ap.parse_args()

    base = args.node.rstrip("/")

    if args.cmd == "put":
        if args.key is None or args.value is None:
            print("put requires key and value")
            sys.exit(2)
        status, obj = http_post_json(base + "/put", {"key": args.key, "value": args.value})
        print(status, json.dumps(obj, indent=2))
        return

    if args.cmd == "get":
        if args.key is None:
            print("get requires key")
            sys.exit(2)
        url = base + "/get?" + parse.urlencode({"key": args.key})
        status, obj = http_get_json(url)
        print(status, json.dumps(obj, indent=2))
        return

    if args.cmd == "status":
        status, obj = http_get_json(base + "/status")
        print(status, json.dumps(obj, indent=2))
        return

if __name__ == "__main__":
    main()
