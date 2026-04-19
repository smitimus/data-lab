#!/usr/bin/env python3
"""
Bulk-adopt all compose stacks into Dockhand.
Run after first start: python3 adopt.py [--password PASSWORD]
Default password: admin
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error

DOCKHAND_URL = "http://localhost:3000"
SCAN_DIRS = ["/opt/stacks", "/opt/server", "/opt/dockge"]


def find_stacks():
    stacks = []
    for base in SCAN_DIRS:
        if not os.path.isdir(base):
            continue
        for entry in sorted(os.listdir(base)):
            compose = os.path.join(base, entry, "compose.yaml")
            if os.path.isfile(compose):
                stacks.append({"name": entry, "composePath": compose})
    return stacks


def request(method, path, data=None, headers=None):
    url = DOCKHAND_URL + path
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode(), dict(resp.headers)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--password", default="admin")
    parser.add_argument("--username", default="admin")
    args = parser.parse_args()

    # Login
    print(f"Logging in to {DOCKHAND_URL} as {args.username}...")
    try:
        body, headers = request(
            "POST", "/api/auth/login",
            {"username": args.username, "password": args.password},
        )
    except urllib.error.URLError as e:
        print(f"ERROR: Could not connect to Dockhand — {e}")
        sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"ERROR: Login failed ({e.code}) — check credentials")
        sys.exit(1)

    # Extract session cookie
    cookie = None
    for header, value in headers.items():
        if header.lower() == "set-cookie":
            cookie = value.split(";")[0]
            break
    if not cookie:
        print("ERROR: No session cookie returned — login may have failed")
        sys.exit(1)

    # Discover stacks
    stacks = find_stacks()
    print(f"Found {len(stacks)} stacks:")
    for s in stacks:
        print(f"  {s['name']:35s}  {s['composePath']}")

    # Adopt
    print("\nAdopting stacks...")
    payload = {"stacks": stacks, "environmentId": 1}
    try:
        body, _ = request(
            "POST", "/api/stacks/adopt", payload,
            headers={"Cookie": cookie},
        )
    except urllib.error.HTTPError as e:
        print(f"ERROR: Adopt request failed ({e.code}): {e.read().decode()}")
        sys.exit(1)

    result = json.loads(body)
    adopted = result.get("adopted", [])
    failed = result.get("failed", [])
    print(f"\nAdopted: {len(adopted)}  Failed: {len(failed)}")
    if failed:
        print("Failed stacks:")
        for f in failed:
            print(f"  {f}")
    else:
        print("All stacks adopted successfully.")


if __name__ == "__main__":
    main()
