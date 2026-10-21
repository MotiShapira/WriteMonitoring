#!/usr/bin/env python3
"""
MongoDB Ops Manager write statistics.

Stores an Ops Manager API key (mode 0600) and reports, for a time window and
per host / total: documents inserted / updated / deleted, write operations,
and bytes written.

Usage:
  write_stats.py configure                       # save base URL, group id, API keys
  write_stats.py report --hours 24
  write_stats.py report --from 2026-10-01 --to 2026-10-02T12:00 --json
  write_stats.py report --hours 6 --all-hosts --csv out.csv

Data source: Ops Manager Public API v1.0
  GET /groups/{GROUP}/hosts
  GET /groups/{GROUP}/hosts/{HOST}/measurements
Ops Manager returns per-second rates; each data point is multiplied by the
granularity (seconds) and summed, so totals are estimates, not exact counts.
"""
import argparse
import csv
import getpass
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from requests.auth import HTTPDigestAuth


def _home():
    """Invoking user's home, also under sudo (needed by `blocks scan`)."""
    user = os.environ.get("SUDO_USER")
    if user and os.geteuid() == 0:
        return os.path.expanduser("~" + user)
    return os.path.expanduser("~")


CONFIG_PATH = os.path.expanduser(os.environ.get(
    "OPSMGR_CONFIG", os.path.join(_home(), ".config/opsmanager-writestats/config.json")))

# measurement name -> output column
DOC_METRICS = {
    "DOCUMENT_METRICS_INSERTED": "docs_inserted",
    "DOCUMENT_METRICS_UPDATED": "docs_updated",
    "DOCUMENT_METRICS_DELETED": "docs_deleted",
}
OP_METRICS = {
    "OPCOUNTER_INSERT": "ops_insert",
    "OPCOUNTER_UPDATE": "ops_update",
    "OPCOUNTER_DELETE": "ops_delete",
}
BYTE_METRICS = {
    "NETWORK_BYTES_IN": "net_bytes_in",              # client+repl traffic received
    "CACHE_BYTES_WRITTEN_FROM": "cache_bytes_written",  # WiredTiger cache -> disk
}
ALL_METRICS = {**DOC_METRICS, **OP_METRICS, **BYTE_METRICS}
COLUMNS = list(ALL_METRICS.values())
# replicated ops applied: non-zero only while a host is a secondary. Fetched by
# `collect` to work out each sample's role, not stored.
REPL_METRICS = ["OPCOUNTER_REPL_INSERT", "OPCOUNTER_REPL_UPDATE", "OPCOUNTER_REPL_DELETE"]

# hosts that accept writes; secondaries only replay them
WRITER_TYPES = {"STANDALONE", "REPLICA_PRIMARY", "SHARD_PRIMARY"}
DATA_TYPES = WRITER_TYPES | {"REPLICA_SECONDARY", "SHARD_SECONDARY"}


# ---------------------------------------------------------------- config
def load_config():
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
    env = {
        "base_url": "OPSMGR_URL", "group_id": "OPSMGR_GROUP_ID",
        "public_key": "OPSMGR_PUBLIC_KEY", "private_key": "OPSMGR_PRIVATE_KEY",
    }
    for k, e in env.items():
        if os.environ.get(e):
            cfg[k] = os.environ[e]
    missing = [k for k in env if not cfg.get(k)]
    if missing:
        sys.exit("Missing config (%s). Run: %s configure" % (", ".join(missing), sys.argv[0]))
    return cfg


def cmd_configure(args):
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

    def ask(label, key, secret=False):
        cur = cfg.get(key, "")
        shown = "****" if (secret and cur) else cur
        prompt = "%s%s: " % (label, " [%s]" % shown if shown else "")
        val = getpass.getpass(prompt) if secret else input(prompt)
        return val.strip() or cur

    cfg["base_url"] = ask("Ops Manager URL (e.g. https://opsmgr:8080)", "base_url").rstrip("/")
    cfg["group_id"] = ask("Project (group) ID", "group_id")
    cfg["public_key"] = ask("API public key", "public_key")
    cfg["private_key"] = ask("API private key", "private_key", secret=True)
    cfg["verify_tls"] = ask("CA bundle path, or 'false' to skip verification (blank = system CAs)",
                            "verify_tls") or True
    if isinstance(cfg["verify_tls"], str) and cfg["verify_tls"].lower() == "false":
        cfg["verify_tls"] = False

    os.makedirs(os.path.dirname(CONFIG_PATH), mode=0o700, exist_ok=True)
    fd = os.open(CONFIG_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(cfg, f, indent=2)
    os.chmod(CONFIG_PATH, 0o600)
    print("Saved to %s (mode 600)" % CONFIG_PATH)


# ---------------------------------------------------------------- API
class OpsManager:
    def __init__(self, cfg):
        self.base = cfg["base_url"].rstrip("/") + "/api/public/v1.0"
        self.group = cfg["group_id"]
        self.s = requests.Session()
        self.s.auth = HTTPDigestAuth(cfg["public_key"], cfg["private_key"])
        self.s.verify = cfg.get("verify_tls", True)
        self.s.headers["Accept"] = "application/json"

    def get(self, url, params=None, allow_invalid_metric=False):
        if not url.startswith("http"):
            url = self.base + url
        r = self.s.get(url, params=params, timeout=60)
        if allow_invalid_metric and r.status_code == 404 and "INVALID_METRIC_NAME" in r.text:
            return None
        if r.status_code >= 400:
            sys.exit("API error %s for %s: %s" % (r.status_code, r.url, r.text[:300]))
        return r.json()

    def paged(self, url, params=None):
        """Yield every page of a response, following rel=next links."""
        data = self.get(url, params)
        while True:
            yield data
            nxt = next((l["href"] for l in data.get("links", []) if l.get("rel") == "next"), None)
            if not nxt:
                return
            data = self.get(nxt)

    def hosts(self):
        out = []
        for page in self.paged("/groups/%s/hosts" % self.group, {"itemsPerPage": 500}):
            out.extend(page.get("results", []))
        return out

    def measurements(self, host_id, start, end, granularity, extra=()):
        url = "/groups/%s/hosts/%s/measurements" % (self.group, host_id)
        params = [("granularity", granularity), ("start", iso(start)), ("end", iso(end)),
                  ("itemsPerPage", 500)] + [("m", m) for m in ALL_METRICS]
        # optional metrics: if the host doesn't report one, fetch without them
        if extra:
            first = self.get(url, params + [("m", m) for m in extra], allow_invalid_metric=True)
            if first is not None:
                params += [("m", m) for m in extra]
        merged = {}
        for page in self.paged(url, params):
            for m in page.get("measurements", []):
                e = merged.setdefault(m["name"], {"units": m.get("units", ""), "dataPoints": []})
                e["dataPoints"].extend(m.get("dataPoints", []))
        return merged


# ---------------------------------------------------------------- helpers
def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(s):
    s = s.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.astimezone()  # treat naive input as local time
    return dt


def granularity_for(window):
    """Pick the finest granularity Ops Manager is likely to still retain."""
    if window <= timedelta(hours=6):
        return "PT1M", 60
    if window <= timedelta(days=2):
        return "PT5M", 300
    return "PT1H", 3600


def granularity_seconds(g):
    return {"PT10S": 10, "PT1M": 60, "PT5M": 300, "PT1H": 3600, "P1D": 86400}.get(g, 60)


def points_for_host(series, step):
    """Per-second rates -> {timestamp: {column: count/bytes in that interval}}."""
    points = {}
    for name, col in ALL_METRICS.items():
        m = series.get(name)
        if not m:
            continue
        per_sec = "PER_SECOND" in m["units"]
        for dp in m["dataPoints"]:
            v = dp.get("value")
            if v is None:
                continue
            ts = iso(parse_time(dp["timestamp"]))
            points.setdefault(ts, {})[col] = v * step if per_sec else v
    return points


def sample_roles(series, current_type):
    """Role per timestamp, inferred from the metrics themselves.

    A host applying replicated ops was a secondary; one taking client writes with no
    replicated ops was the primary. Idle intervals are left out (role unknown).
    """
    if current_type not in DATA_TYPES or current_type == "STANDALONE":
        return {}
    prefix = "SHARD_" if current_type.startswith("SHARD_") else "REPLICA_"
    repl, writes = {}, {}
    for names, acc in ((REPL_METRICS, repl), (OP_METRICS, writes)):
        for name in names:
            for dp in series.get(name, {}).get("dataPoints", []):
                if dp.get("value") is not None:
                    ts = iso(parse_time(dp["timestamp"]))
                    acc[ts] = acc.get(ts, 0) + dp["value"]
    roles = {}
    for ts in set(repl) | set(writes):
        if repl.get(ts, 0) > 0:
            roles[ts] = prefix + "SECONDARY"
        elif writes.get(ts, 0) > 0:
            roles[ts] = prefix + "PRIMARY"
    return roles


def totals_for_host(series, step):
    """Sum per-interval values over the window -> total counts / bytes."""
    row = {c: 0.0 for c in COLUMNS}
    for p in points_for_host(series, step).values():
        for c, v in p.items():
            row[c] += v
    return {k: int(round(v)) for k, v in row.items()}


def human(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return "%.2f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024.0


# ---------------------------------------------------------------- report
def cmd_report(args):
    cfg = load_config()
    end = parse_time(args.to) if args.to else datetime.now(timezone.utc)
    if args.start:
        start = parse_time(args.start)
    else:
        start = end - timedelta(hours=args.hours)
    if start >= end:
        sys.exit("start must be before end")

    gran, step = granularity_for(end - start)
    if args.granularity:
        gran, step = args.granularity, granularity_seconds(args.granularity)

    om = OpsManager(cfg)
    allowed = DATA_TYPES if args.all_hosts else WRITER_TYPES
    hosts = [h for h in om.hosts() if h.get("typeName") in allowed]
    if args.cluster:
        hosts = [h for h in hosts if args.cluster in (h.get("replicaSetName"), h.get("clusterId"))]
    if not hosts:
        sys.exit("No matching hosts found (types: %s)" % ", ".join(sorted(allowed)))

    rows = []
    for h in hosts:
        series = om.measurements(h["id"], start, end, gran)
        row = {"host": "%s:%s" % (h["hostname"], h["port"]),
               "replica_set": h.get("replicaSetName") or "",
               "type": h.get("typeName", "")}
        row.update(totals_for_host(series, step))
        rows.append(row)

    total = {c: sum(r[c] for r in rows) for c in COLUMNS}
    result = {"start": iso(start), "end": iso(end), "granularity": gran,
              "hosts": rows, "total": total}

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["host", "replica_set", "type"] + COLUMNS)
            w.writeheader()
            w.writerows(rows)
        print("Wrote %s" % args.csv)
    if args.json:
        print(json.dumps(result, indent=2))
        return
    print_table(result)


# ---------------------------------------------------------------- history (SQLite)
DB_PATH = os.path.expanduser(
    os.environ.get("OPSMGR_DB", os.path.join(os.path.dirname(CONFIG_PATH), "history.db")))


def open_db(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    db = sqlite3.connect(path)
    db.execute("""CREATE TABLE IF NOT EXISTS samples (
        host_id TEXT NOT NULL, ts TEXT NOT NULL, step INTEGER NOT NULL,
        host TEXT, replica_set TEXT, type TEXT,
        %s,
        cluster_id TEXT,
        PRIMARY KEY (host_id, ts))""" % ", ".join("%s REAL NOT NULL DEFAULT 0" % c for c in COLUMNS))
    # databases created before cluster_id existed
    if "cluster_id" not in {r[1] for r in db.execute("PRAGMA table_info(samples)")}:
        db.execute("ALTER TABLE samples ADD COLUMN cluster_id TEXT")
    db.execute("""CREATE TABLE IF NOT EXISTS block_scans (
        id INTEGER PRIMARY KEY, ts TEXT NOT NULL, path TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS block_files (
        scan_id INTEGER NOT NULL, file TEXT NOT NULL, size INTEGER NOT NULL,
        db_name TEXT, coll TEXT, kind TEXT, hashes BLOB NOT NULL,
        PRIMARY KEY (scan_id, file))""")
    return db


SAMPLE_COLS = ["host_id", "ts", "step", "host", "replica_set", "cluster_id", "type"] + COLUMNS


def cmd_collect(args):
    """Fetch new data points since the last run and store them. Safe to run from cron."""
    cfg = load_config()
    om = OpsManager(cfg)
    db = open_db(args.db)
    step = granularity_seconds(args.granularity)
    # the newest data point may still be filling in, so re-fetch a small overlap
    # and let INSERT OR REPLACE overwrite it
    overlap = timedelta(seconds=3 * step)
    end = datetime.now(timezone.utc)
    stored = 0
    # always collect secondaries too: the writer role is stored per sample, so failovers
    # inside a window are attributed correctly at report time
    insert = "INSERT OR REPLACE INTO samples (%s) VALUES (%s)" % (
        ",".join(SAMPLE_COLS), ",".join("?" * len(SAMPLE_COLS)))
    for h in om.hosts():
        cur_type = h.get("typeName", "")
        if cur_type not in DATA_TYPES:
            continue
        row = db.execute("SELECT MAX(ts) FROM samples WHERE host_id=?", (h["id"],)).fetchone()
        first_run = not row[0]
        if first_run:
            start = end - timedelta(hours=args.backfill_hours)
        else:
            start = parse_time(row[0]) - overlap
        # a standalone is always the writer, so it needs no role detection
        extra = REPL_METRICS if cur_type != "STANDALONE" else ()
        series = om.measurements(h["id"], start, end, args.granularity, extra=extra)
        points = points_for_host(series, step)
        # role per sample: inferred from repl/write activity; for idle intervals keep
        # the role already stored, else fall back to the host's current role
        roles = sample_roles(series, cur_type)
        known = dict(db.execute("SELECT ts, type FROM samples WHERE host_id=? AND ts>=?",
                                (h["id"], iso(start))))
        host = "%s:%s" % (h["hostname"], h["port"])
        rows = []
        for ts, vals in points.items():
            role = roles.get(ts) or known.get(ts) or cur_type
            rows.append((h["id"], ts, step, host, h.get("replicaSetName") or "",
                         h.get("clusterId") or "", role) +
                        tuple(vals.get(c, 0.0) for c in COLUMNS))
        db.executemany(insert, rows)
        stored += len(rows)
        if first_run and points:
            earliest = parse_time(min(points))
            if earliest - start > timedelta(seconds=2 * step):
                print("warning: %s: Ops Manager only had %s data from %s (asked for %s); "
                      "earlier data has expired" % (host, args.granularity, iso(earliest), iso(start)),
                      file=sys.stderr)
    db.commit()
    if args.prune_days:
        cutoff = iso(end - timedelta(days=args.prune_days))
        db.execute("DELETE FROM samples WHERE ts < ?", (cutoff,))
        db.commit()
    print("%s collected %d samples into %s" % (iso(end), stored, args.db))


def cmd_history(args):
    """Report from the local SQLite history (no Ops Manager access needed)."""
    if not os.path.exists(args.db):
        sys.exit("No history database at %s. Run 'collect' first." % args.db)
    end = parse_time(args.to) if args.to else datetime.now(timezone.utc)
    start = parse_time(args.start) if args.start else end - timedelta(hours=args.hours)
    db = open_db(args.db)
    where = "ts > ? AND ts <= ?"
    params = [iso(start), iso(end)]
    if not args.all_hosts:
        where += " AND type IN (%s)" % ",".join("?" * len(WRITER_TYPES))
        params += sorted(WRITER_TYPES)
    if args.cluster:
        where += " AND (replica_set = ? OR cluster_id = ?)"
        params += [args.cluster, args.cluster]
    sums = ", ".join("SUM(%s)" % c for c in COLUMNS)
    q = ("SELECT host, replica_set, GROUP_CONCAT(DISTINCT type), %s FROM samples WHERE %s "
         "GROUP BY host_id ORDER BY host" % (sums, where))
    rows = []
    for r in db.execute(q, params):
        row = {"host": r[0], "replica_set": r[1], "type": "/".join(sorted(set(r[2].split(","))))}
        row.update({c: int(round(v or 0)) for c, v in zip(COLUMNS, r[3:])})
        rows.append(row)
    if not rows:
        sys.exit("No samples in that window.")
    total = {c: sum(r[c] for r in rows) for c in COLUMNS}
    result = {"start": iso(start), "end": iso(end), "granularity": "stored", "hosts": rows, "total": total}
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["host", "replica_set", "type"] + COLUMNS)
            w.writeheader()
            w.writerows(rows)
        print("Wrote %s" % args.csv)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_table(result)


# ---------------------------------------------------------------- changed blocks
# Each scan stores a short hash of every 1 MiB block of every file under the dbPath.
# A larger block counts as changed if any 1 MiB block inside it changed, so one scan
# answers every block size. Blocks are file-relative, not disk-relative.
BLOCK = 1 << 20
BLOCK_SIZES = {"1M": 1, "4M": 4, "8M": 8, "16M": 16}   # in BLOCK units
HASH_LEN = 8
MONGO_URI = os.environ.get("OPSMGR_MONGO_URI", "mongodb://127.0.0.1:27017/?directConnection=true")

# WiredTiger table -> (database, collection, kind) for every collection and index
OWNERS_JS = r"""
const out = {};
for (const d of db.adminCommand({listDatabases: 1}).databases) {
  const sdb = db.getSiblingDB(d.name);
  for (const c of sdb.getCollectionInfos({type: "collection"}, {nameOnly: true})) {
    try {
      const s = sdb.getCollection(c.name).aggregate([{$collStats: {storageStats: {}}}]).toArray()[0].storageStats;
      out[s.wiredTiger.uri] = [d.name, c.name, "collection"];
      for (const [ix, v] of Object.entries(s.indexDetails || {})) out[v.uri] = [d.name, c.name, "index " + ix];
    } catch (e) {}
  }
}
print(JSON.stringify(out));
"""


def file_owners(uri):
    """{'collection-....wt': (db, collection, kind)} via mongosh; {} if unavailable."""
    try:
        r = subprocess.run(["mongosh", "--quiet", uri, "--eval", OWNERS_JS],
                           capture_output=True, text=True, timeout=300)
        raw = json.loads(r.stdout.strip().splitlines()[-1])
    except (OSError, ValueError, IndexError, subprocess.TimeoutExpired) as e:
        print("warning: could not map files to collections (%s); they are reported as "
              "(unmapped)" % e, file=sys.stderr)
        return {}
    return {u.split("table:", 1)[1] + ".wt": tuple(v) for u, v in raw.items() if "table:" in u}


def classify(rel, owners):
    if rel in owners:
        return owners[rel]
    top = rel.split("/", 1)[0]
    if top == "journal":
        return ("(journal)", "", "")
    if top == "diagnostic.data":
        return ("(diagnostic.data)", "", "")
    if rel.startswith(("collection-", "index-")):
        return ("(unmapped)", "", "")
    if rel.endswith(".wt") or rel.startswith("WiredTiger") or top == "_tmp" or \
            rel in ("storage.bson", "mongod.lock"):
        return ("(system)", "", "")
    return ("(other)", "", "")   # logs, config files kept in the dbPath


def hash_file(path):
    out = bytearray()
    with open(path, "rb") as f:
        while True:
            b = f.read(BLOCK)
            if not b:
                return bytes(out)
            out += hashlib.blake2b(b, digest_size=HASH_LEN).digest()


def load_scan(db, scan_id):
    return {f: (size, h, dbn, coll, kind) for f, size, h, dbn, coll, kind in db.execute(
        "SELECT file, size, hashes, db_name, coll, kind FROM block_files WHERE scan_id=?", (scan_id,))}


def compare_scans(base, cur, sizes, by):
    """Changed blocks of `cur` relative to `base`, grouped by database (or collection)."""
    groups = {}
    for f, (size, h, dbn, coll, kind) in cur.items():
        key = dbn if by == "database" or dbn.startswith("(") else "%s.%s" % (dbn, coll)
        g = groups.setdefault(key, {"group": key, "files": 0, "bytes": 0,
                                    "blocks": {s: [0, 0] for s in sizes}})
        old = base.get(f, (0, b""))[1]
        n = len(h) // HASH_LEN
        # new files and blocks past the old end of a file count as changed
        changed = [i for i in range(n)
                   if h[i * HASH_LEN:(i + 1) * HASH_LEN] != old[i * HASH_LEN:(i + 1) * HASH_LEN]]
        g["files"] += 1
        g["bytes"] += size
        for s in sizes:
            k = BLOCK_SIZES[s]
            g["blocks"][s][0] += len({i // k for i in changed})
            g["blocks"][s][1] += -(-n // k)
    rows = sorted(groups.values(), key=lambda g: (g["group"].startswith("("), g["group"]))

    def subtotal(name, part):
        return {"group": name, "files": sum(r["files"] for r in part),
                "bytes": sum(r["bytes"] for r in part),
                "blocks": {s: [sum(r["blocks"][s][i] for r in part) for i in (0, 1)] for s in sizes}}
    totals = [subtotal("databases", [r for r in rows if not r["group"].startswith("(")]),
              subtotal("TOTAL", rows)]
    removed = [f for f in base if f not in cur]
    for r in rows + totals:
        r["pct"] = {s: round(100.0 * c / t, 2) if t else 0.0 for s, (c, t) in r["blocks"].items()}
    return {"rows": rows, "totals": totals, "removed_files": len(removed),
            "removed_bytes": sum(base[f][0] for f in removed)}


def print_blocks(res, sizes):
    print("Changed blocks: scan #%d (%s) -> scan #%d (%s)" % (
        res["base"]["id"], res["base"]["ts"], res["target"]["id"], res["target"]["ts"]))
    hdr = ["group", "files", "size"] + ["%s blocks" % s for s in sizes]
    lines = [hdr]
    for r in res["rows"] + res["totals"]:
        lines.append([r["group"], str(r["files"]), human(r["bytes"])] +
                     ["%.1f%% (%d/%d)" % (r["pct"][s], r["blocks"][s][0], r["blocks"][s][1])
                      for s in sizes])
    widths = [max(len(l[i]) for l in lines) for i in range(len(hdr))]
    for i, l in enumerate(lines):
        print("  ".join(c.ljust(widths[j]) if j == 0 else c.rjust(widths[j]) for j, c in enumerate(l)))
        if i == 0 or i == len(lines) - 3:
            print("  ".join("-" * w for w in widths))
    if res["removed_files"]:
        print("\n%d file(s) removed since the baseline (%s); not counted." % (
            res["removed_files"], human(res["removed_bytes"])))


def parse_sizes(s):
    sizes = [x.strip().upper() for x in s.split(",") if x.strip()]
    bad = [x for x in sizes if x not in BLOCK_SIZES]
    if bad or not sizes:
        sys.exit("--sizes must be a comma list of %s" % ",".join(BLOCK_SIZES))
    return sorted(set(sizes), key=lambda x: BLOCK_SIZES[x])


def cmd_blocks_scan(args):
    """Hash every 1 MiB block under the dbPath and store the hashes."""
    if not os.path.isdir(args.path):
        sys.exit("No such directory: %s" % args.path)
    owners = {} if args.no_map else file_owners(args.mongo_uri)
    t0 = time.time()
    files = []
    for root, dirs, names in os.walk(args.path):
        for name in names:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, args.path)
            try:
                if not os.path.isfile(full) or os.path.islink(full):
                    continue
                size = os.path.getsize(full)
                h = hash_file(full)
            except FileNotFoundError:   # dropped / temp file vanished mid-scan
                continue
            except PermissionError:
                sys.exit("Permission denied reading %s. Run as root: sudo %s blocks scan"
                         % (full, sys.argv[0]))
            files.append((rel, size, h) + classify(rel, owners))
    db = open_db(args.db)
    ts = iso(datetime.now(timezone.utc))
    prev = db.execute("SELECT id, ts FROM block_scans ORDER BY id DESC LIMIT 1").fetchone()
    scan_id = db.execute("INSERT INTO block_scans (ts, path) VALUES (?, ?)", (ts, args.path)).lastrowid
    db.executemany("INSERT INTO block_files (scan_id, file, size, hashes, db_name, coll, kind) "
                   "VALUES (?,?,?,?,?,?,?)", [(scan_id, f, s, h, d, c, k) for f, s, h, d, c, k in files])
    if args.keep_days:
        cutoff = iso(datetime.now(timezone.utc) - timedelta(days=args.keep_days))
        old = "SELECT id FROM block_scans WHERE ts < ? AND id <> ?"
        db.execute("DELETE FROM block_files WHERE scan_id IN (%s)" % old, (cutoff, scan_id))
        db.execute("DELETE FROM block_scans WHERE id IN (%s)" % old, (cutoff, scan_id))
    db.commit()
    db.close()
    # keep the history file owned by the invoking user when run via sudo
    if os.geteuid() == 0 and os.environ.get("SUDO_UID"):
        for p in (args.db, os.path.dirname(args.db)):
            if os.stat(p).st_uid == 0:
                os.chown(p, int(os.environ["SUDO_UID"]), int(os.environ["SUDO_GID"]))
    print("%s scan #%d: %d files, %s in %.1fs" % (
        ts, scan_id, len(files), human(sum(f[1] for f in files)), time.time() - t0))
    if prev and not args.quiet:
        db = open_db(args.db)
        sizes = list(BLOCK_SIZES)
        res = compare_scans(load_scan(db, prev[0]), load_scan(db, scan_id), sizes, "database")
        res.update(base={"id": prev[0], "ts": prev[1]}, target={"id": scan_id, "ts": ts})
        print()
        print_blocks(res, sizes)


def cmd_blocks_report(args):
    """Changed-block percentages between two stored scans."""
    if not os.path.exists(args.db):
        sys.exit("No history database at %s. Run 'blocks scan' first." % args.db)
    db = open_db(args.db)
    sizes = parse_sizes(args.sizes)
    if args.base or args.target:
        def by_id(i):
            r = db.execute("SELECT id, ts FROM block_scans WHERE id=?", (i,)).fetchone()
            return r or sys.exit("No scan #%s (see: blocks list)" % i)
        last = db.execute("SELECT id FROM block_scans ORDER BY id DESC LIMIT 1").fetchone()
        target = by_id(args.target or (last and last[0]))
        base = by_id(args.base) if args.base else db.execute(
            "SELECT id, ts FROM block_scans WHERE id < ? ORDER BY id DESC LIMIT 1", (target[0],)).fetchone()
    else:
        end = parse_time(args.to) if args.to else datetime.now(timezone.utc)
        start = parse_time(args.start) if args.start else end - timedelta(hours=args.hours)
        target = db.execute("SELECT id, ts FROM block_scans WHERE ts <= ? ORDER BY ts DESC, id DESC LIMIT 1",
                            (iso(end),)).fetchone()
        # baseline: last scan at/before the window start, else the first one inside it
        base = db.execute("SELECT id, ts FROM block_scans WHERE ts <= ? ORDER BY ts DESC, id DESC LIMIT 1",
                          (iso(start),)).fetchone() or \
            db.execute("SELECT id, ts FROM block_scans WHERE ts > ? ORDER BY ts, id LIMIT 1",
                       (iso(start),)).fetchone()
    if not base or not target or base[0] == target[0]:
        sys.exit("Need two different scans for that window (see: blocks list).")
    res = compare_scans(load_scan(db, base[0]), load_scan(db, target[0]), sizes, args.by)
    res.update(base={"id": base[0], "ts": base[1]}, target={"id": target[0], "ts": target[1]})
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["group", "files", "bytes"] + [
                "%s_%s" % (s, c) for s in sizes for c in ("changed", "total", "pct")])
            for r in res["rows"] + res["totals"]:
                w.writerow([r["group"], r["files"], r["bytes"]] + [
                    v for s in sizes for v in (r["blocks"][s][0], r["blocks"][s][1], r["pct"][s])])
        print("Wrote %s" % args.csv)
    if args.json:
        print(json.dumps(res, indent=2))
    else:
        print_blocks(res, sizes)


def cmd_blocks_list(args):
    if not os.path.exists(args.db):
        sys.exit("No history database at %s." % args.db)
    db = open_db(args.db)
    q = ("SELECT s.id, s.ts, s.path, COUNT(f.file), COALESCE(SUM(f.size), 0) FROM block_scans s "
         "LEFT JOIN block_files f ON f.scan_id = s.id GROUP BY s.id ORDER BY s.id")
    for sid, ts, path, n, size in db.execute(q):
        print("#%-5d %s  %s  %d files  %s" % (sid, ts, path, n, human(size)))


# ---------------------------------------------------------------- clean
def cmd_clean(args):
    """Delete stored samples and/or block scans, then shrink the database file."""
    if not os.path.exists(args.db):
        sys.exit("No history database at %s." % args.db)
    if not (args.samples or args.blocks or args.all):
        sys.exit("Choose what to delete: --samples, --blocks or --all")
    if args.older_than_days is not None and args.before:
        sys.exit("Use either --older-than-days or --before, not both")
    cutoff = None
    if args.older_than_days is not None:
        cutoff = iso(datetime.now(timezone.utc) - timedelta(days=args.older_than_days))
    elif args.before:
        cutoff = iso(parse_time(args.before))
    where, params = ("ts < ?", (cutoff,)) if cutoff else ("1=1", ())

    db = open_db(args.db)
    plan = []
    if args.samples or args.all:
        n, lo, hi = db.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM samples WHERE " + where,
                               params).fetchone()
        plan.append(("samples", n, "%d samples%s" % (n, " (%s .. %s)" % (lo, hi) if n else "")))
    if args.blocks or args.all:
        n, lo, hi = db.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM block_scans WHERE " + where,
                               params).fetchone()
        plan.append(("blocks", n, "%d block scans%s" % (n, " (#%s .. #%s)" % (lo, hi) if n else "")))

    print("%s: delete %s%s" % (args.db, "; ".join(p[2] for p in plan),
                                " older than %s" % cutoff if cutoff else " (everything)"))
    if not any(p[1] for p in plan):
        print("Nothing to delete.")
        return
    if args.dry_run:
        print("Dry run: nothing deleted.")
        return
    if not args.yes:
        if not sys.stdin.isatty():
            sys.exit("Refusing to delete without confirmation; add --yes")
        if input("Delete? [y/N] ").strip().lower() not in ("y", "yes"):
            sys.exit("Cancelled.")

    before = os.path.getsize(args.db)
    for kind, n, _ in plan:
        if not n:
            continue
        if kind == "samples":
            db.execute("DELETE FROM samples WHERE " + where, params)
        else:
            ids = "SELECT id FROM block_scans WHERE " + where
            db.execute("DELETE FROM block_files WHERE scan_id IN (%s)" % ids, params)
            db.execute("DELETE FROM block_scans WHERE " + where, params)
    db.commit()
    db.execute("VACUUM")
    db.close()
    print("Deleted. Database size %s -> %s" % (human(before), human(os.path.getsize(args.db))))


def print_table(res):
    print("Window: %s -> %s   granularity: %s" % (res["start"], res["end"], res["granularity"]))
    hdr = ["host", "type", "inserted", "updated", "deleted", "net in", "disk written"]
    lines = [hdr]
    for r in res["hosts"] + [dict(res["total"], host="TOTAL", type="")]:
        lines.append([r["host"], r["type"],
                      "{:,}".format(r["docs_inserted"]), "{:,}".format(r["docs_updated"]),
                      "{:,}".format(r["docs_deleted"]),
                      human(r["net_bytes_in"]), human(r["cache_bytes_written"])])
    widths = [max(len(l[i]) for l in lines) for i in range(len(hdr))]
    for i, l in enumerate(lines):
        print("  ".join(c.ljust(widths[j]) if j < 2 else c.rjust(widths[j]) for j, c in enumerate(l)))
        if i == 0 or i == len(lines) - 2:
            print("  ".join("-" * w for w in widths))
    t = res["total"]
    print("\nWrite ops: insert={:,} update={:,} delete={:,}".format(
        t["ops_insert"], t["ops_update"], t["ops_delete"]))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("configure", help="store Ops Manager URL / project / API key").set_defaults(fn=cmd_configure)

    r = sub.add_parser("report", help="write statistics for a time window")
    r.add_argument("--hours", type=float, default=24, help="window length ending now/--to (default 24)")
    r.add_argument("--from", dest="start", help="window start, ISO 8601 (UTC if it ends in Z, else local)")
    r.add_argument("--to", help="window end, ISO 8601 (default now)")
    r.add_argument("--granularity", choices=["PT10S", "PT1M", "PT5M", "PT1H", "P1D"],
                   help="override automatic granularity")
    r.add_argument("--cluster", help="limit to a replica set name or cluster id")
    r.add_argument("--all-hosts", action="store_true",
                   help="include secondaries (counts replicated writes again)")
    r.add_argument("--json", action="store_true", help="print JSON instead of a table")
    r.add_argument("--csv", help="also write per-host rows to this CSV file")
    r.set_defaults(fn=cmd_report)

    c = sub.add_parser("collect", help="store new samples in SQLite (run from cron)")
    c.add_argument("--db", default=DB_PATH, help="SQLite file (default %s)" % DB_PATH)
    c.add_argument("--granularity", default="PT1M", choices=["PT10S", "PT1M", "PT5M", "PT1H"])
    c.add_argument("--backfill-hours", type=float, default=24,
                   help="how far back to fetch on a host's first run (default 24)")
    c.add_argument("--prune-days", type=int, default=0, help="delete samples older than N days")
    c.set_defaults(fn=cmd_collect)

    h = sub.add_parser("history", help="report from the SQLite history")
    h.add_argument("--db", default=DB_PATH)
    h.add_argument("--hours", type=float, default=24)
    h.add_argument("--from", dest="start")
    h.add_argument("--to")
    h.add_argument("--cluster", help="limit to a replica set name or cluster id")
    h.add_argument("--all-hosts", action="store_true",
                   help="include samples taken while a host was a secondary")
    h.add_argument("--json", action="store_true")
    h.add_argument("--csv")
    h.set_defaults(fn=cmd_history)

    b = sub.add_parser("blocks", help="percentage of changed data-file blocks between scans")
    bsub = b.add_subparsers(dest="bcmd", required=True)
    bs = bsub.add_parser("scan", help="hash the dbPath files (needs read access, usually root)")
    bs.add_argument("--path", default="/data", help="MongoDB dbPath (default /data)")
    bs.add_argument("--db", default=DB_PATH)
    bs.add_argument("--mongo-uri", default=MONGO_URI,
                    help="used to map files to databases/collections (default %s)" % MONGO_URI)
    bs.add_argument("--no-map", action="store_true", help="skip the file -> collection mapping")
    bs.add_argument("--keep-days", type=int, default=0, help="delete scans older than N days")
    bs.add_argument("--quiet", action="store_true", help="don't compare with the previous scan")
    bs.set_defaults(fn=cmd_blocks_scan)

    br = bsub.add_parser("report", help="changed blocks between two scans")
    br.add_argument("--db", default=DB_PATH)
    br.add_argument("--hours", type=float, default=24,
                    help="compare the latest scan with the one this many hours earlier (default 24)")
    br.add_argument("--from", dest="start")
    br.add_argument("--to")
    br.add_argument("--base", type=int, help="baseline scan id (overrides the time window)")
    br.add_argument("--target", type=int, help="target scan id (default latest)")
    br.add_argument("--sizes", default=",".join(BLOCK_SIZES),
                    help="block sizes to report, comma separated (default %s)" % ",".join(BLOCK_SIZES))
    br.add_argument("--by", choices=["database", "collection"], default="database")
    br.add_argument("--json", action="store_true")
    br.add_argument("--csv")
    br.set_defaults(fn=cmd_blocks_report)

    bl = bsub.add_parser("list", help="list stored scans")
    bl.add_argument("--db", default=DB_PATH)
    bl.set_defaults(fn=cmd_blocks_list)

    k = sub.add_parser("clean", help="delete stored samples and/or block scans")
    k.add_argument("--db", default=DB_PATH)
    k.add_argument("--samples", action="store_true", help="delete Ops Manager samples (collect/history)")
    k.add_argument("--blocks", action="store_true", help="delete block scans")
    k.add_argument("--all", action="store_true", help="delete both")
    k.add_argument("--older-than-days", type=float, help="only data older than N days (default: everything)")
    k.add_argument("--before", help="only data before this time, ISO 8601")
    k.add_argument("--dry-run", action="store_true", help="show what would be deleted")
    k.add_argument("--yes", "-y", action="store_true", help="don't ask for confirmation (cron)")
    k.set_defaults(fn=cmd_clean)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
