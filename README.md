# Ops Manager Write Statistics

`write_stats.py` measures how much a MongoDB deployment writes, in two independent ways:

1. **Write statistics from Ops Manager** (Public API v1.0): documents inserted / updated / deleted, write operations, network bytes in, and WiredTiger bytes written to disk. Query live (`report`) or keep a SQLite history fed by cron (`collect` + `history`).
2. **Changed blocks** (`blocks`): the percentage of data-file blocks that changed between two points in time, at 1M / 4M / 8M / 16M block sizes, measured directly on the dbPath. Use it to size incremental or block-level backups and storage replication.

| Command | What it does | Needs |
| --- | --- | --- |
| `configure` | save Ops Manager URL, project ID, API key | |
| `report` | live write statistics for a time window | Ops Manager API |
| `collect` | store new samples in SQLite (cron) | Ops Manager API |
| `history` | write statistics from SQLite | nothing (local) |
| `blocks scan` | fingerprint every 1 MiB block of the data files | root (data files are `mongod`-only) |
| `blocks report` | changed-block percentages between two scans | nothing (local) |
| `blocks list` | list stored scans | nothing (local) |
| `clean` | delete stored samples and/or block scans | nothing (local) |

Every command has `--help`, e.g. `./write_stats.py blocks report --help`.

## Requirements

- Python 3.7+ and `requests` (`pip install requests`); for `blocks scan` run as root, root's Python needs `requests` too
- An Ops Manager API key (public + private) for the project with read access to hosts and measurements (Project Read Only is enough), and this machine on the key's API access list
- For `blocks scan`: read access to the dbPath (root) and `mongosh` (to name files by database/collection; optional)

## Setup

```
git clone <repo-url> /opt/WriteMonitoring
cd /opt/WriteMonitoring
pip install requests
./write_stats.py configure
```

The examples below assume the script lives in `/opt/WriteMonitoring` and runs as the user `monitor`; adjust paths to your setup.

| Prompt | Example |
| --- | --- |
| Ops Manager URL | `http://localhost:30080` |
| Project (group) ID | `5f1c2b3a4d5e6f7a8b9c0d1e` (Project Settings page) |
| API public key | |
| API private key | input is hidden |
| CA bundle path, or `false` | path to your CA file; `false` skips TLS verification (self-signed certs); blank = system CAs |

Values are saved to `~/.config/opsmanager-writestats/config.json` (mode `600`). Re-running `configure` shows the current values; press Enter to keep them.

### Files and environment variables

| What | Default | Override |
| --- | --- | --- |
| Config | `~/.config/opsmanager-writestats/config.json` | `OPSMGR_CONFIG` |
| History database (samples and block scans) | `history.db` next to the config | `OPSMGR_DB` or `--db` |
| Ops Manager URL / project / keys | config file | `OPSMGR_URL`, `OPSMGR_GROUP_ID`, `OPSMGR_PUBLIC_KEY`, `OPSMGR_PRIVATE_KEY` (take precedence) |
| MongoDB URI for `blocks scan` file mapping | `mongodb://127.0.0.1:27017/?directConnection=true` | `OPSMGR_MONGO_URI` or `--mongo-uri` |

Under `sudo`, `~` means the invoking user's home, so `sudo ./write_stats.py blocks scan` uses the same config and history database as your own runs.

Times are ISO 8601. A trailing `Z` means UTC; otherwise local time (`2026-10-20`, `2026-10-20T08:00`, `2026-10-20T05:00Z`).

## 1. Live report (`report`)

```
./write_stats.py report                                          # last 24 hours
./write_stats.py report --hours 6                                # last 6 hours
./write_stats.py report --hours 0.25 --granularity PT10S         # last 15 minutes, 10-second data
./write_stats.py report --from 2026-10-19 --to 2026-10-20        # one calendar day (local time)
./write_stats.py report --from 2026-10-20T05:00Z --to 2026-10-20T06:00Z   # one hour, UTC
./write_stats.py report --hours 168 --granularity PT1H           # last week, hourly data
./write_stats.py report --hours 24 --cluster myReplicaSet        # one replica set (name or cluster ID)
./write_stats.py report --hours 24 --all-hosts                   # include secondaries
./write_stats.py report --hours 24 --csv /tmp/writes.csv         # table + per-host CSV
./write_stats.py report --hours 24 --json                        # JSON only
```

Sample output:

```
Window: 2026-10-20T19:16:57Z -> 2026-10-20T19:31:57Z   granularity: PT10S
host               type        inserted  updated  deleted     net in  disk written
-----------------  ----------  --------  -------  -------  ---------  ------------
mongo-node1:27017  STANDALONE     4,167    4,011    1,551  86.05 MiB      1.24 GiB
-----------------  ----------  --------  -------  -------  ---------  ------------
TOTAL                             4,167    4,011    1,551  86.05 MiB      1.24 GiB

Write ops: insert=4,167 update=4,064 delete=958
```

| Option | Meaning |
| --- | --- |
| `--hours N` | window length ending now or at `--to` (default 24; fractions allowed) |
| `--from` / `--to` | explicit window |
| `--granularity` | `PT10S`, `PT1M`, `PT5M`, `PT1H`, `P1D`. Default: 1 min up to 6 h, 5 min up to 2 days, 1 h beyond. `PT10S` is kept by Ops Manager for 24 h only |
| `--cluster` | limit to a replica set name or cluster ID |
| `--all-hosts` | include secondaries (replicated writes are then counted again) |
| `--json` | print JSON instead of the table |
| `--csv FILE` | also write per-host rows to a CSV file |

Only primaries and standalones are summed by default. A failover inside the window misses the old primary's share; use history mode for exact attribution across failovers.

## 2. History mode (`collect` + `history`)

`collect` fetches new data points since its last run and stores them; `history` reports from the local database without contacting Ops Manager.

```
./write_stats.py collect                                  # 1-minute samples (default)
./write_stats.py collect --prune-days 90                  # also delete samples older than 90 days
./write_stats.py collect --backfill-hours 6               # first run for a host: fetch 6 h back (default 24)
./write_stats.py collect --granularity PT5M               # 5-minute samples
./write_stats.py collect --db /srv/writestats/history.db  # alternative database file
```

Sample output:

```
2026-10-20T19:14:51Z collected <N> samples into /home/monitor/.config/opsmanager-writestats/history.db
```

```
./write_stats.py history                                          # last 24 hours
./write_stats.py history --hours 1
./write_stats.py history --from 2026-10-13 --to 2026-10-20        # one week
./write_stats.py history --from 2026-10-20T00:00 --to 2026-10-20T12:00 --cluster myRS
./write_stats.py history --hours 24 --all-hosts                   # include secondary samples
./write_stats.py history --hours 24 --csv /tmp/day.csv
./write_stats.py history --hours 24 --json
```

`history` prints the same table as `report`, with `granularity: stored`.

Details:

- The first run for a host backfills 24 hours (`--backfill-hours`), but only as far back as Ops Manager still keeps data at that granularity; if less came back, a warning goes to stderr.
- Each run re-fetches the last 3 intervals and overwrites them, because the newest point may still be filling in. Re-running is safe and never double-counts.
- Keep one granularity per database. Switching between e.g. `PT10S` and `PT1M` double-counts the few minutes where they overlap.
- All data-bearing hosts are stored with their role per sample, taken from the metrics themselves: a host applying replicated ops (`OPCOUNTER_REPL_*`) was a secondary; one taking client writes without them was the primary; idle intervals keep the stored role, else the current one. So failovers are attributed correctly, also inside backfilled data. `history` counts primary/standalone samples only; `--all-hosts` adds secondary samples.
- `--cluster` matches a replica set name or a cluster ID (samples stored before the cluster ID was recorded match by replica set name only).

Check it works:

```
./write_stats.py collect && sqlite3 ~/.config/opsmanager-writestats/history.db 'select count(*), max(ts) from samples'
```

## 3. Changed blocks (`blocks`)

Ops Manager metrics cannot tell how many distinct blocks change: bytes written count the same block over and over, and WiredTiger writes pages to new locations on every checkpoint. `blocks` measures it on the data files instead.

### Scan

```
sudo ./write_stats.py blocks scan                          # scan /data, show change vs. previous scan
sudo ./write_stats.py blocks scan --quiet                  # scan only (for cron)
sudo ./write_stats.py blocks scan --path /var/lib/mongo    # other dbPath
sudo ./write_stats.py blocks scan --keep-days 14           # also delete scans older than 14 days
sudo ./write_stats.py blocks scan --no-map                 # don't name files by collection (no mongosh)
sudo ./write_stats.py blocks scan --mongo-uri 'mongodb://admin:secret@127.0.0.1:27017/?authSource=admin&directConnection=true'
```

Sample output (second scan, one minute after the first, under a heavy test load):

```
2026-10-20T19:49:32Z scan #2: 46 files, 625.02 MiB in 1.0s

Changed blocks: scan #1 (2026-10-20T19:48:28Z) -> scan #2 (2026-10-20T19:49:32Z)
group              files        size        1M blocks        4M blocks       8M blocks      16M blocks
-----------------  -----  ----------  ---------------  ---------------  --------------  --------------
WriteTest             10  287.02 MiB  79.7% (235/295)    94.9% (75/79)  100.0% (43/43)  100.0% (25/25)
admin                  2   40.00 KiB       0.0% (0/2)       0.0% (0/2)      0.0% (0/2)      0.0% (0/2)
config                 3   96.00 KiB     100.0% (3/3)     100.0% (3/3)    100.0% (3/3)    100.0% (3/3)
local                  4  112.00 KiB       0.0% (0/4)       0.0% (0/4)      0.0% (0/4)      0.0% (0/4)
test                   2   24.00 KiB       0.0% (0/2)       0.0% (0/2)      0.0% (0/2)      0.0% (0/2)
(diagnostic.data)      3    3.34 MiB      16.7% (1/6)      33.3% (1/3)     33.3% (1/3)     33.3% (1/3)
(journal)              3  299.81 MiB  68.7% (206/300)    69.3% (52/75)   71.8% (28/39)   76.2% (16/21)
(other)                5   34.35 MiB      2.7% (1/37)      8.3% (1/12)     12.5% (1/8)     16.7% (1/6)
(system)              14  231.41 KiB     21.4% (3/14)     21.4% (3/14)    21.4% (3/14)    21.4% (3/14)
-----------------  -----  ----------  ---------------  ---------------  --------------  --------------
databases             21  287.29 MiB  77.8% (238/306)    86.7% (78/90)   85.2% (46/54)   77.8% (28/36)
TOTAL                 46  625.02 MiB  67.7% (449/663)  69.6% (135/194)  67.0% (79/118)   61.2% (49/80)

1 file(s) removed since the baseline (100.00 MiB); not counted.
```

Each cell is `percent (changed blocks / total blocks)`. `databases` sums database files only; `TOTAL` is everything under the dbPath.

### Report

```
./write_stats.py blocks report                                   # latest scan vs. the one 24 h earlier
./write_stats.py blocks report --hours 1                         # latest scan vs. 1 h earlier
./write_stats.py blocks report --hours 168                       # one week
./write_stats.py blocks report --from 2026-10-20 --to 2026-10-21 # one calendar day (local time)
./write_stats.py blocks report --base 3 --target 9               # two specific scans (ids from 'blocks list')
./write_stats.py blocks report --base 3                          # scan 3 vs. latest
./write_stats.py blocks report --sizes 1M                        # one block size
./write_stats.py blocks report --sizes 4M,16M                    # some block sizes
./write_stats.py blocks report --by collection                   # per collection (with its indexes)
./write_stats.py blocks report --hours 24 --csv /tmp/blocks.csv  # table + CSV
./write_stats.py blocks report --hours 24 --json                 # JSON only
```

Sample `--by collection --sizes 1M,16M`:

```
group                   files        size        1M blocks     16M blocks
----------------------  -----  ----------  ---------------  -------------
WriteTest.collection_1      2   58.37 MiB    81.7% (49/60)   100.0% (5/5)
WriteTest.collection_2      2   57.19 MiB    74.6% (44/59)   100.0% (5/5)
...
admin.system.version        2   40.00 KiB       0.0% (0/2)     0.0% (0/2)
(journal)                   3  299.81 MiB  68.7% (206/300)  76.2% (16/21)
----------------------  -----  ----------  ---------------  -------------
databases                  21  287.29 MiB  77.8% (238/306)  77.8% (28/36)
TOTAL                      46  625.02 MiB  67.7% (449/663)  61.2% (49/80)
```

CSV columns: `group, files, bytes`, then `<size>_changed, <size>_total, <size>_pct` for each size.

| Option | Meaning |
| --- | --- |
| `--hours N` | compare the latest scan with the last one taken at least N hours earlier (default 24) |
| `--from` / `--to` | baseline = last scan at/before `--from` (else the first after it); target = last scan at/before `--to` |
| `--base ID` / `--target ID` | compare specific scans (target defaults to the latest) |
| `--sizes` | any of `1M,4M,8M,16M` (default all) |
| `--by` | `database` (default) or `collection` |
| `--json`, `--csv FILE` | machine-readable output |

### List

```
./write_stats.py blocks list
```
```
#1     2026-10-20T19:48:28Z  /data  45 files  525.04 MiB
#2     2026-10-20T19:49:32Z  /data  46 files  625.02 MiB
```

### How it works

- `scan` reads every file under the dbPath and stores an 8-byte hash per 1 MiB block. A 4M/8M/16M block counts as changed if any 1 MiB block inside it changed, so one scan serves all sizes. Scanning reads all data once (about 1 s for 600 MB here; plan for disk read throughput on large databases).
- A block is changed if its content differs between the two scans, or it did not exist in the baseline (new files, file growth). Files deleted in between are reported but not counted. A block that changed and changed back in between counts as unchanged, like an incremental backup taken at those two points.
- Files are named by database/collection via `mongosh` and `$collStats`. Others are grouped as `(journal)`, `(diagnostic.data)`, `(system)` (WiredTiger metadata), `(other)` (e.g. a log file kept in the dbPath) or `(unmapped)`.
- Small files count as one block at every size, so a group's percentage at 16M can be lower than at 4M.
- Blocks are counted from the start of each file. Storage tracks blocks on the volume, and files need not start on a block boundary, so treat the result as a close estimate.
- Storage cost: about 8 bytes per MiB of data per scan (1 TB of data is ~8 MB per scan). Prune with `--keep-days`.

## 4. Cleaning the database (`clean`)

Deletes Ops Manager samples (`collect` / `history`), block scans (`blocks`) or both from the history database, then compacts the file (SQLite `VACUUM`). Without an age limit, everything of the chosen kind is deleted.

```
./write_stats.py clean --all --dry-run                          # show what would be deleted, delete nothing
./write_stats.py clean --samples                                # delete all samples (asks for confirmation)
./write_stats.py clean --blocks                                 # delete all block scans
./write_stats.py clean --all                                    # empty the database
./write_stats.py clean --samples --older-than-days 90           # samples older than 90 days
./write_stats.py clean --blocks --older-than-days 14 --yes      # block scans older than 14 days, no prompt
./write_stats.py clean --all --before 2026-10-01                # everything before a date (local time)
./write_stats.py clean --all --before 2026-10-20T19:58:00Z -y   # everything before a UTC time
./write_stats.py clean --all --db /srv/writestats/history.db    # another database file
```

Sample output:

```
/home/monitor/.config/opsmanager-writestats/history.db: delete 87 samples (2026-10-05T09:56:11Z .. 2026-10-05T11:22:11Z); 3 block scans (#1 .. #3) older than 2026-10-13T20:07:23Z
Delete? [y/N] y
Deleted. Database size 144.00 KiB -> 88.00 KiB
```

| Option | Meaning |
| --- | --- |
| `--samples` / `--blocks` / `--all` | what to delete (one is required) |
| `--older-than-days N` | only data older than N days (fractions allowed) |
| `--before TIME` | only data before this time |
| `--dry-run` | print what would be deleted and stop |
| `--yes`, `-y` | no confirmation prompt; required when not run from a terminal (cron) |

`collect --prune-days` and `blocks scan --keep-days` prune automatically on every run; `clean` is for one-off cleanups or a separate schedule. Deleting the scan that a later `blocks report` would use as its baseline makes that report fall back to the nearest remaining scan.

## Cron

Cron runs with a minimal environment: use full paths, and remember that `%` must be escaped as `\%` inside a crontab line. Credentials come from the config file, so the Ops Manager jobs must run as the user who ran `configure` (or set `OPSMGR_CONFIG` / `OPSMGR_DB` in the line).

### Your crontab (`crontab -e`): Ops Manager statistics and reports

```
# Store new Ops Manager samples every 10 minutes, keep 90 days
*/10 * * * * /opt/WriteMonitoring/write_stats.py collect --prune-days 90 >> /opt/WriteMonitoring/collect.log 2>&1

# Daily write statistics for the previous day, one CSV per day (00:05)
5 0 * * * /opt/WriteMonitoring/write_stats.py history --from "$(date -d yesterday +\%F)" --to "$(date +\%F)" --csv /opt/WriteMonitoring/reports/writes-$(date -d yesterday +\%F).csv >> /opt/WriteMonitoring/reports/writes.log 2>&1

# Weekly write statistics, Mondays 00:10
10 0 * * 1 /opt/WriteMonitoring/write_stats.py history --hours 168 > /opt/WriteMonitoring/reports/writes-week-$(date +\%F).txt 2>&1

# Daily changed-block percentages for the previous day (00:15), table + CSV
15 0 * * * /opt/WriteMonitoring/write_stats.py blocks report --from "$(date -d yesterday +\%F)" --to "$(date +\%F)" --csv /opt/WriteMonitoring/reports/blocks-$(date -d yesterday +\%F).csv > /opt/WriteMonitoring/reports/blocks-$(date -d yesterday +\%F).txt 2>&1

# Weekly changed blocks per collection, Mondays 00:20
20 0 * * 1 /opt/WriteMonitoring/write_stats.py blocks report --hours 168 --by collection > /opt/WriteMonitoring/reports/blocks-week-$(date +\%F).txt 2>&1

# Monthly cleanup, 1st of the month 03:00: samples older than 90 days, block scans older than 30 days
0 3 1 * * /opt/WriteMonitoring/write_stats.py clean --samples --older-than-days 90 --yes >> /opt/WriteMonitoring/clean.log 2>&1
5 3 1 * * /opt/WriteMonitoring/write_stats.py clean --blocks --older-than-days 30 --yes >> /opt/WriteMonitoring/clean.log 2>&1
```

Create the reports directory once: `mkdir -p /opt/WriteMonitoring/reports`.

### Root crontab (`sudo crontab -e`): block scans

Root's `~` is `/root`, so point `OPSMGR_DB` at your history database.

```
# Hourly block scan, keep 14 days of scans
0 * * * * OPSMGR_DB=/home/monitor/.config/opsmanager-writestats/history.db /opt/WriteMonitoring/write_stats.py blocks scan --quiet --keep-days 14 >> /opt/WriteMonitoring/blocks.log 2>&1
```

Alternatives:

```
# One scan a day at midnight (enough for daily change rates), keep 60 days
0 0 * * * OPSMGR_DB=/home/monitor/.config/opsmanager-writestats/history.db /opt/WriteMonitoring/write_stats.py blocks scan --quiet --keep-days 60 >> /opt/WriteMonitoring/blocks.log 2>&1

# Scan right before the backup window (e.g. 01:55) to measure change between backups
55 1 * * * OPSMGR_DB=/home/monitor/.config/opsmanager-writestats/history.db /opt/WriteMonitoring/write_stats.py blocks scan --quiet --keep-days 60 >> /opt/WriteMonitoring/blocks.log 2>&1

# MongoDB with authentication: pass the URI for the file -> collection mapping
0 * * * * OPSMGR_DB=/home/monitor/.config/opsmanager-writestats/history.db OPSMGR_MONGO_URI='mongodb://monitor:secret@127.0.0.1:27017/?authSource=admin&directConnection=true' /opt/WriteMonitoring/write_stats.py blocks scan --quiet --keep-days 14 >> /opt/WriteMonitoring/blocks.log 2>&1
```

`blocks report --hours N` and `--from/--to` pick the nearest scans, so the report's accuracy depends on the scan interval: hourly scans give daily reports that are within an hour of the requested window.

### Check the jobs

```
crontab -l; sudo crontab -l
tail /opt/WriteMonitoring/collect.log /opt/WriteMonitoring/blocks.log
./write_stats.py blocks list | tail -3
sqlite3 ~/.config/opsmanager-writestats/history.db 'select count(*), max(ts) from samples'
```

## How the numbers are computed

Ops Manager returns per-second rates. Each data point is multiplied by the granularity in seconds and summed, so totals are close estimates, not exact counts. Measurements used:

| Column | Ops Manager measurement |
| --- | --- |
| `docs_inserted/updated/deleted` | `DOCUMENT_METRICS_INSERTED/UPDATED/DELETED` |
| `ops_insert/update/delete` | `OPCOUNTER_INSERT/UPDATE/DELETE` |
| `net_bytes_in` | `NETWORK_BYTES_IN` |
| `cache_bytes_written` | `CACHE_BYTES_WRITTEN_FROM` |
| (role detection only) | `OPCOUNTER_REPL_INSERT/UPDATE/DELETE` |

mongos hosts are skipped (they do not report document metrics).

## Troubleshooting

- **`API error 401`**: wrong key, or this machine is not on the key's access list.
- **`No matching hosts found`**: wrong project ID, or `--cluster` does not match a replica set name or cluster ID.
- **All zeros for a column**: that measurement may not exist on your Ops Manager/MongoDB version; check the project's available measurements.
- **Empty or sparse older data**: fine-grained data is retained only briefly; use a coarser `--granularity` for old windows.
- **No data at `PT1M` but the agent is healthy**: Ops Manager stores raw 10-second data (kept 24 h) and builds 1-minute data from it in a background rollup. If the rollup falls behind (e.g. after Ops Manager was down), recent 1-minute data is missing until it catches up; `--granularity PT10S` works meanwhile. Check progress with `kubectl -n opsmanager logs opsmanager-0 | grep 'Beginning metric rollup' | tail -1`.
- **Agent shows ACTIVE but no data**: check `/var/log/mongodb-mms-automation/monitoring-agent.log` (as root) for `connection refused` (Ops Manager URL points at an old IP) or `serverStatus` timeouts (slow hostname resolution; make sure the hostname resolves via `/etc/hosts`).
- **`blocks scan`: Permission denied**: run it with `sudo` or from root's crontab.
- **`blocks` rows show `(unmapped)`**: `mongosh` was missing or could not connect; check `--mongo-uri`.
- **`Refusing to delete without confirmation`**: `clean` was run without a terminal (cron, pipe); add `--yes`.
- **`Need two different scans for that window`**: take more scans, or see `blocks list` and use `--base` / `--target`.
- **TLS errors**: set the CA bundle path in `configure`.
