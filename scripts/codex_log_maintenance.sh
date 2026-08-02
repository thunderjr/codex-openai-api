#!/usr/bin/env bash
# Reclaim disk from the Codex tracing log database.
#
# Codex prunes old rows on its own but never gives the pages back: the DB is
# created with auto_vacuum=INCREMENTAL, which only releases free pages when
# something issues `PRAGMA incremental_vacuum`, and Codex never does. Left alone
# the file grows without bound while the live row count stays flat -- it reached
# 3.4 GB holding 3.2 GB of free pages and ~52k rows before this script existed.
#
# incremental_vacuum is used rather than a full VACUUM: it releases the free
# pages in place, so it needs no 2x temp space and no full-file rewrite.
#
# Run it from cron/systemd (weekly is plenty). Safe to run while the gateway is
# up -- SQLite serialises it against Codex's writers -- but a stopped container
# means it never has to wait on a busy lock.
set -euo pipefail

CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
BUSY_TIMEOUT_MS="${BUSY_TIMEOUT_MS:-30000}"

command -v sqlite3 >/dev/null || { echo "sqlite3 not installed" >&2; exit 1; }

shopt -s nullglob
databases=("$CODEX_HOME"/logs_*.sqlite)
(( ${#databases[@]} )) || { echo "no log databases under $CODEX_HOME"; exit 0; }

# KiB used by a DB and its sidecars. Only sums files that exist: a checkpointed
# database has no -wal, and `du` on a missing path would fail the whole script.
disk_kib() {
    local total=0 path size
    for path in "$1" "$1-wal" "$1-shm"; do
        [[ -f $path ]] || continue
        size=$(du -sk "$path" | cut -f1)
        total=$((total + size))
    done
    printf '%s' "$total"
}

total_before=0 total_after=0
for db in "${databases[@]}"; do
    before=$(disk_kib "$db")

    # Checkpoint first so WAL contents land in the main DB, then release the free
    # pages, then truncate the now-redundant WAL back to zero.
    #
    # stdout is discarded on purpose: the sqlite3 CLI prints a blank line per page
    # released, which is hundreds of thousands of lines on a neglected database.
    # stderr still surfaces real failures, and `set -e` still catches a bad exit.
    sqlite3 "$db" >/dev/null <<SQL
PRAGMA busy_timeout = $BUSY_TIMEOUT_MS;
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA incremental_vacuum;
PRAGMA wal_checkpoint(TRUNCATE);
SQL

    after=$(disk_kib "$db")
    total_before=$((total_before + before))
    total_after=$((total_after + after))
    printf '%s: %d MiB -> %d MiB\n' "$(basename "$db")" $((before / 1024)) $((after / 1024))
done

printf 'total: %d MiB -> %d MiB (reclaimed %d MiB)\n' \
    $((total_before / 1024)) $((total_after / 1024)) \
    $(((total_before - total_after) / 1024))
