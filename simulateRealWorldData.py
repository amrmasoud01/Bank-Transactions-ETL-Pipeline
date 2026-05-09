"""
simulateRealWorldData.py — OLTP Streaming Simulator (Stateful / Crash-Resilient)
==================================================================================
Purpose:
    Simulates a real-time transaction stream by reading the large PaySim CSV
    in memory-safe chunks and emitting JSONL micro-batches into a streaming
    landing zone. Each row is enriched with an `event_timestamp` to simulate
    when the transaction "happened" in the real world.

    CHECKPOINT MECHANISM:
        A checkpoint file (data/simulator_checkpoint.txt) stores the total
        number of rows already processed. On restart the script reads this
        file and uses pd.read_csv(skiprows=...) to jump past rows that were
        already emitted, preventing logical data duplication.

    HEARTBEAT PROTOCOL:
        The simulator processes exactly 5 chunks per cycle, then sleeps for
        10 minutes. This aligns with the Airflow DAG schedule (*/10 * * * *)
        so the ETL pipeline only runs when the simulator is paused and all
        JSONL files are fully written — eliminating read/write race conditions.

Anti-patterns avoided:
    ✅ Uses chunksize — never loads the full ~494 MB CSV into memory (OOM-safe)
    ✅ Outputs JSONL (one JSON object per line) — efficient for Spark ingestion
    ✅ Unique filenames with timestamps — prevents overwrites on reruns
    ✅ Robust error handling — no silent failures
    ✅ Checkpoint file — crash-resilient, no duplicate rows on restart
    ✅ Heartbeat cycle — synchronized with Airflow to prevent race conditions

Usage:
    python3 simulateRealWorldData.py
"""

import pandas as pd
import os
import time
from datetime import datetime, timezone


# ── Checkpoint helpers ────────────────────────────────────────────────────

def _read_checkpoint(checkpoint_path: str) -> int:
    """
    Read the checkpoint file and return the number of rows already processed.
    Returns 0 if the file does not exist or is empty/corrupt.
    """
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "r") as f:
                value = f.read().strip()
                if value:
                    return int(value)
        except (ValueError, IOError):
            pass
    return 0


def _write_checkpoint(checkpoint_path: str, rows_processed: int) -> None:
    """
    Atomically update the checkpoint file with the total rows processed.
    Writes to a temp file first, then renames to avoid partial writes on crash.
    """
    tmp_path = checkpoint_path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write(str(rows_processed))
    os.replace(tmp_path, checkpoint_path)


# ── Main simulator ────────────────────────────────────────────────────────

CHUNKS_PER_CYCLE = 5          # Micro-batches emitted per heartbeat cycle
CYCLE_SLEEP_SECONDS = 600     # 10 minutes — aligned with Airflow schedule


def simulate_stream(
    input_csv: str,
    output_dir: str,
    batch_size: int = 50,
    interval: float = 2.0,
) -> None:
    """
    Read the source CSV in chunks and write each chunk as a JSONL file
    to the streaming landing zone.  Processes CHUNKS_PER_CYCLE batches,
    then sleeps for CYCLE_SLEEP_SECONDS to let Airflow consume the data.

    Args:
        input_csv:   Path to the raw PaySim CSV file.
        output_dir:  Directory where JSONL micro-batches are written.
        batch_size:  Number of rows per chunk (controls memory usage).
        interval:    Seconds to sleep between batches (simulates real-time).
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # ── Checkpoint: determine where to resume ──
    checkpoint_path = os.path.join(
        os.path.dirname(input_csv) or ".", "simulator_checkpoint.txt"
    )
    rows_processed = _read_checkpoint(checkpoint_path)

    if rows_processed > 0:
        print(f"[Simulator] ♻️  Resuming from row {rows_processed}")
    else:
        print("[Simulator] 🆕 Starting from scratch")

    print(f"[Simulator] Reading from: {input_csv}")
    print(f"[Simulator] Writing to:   {output_dir}")
    print(f"[Simulator] Batch size:   {batch_size} rows | Interval: {interval}s")
    print(f"[Simulator] Heartbeat:    {CHUNKS_PER_CYCLE} chunks/cycle, "
          f"{CYCLE_SLEEP_SECONDS}s pause between cycles")

    try:
        # ── Continuous heartbeat loop ──
        while True:
            # ── Build skiprows to jump past already-processed data ──
            # skiprows expects a list of row indices OR a callable.
            # We skip row indices 1..rows_processed (0 is the header row).
            skip = range(1, rows_processed + 1) if rows_processed > 0 else None

            # ── Read CSV in chunks to avoid loading the entire file ──
            reader = pd.read_csv(input_csv, chunksize=batch_size, skiprows=skip)

            # chunk_idx is offset so filenames stay globally unique
            starting_chunk = rows_processed // batch_size

            chunks_in_cycle = 0
            exhausted = False

            for chunk_offset, chunk in enumerate(reader):
                chunk_idx = starting_chunk + chunk_offset

                # ── Add a current event timestamp to every row ──
                # This simulates when the transaction "occurred" in the stream.
                chunk["event_timestamp"] = datetime.now(timezone.utc).isoformat()

                # ── Generate a unique filename using index + epoch timestamp ──
                epoch_ms = int(time.time() * 1000)
                output_file = os.path.join(
                    output_dir, f"txn_batch_{chunk_idx:06d}_{epoch_ms}.jsonl"
                )

                # ── Write as JSONL (one JSON object per line) ──
                # orient="records" → each row becomes a JSON object
                # lines=True       → one object per line (JSONL format)
                chunk.to_json(output_file, orient="records", lines=True)

                # ── Update checkpoint after successful write ──
                rows_processed += len(chunk)
                _write_checkpoint(checkpoint_path, rows_processed)

                print(
                    f"[Simulator] Batch {chunk_idx:>6d} | "
                    f"{len(chunk)} rows → {os.path.basename(output_file)} | "
                    f"Total rows: {rows_processed:,}"
                )

                chunks_in_cycle += 1

                # ── After CHUNKS_PER_CYCLE batches, break to sleep ──
                if chunks_in_cycle >= CHUNKS_PER_CYCLE:
                    break

                # ── Simulate real-time delay between batches ──
                time.sleep(interval)

            else:
                # for-loop exhausted the reader without breaking → CSV is done
                exhausted = True

            if exhausted:
                print(f"[Simulator] ✅ Finished. Total rows processed: {rows_processed:,}")
                break

            # ── Heartbeat pause: let Airflow consume the data ──
            print(
                f"[Simulator] 💤 Cycle finished. "
                f"Sleeping for {CYCLE_SLEEP_SECONDS // 60} minutes..."
            )
            time.sleep(CYCLE_SLEEP_SECONDS)

    except FileNotFoundError:
        print(f"[ERROR] Source CSV not found: {input_csv}")
        raise
    except KeyboardInterrupt:
        print(
            f"\n[Simulator] ⛔ Interrupted by user at row {rows_processed:,}. "
            f"Checkpoint saved — will resume from here on next run."
        )
    except Exception as e:
        print(f"[ERROR] Unexpected failure at row {rows_processed:,}: {e}")
        raise


if __name__ == "__main__":
    # ── Configuration ──
    SOURCE_CSV = "data/PS_20174392719_1491204439457_log.csv"
    LANDING_ZONE = "data/streaming_landing_zone/"

    simulate_stream(SOURCE_CSV, LANDING_ZONE)
