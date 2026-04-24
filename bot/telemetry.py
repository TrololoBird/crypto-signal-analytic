from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


UTC = timezone.utc


def symbol_storage_dirname(symbol: str) -> str:
    raw = str(symbol or "").strip()
    if not raw:
        return "symbol"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._")
    if not safe or safe in {".", ".."}:
        safe = "symbol"
    if safe == raw and "/" not in raw and "\\" not in raw:
        return safe
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{safe}__{digest}"


def rotate_file_if_needed(path: Path, max_size_mb: int) -> None:
    if max_size_mb <= 0 or not path.exists():
        return
    max_bytes = max_size_mb * 1024 * 1024
    if path.stat().st_size <= max_bytes:
        return
    stamp = date.today().isoformat()
    archive = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
    counter = 1
    while archive.exists():
        archive = path.with_name(f"{path.stem}.{stamp}.{counter}{path.suffix}")
        counter += 1
    archive.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(archive))


class TelemetryStore:
    def __init__(self, base_dir: Path, run_id: str | None = None, rotation_max_mb: int = 50) -> None:
        self.root_dir = base_dir
        self.run_id = run_id
        self.rotation_max_mb = max(1, int(rotation_max_mb))
        self.base_dir = base_dir / "runs" / run_id if run_id else base_dir
        self.analysis_dir = self.base_dir / "analysis"
        self.raw_dir = self.base_dir / "raw"
        self.features_dir = self.base_dir / "features"
        self.replay_dir = self.base_dir / "replay"
        self.market_dir = self.base_dir / "market_history"
        self.market_dir.mkdir(parents=True, exist_ok=True)
        self.analysis_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.features_dir.mkdir(parents=True, exist_ok=True)
        self.replay_dir.mkdir(parents=True, exist_ok=True)
        if run_id:
            metadata_path = self.base_dir / "run_metadata.json"
            if not metadata_path.exists():
                metadata_path.write_text(json.dumps({"run_id": run_id}, indent=2, ensure_ascii=True), encoding="utf-8")

    def append_jsonl(self, relative_name: str, row: dict[str, Any]) -> None:
        path = self.analysis_dir / relative_name
        self._append_jsonl_path(path, row)

    def append_raw_jsonl(self, relative_name: str, row: dict[str, Any]) -> None:
        path = self.raw_dir / relative_name
        self._append_jsonl_path(path, row)

    def append_feature_jsonl(self, relative_name: str, row: dict[str, Any]) -> None:
        path = self.features_dir / relative_name
        self._append_jsonl_path(path, row)

    def append_replay_jsonl(self, relative_name: str, row: dict[str, Any]) -> None:
        path = self.replay_dir / relative_name
        self._append_jsonl_path(path, row)

    def append_symbol_jsonl(self, bucket: str, symbol: str, relative_name: str, row: dict[str, Any]) -> None:
        if bucket == "analysis":
            base_dir = self.analysis_dir
        elif bucket == "raw":
            base_dir = self.raw_dir
        elif bucket == "features":
            base_dir = self.features_dir
        elif bucket == "replay":
            base_dir = self.replay_dir
        else:
            raise ValueError(f"unsupported telemetry bucket: {bucket}")
        path = base_dir / "by_symbol" / symbol_storage_dirname(symbol) / relative_name
        self._append_jsonl_path(path, row)

    def read_csv_tail(self, path: Path, max_rows: int) -> pl.DataFrame | None:
        return self._read_csv_tail(path, max_rows)

    def _append_jsonl_path(self, path: Path, row: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        rotate_file_if_needed(path, self.rotation_max_mb)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=True, default=str) + "\n")

    def write_rejection_summary(self, cycle_id: str, rejections: dict[str, int]) -> None:
        self.append_jsonl(
            "rejections.jsonl",
            {
                "ts": datetime.now(UTC).isoformat(),
                "cycle_id": cycle_id,
                "rejections": dict(rejections),
            },
        )

    def append_calibration_snapshot(self, symbol: str, snapshot: dict[str, Any]) -> None:
        row = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "funding_rate": snapshot.get("funding_rate"),
            "liquidation_notional_usd": snapshot.get("liquidation_notional_usd", snapshot.get("liquidation_notional")),
            "oi_growth_pct": snapshot.get("oi_growth_pct"),
            "volume_ratio_15m": snapshot.get("volume_ratio_15m"),
            "spread_bps": snapshot.get("spread_bps"),
            "asset_group": snapshot.get("asset_group"),
            "cycle_timestamp": snapshot.get("cycle_timestamp"),
        }
        if all(
            row[key] is None
            for key in ("funding_rate", "liquidation_notional_usd", "oi_growth_pct", "volume_ratio_15m", "spread_bps")
        ):
            return
        self._append_jsonl_path(self.root_dir / "calibration_snapshots.jsonl", row)

    def persist_candles(self, symbol: str, timeframe: str, df: pl.DataFrame, max_rows: int) -> None:
        out_dir = self.market_dir / symbol_storage_dirname(symbol)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{timeframe}.csv"
        frame = df.clone()
        if frame.is_empty():
            return
        # Convert time columns to string for consistent CSV storage
        for column in ("time", "close_time"):
            if column in frame.columns:
                frame = frame.with_columns(pl.col(column).cast(pl.Utf8).alias(column))
        frame = frame.unique(subset=["time"], keep="last").sort("time")
        if not path.exists() or path.stat().st_size == 0:
            if max_rows > 0:
                frame = frame.tail(max_rows)
            frame.write_csv(path)
            return

        tail_rows = min(max_rows if max_rows > 0 else len(frame), max(len(frame) * 3, 256))
        existing_tail = self._read_csv_tail(path, tail_rows)
        if existing_tail is None or existing_tail.is_empty():
            combined = frame
        else:
            combined = pl.concat([existing_tail, frame], how="diagonal")
            combined = combined.unique(subset=["time"], keep="last").sort("time")
        # Write only new rows that don't exist in the file
        if existing_tail is not None and not existing_tail.is_empty():
            known_times = set(existing_tail["time"].cast(pl.Utf8).to_list())
            append_rows = combined.filter(~pl.col("time").cast(pl.Utf8).is_in(list(known_times)))
        else:
            append_rows = combined
        if not append_rows.is_empty():
            # Polars doesn't support append mode - read existing, concat, write
            try:
                existing_full = pl.read_csv(path)
                full_data = pl.concat([existing_full, append_rows], how="diagonal")
                full_data = full_data.unique(subset=["time"], keep="last").sort("time")
                if max_rows > 0:
                    full_data = full_data.tail(max_rows)
                full_data.write_csv(path)
            except Exception:
                # Fallback: just write the combined data
                combined.write_csv(path)
        if max_rows > 0:
            self._compact_csv(path, max_rows)

    def _compact_csv(self, path: Path, max_rows: int) -> None:
        existing = self._read_csv_tail(path, max(max_rows * 3, 512))
        if existing is None or existing.is_empty():
            return
        existing = existing.unique(subset=["time"], keep="last").sort("time")
        if max_rows > 0:
            existing = existing.tail(max_rows)
        existing.write_csv(path)

    def _read_csv_tail(self, path: Path, max_rows: int) -> pl.DataFrame | None:
        if not path.exists():
            return None
        if max_rows <= 0:
            return pl.read_csv(path)
        # Polars doesn't have native tail reading - read all then tail
        try:
            df = pl.read_csv(path)
            if df.is_empty():
                return None
            if max_rows > 0:
                return df.tail(max_rows)
            return df
        except Exception:
            return None
