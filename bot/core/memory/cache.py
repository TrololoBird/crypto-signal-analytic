"""Parquet-based caching for time-series data."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

import polars as pl

LOG = logging.getLogger("bot.core.memory.cache")


class ParquetCache:
    """Parquet-based cache for efficient time-series storage.
    
    Uses chunked storage by date for efficient append and query.
    Supports automatic compaction for old data.
    """
    
    def __init__(self, cache_dir: Path | str, max_chunk_days: int = 7):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._max_chunk_days = max_chunk_days
    
    def _get_chunk_path(self, symbol: str, timeframe: str, date: datetime) -> Path:
        """Get path for a specific chunk."""
        chunk_name = f"{symbol}_{timeframe}_{date.strftime('%Y%m%d')}.parquet"
        return self._cache_dir / chunk_name
    
    def _get_chunk_pattern(self, symbol: str, timeframe: str) -> str:
        """Get glob pattern for symbol/timeframe chunks."""
        return f"{symbol}_{timeframe}_*.parquet"
    
    def append(
        self, 
        symbol: str, 
        timeframe: str, 
        df: pl.DataFrame,
        timestamp_col: str = "timestamp"
    ) -> None:
        """Append data to appropriate chunk(s).
        
        Data is partitioned by date for efficient storage.
        """
        if df.is_empty():
            return
        
        # Ensure timestamp column exists
        if timestamp_col not in df.columns:
            if "close_time" in df.columns:
                # Convert close_time (ms) to timestamp
                df = df.with_columns([
                    (pl.col("close_time") / 1000).cast(pl.Datetime).alias(timestamp_col)
                ])
            elif "open_time" in df.columns:
                df = df.with_columns([
                    (pl.col("open_time") / 1000).cast(pl.Datetime).alias(timestamp_col)
                ])
        
        # Group by date and write to separate chunks
        dates = df[timestamp_col].dt.date().unique().to_list()
        
        for date in dates:
            chunk_path = self._get_chunk_path(symbol, timeframe, datetime.combine(date, datetime.min.time()))
            
            # Filter data for this date
            day_df = df.filter(pl.col(timestamp_col).dt.date() == date)
            
            if chunk_path.exists():
                # Read existing and merge
                existing = pl.read_parquet(chunk_path)
                merged = pl.concat([existing, day_df]).unique(subset=[timestamp_col], keep="last")
                merged = merged.sort(timestamp_col)
            else:
                merged = day_df.sort(timestamp_col)
            
            # Write to parquet
            merged.write_parquet(chunk_path, compression="zstd")
        
        LOG.debug("Cached %d rows for %s/%s", len(df), symbol, timeframe)
    
    def read(
        self, 
        symbol: str, 
        timeframe: str,
        since: datetime | None = None,
        until: datetime | None = None
    ) -> pl.DataFrame:
        """Read cached data for symbol/timeframe.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Time interval (1h, 4h, etc.)
            since: Start datetime (optional)
            until: End datetime (optional)
            
        Returns:
            Polars DataFrame with cached data
        """
        pattern = self._get_chunk_pattern(symbol, timeframe)
        chunks = list(self._cache_dir.glob(pattern))
        
        if not chunks:
            return pl.DataFrame()
        
        # Read all chunks and concatenate
        dfs = [pl.read_parquet(chunk) for chunk in chunks]
        combined = pl.concat(dfs).unique(keep="last").sort("timestamp")
        
        # Apply time filters
        if since:
            combined = combined.filter(pl.col("timestamp") >= since)
        if until:
            combined = combined.filter(pl.col("timestamp") <= until)
        
        return combined
    
    def read_recent(
        self, 
        symbol: str, 
        timeframe: str,
        lookback: timedelta
    ) -> pl.DataFrame:
        """Read recent data for symbol/timeframe."""
        since = datetime.utcnow() - lookback
        return self.read(symbol, timeframe, since=since)
    
    def compact(self, max_age_days: int = 30) -> None:
        """Compact old chunks into monthly files.
        
        This reduces file count for old data while maintaining query performance.
        """
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        
        # Find all daily chunks older than cutoff
        for chunk in self._cache_dir.glob("*.parquet"):
            # Parse date from filename
            try:
                parts = chunk.stem.split("_")
                if len(parts) >= 3:
                    date_str = parts[-1]
                    chunk_date = datetime.strptime(date_str, "%Y%m%d")
                    
                    if chunk_date < cutoff:
                        # TODO: Implement monthly compaction
                        # For now, just log old chunks
                        LOG.debug("Old chunk: %s (age > %d days)", chunk, max_age_days)
            except ValueError:
                continue
    
    def clear(self, symbol: str | None = None) -> None:
        """Clear cache for symbol or all symbols."""
        if symbol:
            pattern = f"{symbol}_*.parquet"
            files = list(self._cache_dir.glob(pattern))
        else:
            files = list(self._cache_dir.glob("*.parquet"))
        
        for f in files:
            f.unlink()
        
        LOG.info("Cleared %d cache files", len(files))


class TimeSeriesCache:
    """In-memory LRU cache for recent time-series data with disk backing.
    
    Optimized for frequently accessed recent data.
    """
    
    def __init__(
        self, 
        disk_cache: ParquetCache,
        max_symbols: int = 200,
        memory_bars: int = 500  # Bars to keep in memory per symbol/timeframe
    ):
        self._disk = disk_cache
        self._max_symbols = max_symbols
        self._memory_bars = memory_bars
        self._memory: dict[str, pl.DataFrame] = {}
        self._access_times: dict[str, datetime] = {}
    
    def _make_key(self, symbol: str, timeframe: str) -> str:
        """Create cache key."""
        return f"{symbol}:{timeframe}"
    
    def get(
        self, 
        symbol: str, 
        timeframe: str,
        lookback_bars: int | None = None
    ) -> pl.DataFrame:
        """Get data from cache (memory first, then disk)."""
        key = self._make_key(symbol, timeframe)
        bars = lookback_bars or self._memory_bars
        
        # Update access time
        self._access_times[key] = datetime.utcnow()
        
        # Check memory cache
        if key in self._memory:
            df = self._memory[key]
            if len(df) >= bars:
                return df.tail(bars)
        
        # Load from disk
        lookback = timedelta(hours=bars) if timeframe == "1h" else timedelta(days=bars)
        df = self._disk.read_recent(symbol, timeframe, lookback)
        
        # Store in memory (limited size)
        self._store_in_memory(key, df)
        
        return df.tail(bars) if len(df) > bars else df
    
    def put(self, symbol: str, timeframe: str, df: pl.DataFrame) -> None:
        """Store data in cache."""
        key = self._make_key(symbol, timeframe)
        
        # Store in memory
        self._store_in_memory(key, df)
        
        # Persist to disk
        self._disk.append(symbol, timeframe, df)
        
        self._access_times[key] = datetime.utcnow()
    
    def _store_in_memory(self, key: str, df: pl.DataFrame) -> None:
        """Store in memory with LRU eviction."""
        # Evict oldest if at capacity
        while len(self._memory) >= self._max_symbols and self._memory:
            oldest_key = min(
                self._access_times,
                key=lambda item_key: self._access_times.get(item_key, datetime.utcnow()),
            )
            del self._memory[oldest_key]
            del self._access_times[oldest_key]
        
        # Store truncated version
        if len(df) > self._memory_bars:
            self._memory[key] = df.tail(self._memory_bars)
        else:
            self._memory[key] = df
    
    def invalidate(self, symbol: str | None = None) -> None:
        """Invalidate cache entries."""
        if symbol:
            keys_to_remove = [
                k for k in self._memory.keys() 
                if k.startswith(f"{symbol}:")
            ]
            for key in keys_to_remove:
                del self._memory[key]
                del self._access_times[key]
        else:
            self._memory.clear()
            self._access_times.clear()
    
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        return {
            "memory_entries": len(self._memory),
            "max_entries": self._max_symbols,
            "memory_bars_per_entry": self._memory_bars,
        }
