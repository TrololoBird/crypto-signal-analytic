from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from bot.config import BotSettings

from .engine import VectorizedBacktester


def main() -> None:
    parser = argparse.ArgumentParser(description="Run vectorized backtest")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--setup", default="")
    parser.add_argument("--initial-equity", type=float, default=1.0)
    args = parser.parse_args()

    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    backtester = VectorizedBacktester(settings)
    result = backtester.run(
        symbol=args.symbol,
        start=datetime.fromisoformat(args.start),
        end=datetime.fromisoformat(args.end),
        timeframe=args.timeframe,
        setup_id=args.setup or None,
        initial_equity=args.initial_equity,
    )

    output_dir = settings.data_dir / "backtests"
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    csv_path = output_dir / f"equity_{args.symbol}_{ts}.csv"
    json_path = output_dir / f"summary_{args.symbol}_{ts}.json"

    result.equity_curve.write_csv(csv_path)
    json_path.write_text(
        json.dumps(result.to_dict(), ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps({"summary": str(json_path), "equity_curve": str(csv_path)}))


if __name__ == "__main__":
    main()
