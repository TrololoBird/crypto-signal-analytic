from __future__ import annotations

import argparse
from datetime import datetime

from .training_pipeline import MLTrainingPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward ML training")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    pipeline = MLTrainingPipeline()
    pipeline.walk_forward_train(
        start_date=datetime.fromisoformat(args.start),
        end_date=datetime.fromisoformat(args.end),
    )


if __name__ == "__main__":
    main()
