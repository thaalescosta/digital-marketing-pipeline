"""Thin command-line entry points: ``datagen-backfill`` and ``datagen-daily``.

The CLIs are the only place the wall clock may be read (``inserted_at``);
the library itself never touches it (D5).
"""

from __future__ import annotations

import argparse
import calendar
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from . import config
from .api import generate_backfill

_DEFAULT_HISTORY_MONTHS = 6


def _add_months(day: date, months: int) -> date:
    month_index = day.year * 12 + (day.month - 1) + months
    year, month_zero = divmod(month_index, 12)
    month = month_zero + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, last_day))


def _inserted_at(value: str | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    return pd.Timestamp(value)


def _backfill_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill deterministic fake marketing data as "
        "partitioned Parquet files."
    )
    parser.add_argument(
        "--start",
        default=config.SIM_START_DATE.isoformat(),
        help="First data date (inclusive).",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Last data date (inclusive); defaults to six months after --start.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Root directory for partitioned Parquet output.",
    )
    parser.add_argument(
        "--inserted-at",
        default=None,
        help="Run timestamp (ISO); defaults to the current UTC time.",
    )
    return parser


def _daily_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write deterministic fake marketing Parquet partitions "
        "for a single date."
    )
    parser.add_argument("--date", required=True, help="Data date (YYYY-MM-DD).")
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Root directory for partitioned Parquet output.",
    )
    parser.add_argument(
        "--inserted-at",
        default=None,
        help="Run timestamp (ISO); defaults to the current UTC time.",
    )
    return parser


def backfill_main(argv: list[str] | None = None) -> int:
    """Run the historical backfill for all sources and print the file count."""
    args = _backfill_parser().parse_args(argv)
    start = date.fromisoformat(args.start)
    if args.end is None:
        end = _add_months(start, _DEFAULT_HISTORY_MONTHS) - timedelta(days=1)
    else:
        end = date.fromisoformat(args.end)
    inserted_at = _inserted_at(args.inserted_at)
    paths = generate_backfill(
        start, end, Path(args.out_dir), inserted_at, config.GEN_SEED
    )
    print(f"wrote {len(paths)} Parquet partition files under {args.out_dir}")
    return 0


def daily_main(argv: list[str] | None = None) -> int:
    """Write every source for a single date and print the file count."""
    args = _daily_parser().parse_args(argv)
    day = date.fromisoformat(args.date)
    inserted_at = _inserted_at(args.inserted_at)
    paths = generate_backfill(
        day, day, Path(args.out_dir), inserted_at, config.GEN_SEED
    )
    print(f"wrote {len(paths)} Parquet partition files under {args.out_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Dispatch ``python -m datagen backfill|daily ...`` to the sub-commands."""
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if not raw_args:
        print("usage: datagen backfill|daily ...", file=sys.stderr)
        return 2
    command, rest = raw_args[0], raw_args[1:]
    if command == "backfill":
        return backfill_main(rest)
    if command == "daily":
        return daily_main(rest)
    print(f"unknown datagen command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())