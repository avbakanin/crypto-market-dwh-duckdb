"""Command line shared by the extracts and the loader: how a run with nothing new ends.

Nothing new -> no batch written (an extract, see batches.py) or none loaded (the loader).
By default that is a success (exit 0) for a person at a terminal. An orchestrator can ask
for its own exit code instead: the Airflow DAG passes 99, which BashOperator reports as a
skipped task, so the tasks after it do not run and nothing is rebuilt for no new data.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

NOTHING_NEW_OPTION = "--nothing-new-exit-code"


def add_nothing_new_option(parser: argparse.ArgumentParser, what: str) -> None:
    parser.add_argument(
        NOTHING_NEW_OPTION,
        type=int,
        default=0,
        metavar="N",
        help=f"exit code when nothing is new and {what} (default 0)",
    )


def check_nothing_new_option(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Refuse a code the OS cannot return (it keeps only the low byte): exit 2, before work."""
    if not 0 <= args.nothing_new_exit_code <= 255:
        parser.error(f"{NOTHING_NEW_OPTION} must be between 0 and 255")


def parse_extract_args(description: str, argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    add_nothing_new_option(parser, "no batch is written")
    args = parser.parse_args(argv)
    check_nothing_new_option(parser, args)
    return args


def exit_code(batch_dir: Path | None, nothing_new_exit_code: int) -> int:
    """0 when the run wrote a batch, otherwise the code asked for 'nothing new'."""
    return 0 if batch_dir is not None else nothing_new_exit_code
