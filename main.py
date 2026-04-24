"""
Main entry point — runs the bot. No CLI commands, no flags, no sub-commands.

Usage:
    python main.py
"""

import sys

from bot.cli import run


def main() -> None:
    run()


if __name__ == "__main__":
    main()
