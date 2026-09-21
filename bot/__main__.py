"""Point d'entree : "python -m bot"."""

from __future__ import annotations

import asyncio
import sys

from .main import main

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
