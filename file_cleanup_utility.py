# coding=utf-8
# !/usr/bin/env python

"""Delete camera files older than N days from approved network shares."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from timeit import default_timer as timer

# ----------------------------
# Configuration
# ----------------------------

DAYS_TO_KEEP = 10

ROOTS = (
    r"\\tn-san-fileserv\TOOLING\2794 ACM Screw Head Vision\Camera\Screw Images\Bad",
    r"\\tn-san-fileserv\TOOLING\2874 PREFORM CLAMP AUTOMATION\Vision\Lok Images\Bad",
    r"\\tn-san-fileserv\TOOLING\2874 PREFORM CLAMP AUTOMATION\Vision\Lok Images\Good",
)

LOG_FILE = r"c:\logs\cleanup.log"
DELETE_EMPTY_DIRECTORIES = False  # safer default


# ----------------------------
# Logging
# ----------------------------

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("cleanup")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    log_dir = Path(LOG_FILE).parent
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Fall back to console-only logging if the log folder is unavailable.
        pass

    formatter = logging.Formatter(
        "%(asctime)s : %(levelname)s : %(name)s : %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as exc:
        logger.warning("Could not open log file %s: %s", LOG_FILE, exc)

    return logger


logger = setup_logging()

FILES_REMOVED = 0
FOLDERS_REMOVED = 0
FILES_SCANNED = 0
ERRORS = 0


# ----------------------------
# Helpers
# ----------------------------

def remove_path(path: Path) -> bool:
    """Remove a file or an empty directory. Return True on success."""
    global FILES_REMOVED, FOLDERS_REMOVED, ERRORS

    try:
        if path.is_dir():
            path.rmdir()
            FOLDERS_REMOVED += 1
        else:
            path.unlink(missing_ok=True)
            FILES_REMOVED += 1
        return True
    except FileNotFoundError:
        logger.warning("Path vanished before deletion: %s", path)
    except OSError as exc:
        logger.error("Unable to remove %s: %s", path, exc)

    ERRORS += 1
    return False


def cleanup(number_of_days: int, root_path: str) -> None:
    """
    Remove files older than or equal to number_of_days in root_path.

    Uses safe traversal and tolerates transient network/share errors.
    """
    global FILES_SCANNED, ERRORS

    root = Path(root_path)
    if not root.exists():
        logger.warning("Root path does not exist: %s", root)
        ERRORS += 1
        return

    cutoff = time.time() - (number_of_days * 24 * 60 * 60)

    for dirpath, dirnames, filenames in os.walk(root, topdown=False,
                                                onerror=lambda e: logger.error("Walk error: %s", e)):
        current_dir = Path(dirpath)

        for filename in filenames:
            file_path = current_dir / filename
            FILES_SCANNED += 1

            try:
                stat = file_path.stat()
            except FileNotFoundError:
                logger.warning("File disappeared before stat: %s", file_path)
                ERRORS += 1
                continue
            except OSError as exc:
                logger.error("Could not stat file %s: %s", file_path, exc)
                ERRORS += 1
                continue

            if stat.st_mtime <= cutoff:
                logger.info("Removing file: %s", file_path)
                remove_path(file_path)

        # Safer default: do not delete empty directories unless explicitly enabled.
        if DELETE_EMPTY_DIRECTORIES:
            try:
                if current_dir != root and not any(current_dir.iterdir()):
                    logger.info("Removing empty folder: %s", current_dir)
                    remove_path(current_dir)
            except OSError as exc:
                logger.error("Could not inspect folder %s: %s", current_dir, exc)
                ERRORS += 1

    logger.info("Files scanned = %d", FILES_SCANNED)


def main() -> int:
    """Main entry point."""
    start = timer()
    logger.info("Program started")

    for item in ROOTS:
        logger.info("Checking: %s", item)
        try:
            cleanup(DAYS_TO_KEEP, item)
        except Exception as exc:
            logger.exception("Unexpected failure while cleaning %s: %s", item, exc)

    logger.info("Files deleted = %d", FILES_REMOVED)
    logger.info("Folders deleted = %d", FOLDERS_REMOVED)
    logger.info("Errors = %d", ERRORS)
    logger.info("File cleanup complete in %.3f sec", timer() - start)

    return 0 if ERRORS == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
