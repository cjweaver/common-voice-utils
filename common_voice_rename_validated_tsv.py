"""
Common Voice TSV Renamer

A script to recursively find all "validated.tsv" files under a "common_voice"
directory, parse the language name and code from the path, rename them to
<LANGUAGE_NAME>_<LANGUAGE_CODE>_validated.tsv, and copy them to a specified
destination directory.

Example directory structure:
    common_voice/
      ├── Abkhaz/
      │    └── cv-corpus-21.0-2025-03-14-ab.tar/
      │         └── cv-corpus-21.0-2025-03-14/
      │              └── ab/
      │                   └── validated.tsv
      └── Icelandic/
           └── cv-corpus-21.0-2025-03-14-is.tar/
                └── cv-corpus-21.0-2025-03-14/
                     └── is/
                          └── validated.tsv

Usage:
    python common_voice_rename_validated_tsv.py --source-dir /path/to/common_voice \
                               --dest-dir /path/to/renamed_tsvs
"""

import argparse
import logging
import shutil
from pathlib import Path
from typing import List


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure logging for the script.

    Args:
        level: The logging level to use. Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger("tsv_renamer")
    logger.setLevel(level)

    # Create console handler with formatting
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


logger = setup_logging()


def process_validated_tsvs(source_dir: Path, dest_dir: Path) -> None:
    """Recursively find 'validated.tsv' in source_dir, rename, and copy to dest_dir.

    Files are renamed to <LANGUAGE_NAME>_<LANGUAGE_CODE>_validated.tsv, where:
      - LANGUAGE_NAME is assumed to be the first directory under source_dir
      - LANGUAGE_CODE is assumed to be the directory immediately above validated.tsv

    Args:
        source_dir: The root path containing language subdirectories.
        dest_dir: The path where the renamed TSV files will be copied.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Searching for validated.tsv files in {source_dir}")

    # Track all validated.tsv files found
    found_files: List[Path] = list(source_dir.rglob("validated.tsv"))

    if not found_files:
        logger.warning(f"No validated.tsv files found in {source_dir}")
        return

    logger.info(f"Found {len(found_files)} validated.tsv files")

    # Process each validated.tsv file
    for file_path in found_files:
        try:
            # Build a relative path from source_dir to the file
            rel_path = file_path.relative_to(source_dir)
            # Example: Abkhaz/cv-corpus-21.0-.../cv-corpus-21.0-.../ab/validated.tsv
            path_parts = rel_path.parts

            # LANGUAGE_NAME is the first directory under source_dir
            language_name = path_parts[0]
            # LANGUAGE_CODE is the directory just above 'validated.tsv'
            language_code = path_parts[-2]

            # Construct the new filename
            new_filename = f"{language_name}_{language_code}_validated.tsv"
            destination_path = dest_dir / new_filename

            # Check for duplicate names before copying
            if destination_path.exists():
                logger.warning(
                    f"Destination file {destination_path} already exists. " f"Skipping {file_path}"
                )
                continue

            # Copy the file
            shutil.copyfile(file_path, destination_path)
            logger.info(f"Copied {file_path} -> {destination_path}")

        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def parse_cmd_line_args():
    """Parse the command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments containing the base directory path,
        database path, and optional processing batch size.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Recursively find 'validated.tsv' in a Common Voice directory, "
            "rename them using <LANGUAGE_NAME>_<LANGUAGE_CODE>, "
            "and copy them to a destination directory."
        )
    )
    parser.add_argument(
        "--source-dir",
        "-s",
        required=True,
        type=Path,
        help="Path to the 'common_voice' directory that contains language subdirectories.",
    )
    parser.add_argument(
        "--dest-dir",
        "-d",
        required=True,
        type=Path,
        help="Path to the output directory where renamed TSV files will be placed.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def main() -> None:
    """Parse command-line arguments and process TSV files."""

    args = parse_cmd_line_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Starting Common Voice TSV renaming process")

    if not args.source_dir.exists():
        logger.error(f"Source directory {args.source_dir} does not exist")
        return

    process_validated_tsvs(args.source_dir, args.dest_dir)

    logger.info("TSV renaming process complete")


if __name__ == "__main__":
    main()
