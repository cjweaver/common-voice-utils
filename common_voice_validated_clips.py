"""
Common Voice Symlink Creator

A utility script to create symbolic links to the validated wav files (this assumes the original mp3 files have been converted to wav)
from Common Voice datasets.

This script processes the validated.tsv files found in Common Voice datasets to create
symlinks to the corresponding wav files in the clips directory.

Features:
- Processes single or multiple nested Common Voice datasets
- Creates organized symlinks in destination directories
- Logs operations and errors to a log file
- Handles large CSV files and various error conditions

Usage:
    python common_voice_validated_clips.py <top_level_common_voice_dir> <destination_root>

Where:
    <top_level_common_voice_dir>: Path to directory containing Common Voice dataset(s)
    <destination_root>: Path where symlinks will be created

Example:
    python common_voice_validated_clips.py /data/common_voice /symlinks/common_voice

"""

import csv
import os
import sys
from pathlib import Path


def get_log_file_path() -> Path:
    """Returns a path to the log file in the script's directory.

    Returns:
        Path: Path to the log file 'create_symlinks.log'
    """
    script_dir = Path(__file__).resolve().parent
    return script_dir / "create_symlinks.log"


def log_message(msg: str) -> None:
    """Appends a message to the log file.

    Args:
        msg: The message to append to the log file
    """
    log_file = get_log_file_path()
    with open(log_file, "a", encoding="utf-8") as logf:
        logf.write(f"{msg}\n")


def create_symlinks_for_common_voice(common_voice_dir: str, destination_dir: str) -> None:
    """Creates symlinks for wav files from a Common Voice dataset.

    The function processes the validated.tsv file in the given Common Voice
    directory, identifies corresponding wav files, and creates symlinks in
    the destination directory.

    Args:
        common_voice_dir: Path to the Common Voice dataset directory
        destination_dir: Path where symlinks will be created
    """
    # Raise the CSV field size limit (some large integer or sys.maxsize)
    # I had issues processing some of the very large CSV files.
    csv.field_size_limit(sys.maxsize)

    common_voice_path = Path(common_voice_dir)
    destination = Path(destination_dir)
    destination.mkdir(parents=True, exist_ok=True)

    tsv_file = (
        common_voice_path / "validated.tsv"
    )  # Change this if filename changes in future Common Voice releases.
    if not tsv_file.is_file():
        print(f"No 'validated.tsv' found in {common_voice_path}. Skipping...")
        log_message(f"No 'validated.tsv' found in {common_voice_path}. Skipping...")
        return

    clips_folder = common_voice_path / "clips"
    if not clips_folder.is_dir():
        print(f"No 'clips' directory found in {common_voice_path}. Skipping...")
        log_message(f"No 'clips' directory found in {common_voice_path}. Skipping...")
        return

    if not any(clips_folder.iterdir()):
        msg = f"'clips' directory in {common_voice_path} is empty. No files to symlink."
        print(msg)
        log_message(msg)
        return

    with open(tsv_file, "r", encoding="utf-8") as f:
        try:
            reader = csv.DictReader(f, delimiter="\t")
            # Enumerate lines, starting after the header = line 2
            for line_idx, row in enumerate(reader, start=2):
                if "path" not in row:
                    continue

                mp3_filename = row["path"]
                wav_filename = mp3_filename.replace(
                    ".mp3", ".wav"
                )  # The original files and thus those in the CSV are .mp3
                wav_file_path = clips_folder / wav_filename

                if wav_file_path.exists():
                    symlink_path = destination / wav_filename
                    if symlink_path.exists() or symlink_path.is_symlink():
                        symlink_path.unlink()
                    os.symlink(wav_file_path, symlink_path)
                else:
                    warning_msg = f"Warning: {wav_file_path} does not exist."
                    print(warning_msg)
                    log_message(warning_msg)

        except csv.Error as e:
            error_str = (
                f"CSV error on line {line_idx} of '{tsv_file}': {e}\n"
                f"You may want to inspect this line in a text editor, or use a manual parse."
            )
            print(error_str)
            log_message(error_str)
            return

    complete_msg = f"Symlinks created for dataset in {common_voice_path}."
    print(complete_msg)
    log_message(complete_msg)


def create_symlinks_for_all_common_voice(top_level_dir: str, destination_root: str) -> None:
    """Recursively process multiple Common Voice datasets.

    Scans the top-level directory for Common Voice datasets (identified by
    the presence of validated.tsv files) and creates symlinks for each.

    Args:
        top_level_dir: Directory containing one or more Common Voice datasets
        destination_root: Base directory where symlinks will be organized
    """
    top_dir = Path(top_level_dir)
    dest_root = Path(destination_root)
    dest_root.mkdir(parents=True, exist_ok=True)

    # Find every validated.tsv in the subfolders
    for tsv_file in top_dir.rglob("validated.tsv"):
        dataset_dir = tsv_file.parent
        # Use the dataset directory name (parent folder) to name the destination subfolder
        language_subfolder = dataset_dir.name
        language_destination = dest_root / language_subfolder

        create_symlinks_for_common_voice(
            common_voice_dir=str(dataset_dir), destination_dir=str(language_destination)
        )


def main() -> None:
    """Process command line arguments and execute symlink creation."""
    if len(sys.argv) < 3:
        print("Usage: python script.py <top_level_common_voice_dir> <destination_root>")
        sys.exit(1)

    top_level_dir = sys.argv[1]
    destination_root = sys.argv[2]

    create_symlinks_for_all_common_voice(top_level_dir, destination_root)


if __name__ == "__main__":
    main()
