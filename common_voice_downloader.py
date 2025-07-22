"""
Utility script to download each Common Voice language dataset via Chromedriver automation of
https://commonvoice.mozilla.org/en/datasets

This script automates the download process for Common Voice datasets by:
1. Scraping dataset URLs from the Common Voice website
2. Creating language-specific directories
3. Downloading datasets with resume capability (allows downloading on another machine)
4. Optionally extracting archives


This project uses webdriver-manager to handle Chromedriver installation automatically.
No manual Chromedriver download required!

Usage:
    python common_voice_downloader.py --download_path /path/to/store/datasets

Optional arguments:
    --file FILE          Path to a JSON file containing dataset entries (skip scraping and start downloading files)
    --warnsize           Warn about uncompressed .tar.gz size (gives an estimate of size of downloaded corpus)
    --untar              Automatically extract downloaded .tar.gz files
"""

import argparse
import concurrent.futures
import json
import re
import tarfile
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

DATASETS_URL: str = "https://commonvoice.mozilla.org/en/datasets"
COMMON_VOICE_VERSION: float = 21.0  # Version of Common Voice to download
EMAIL: str = "your@email.goes.here"


def parse_size(text: str) -> Tuple[float, Optional[str]]:
    """Parse a size string into numeric value and unit.

    Args:
        text: The text containing a size specification (e.g., '1.83 GB')

    Returns:
        A tuple containing the numeric size value and unit (if present)
    """
    pattern = r"(\d+(?:\.\d+)?)\s*(GB|MB|TB|KB)?"
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return 0.0, None
    size_value_str = match.group(1)
    size_unit = match.group(2)
    size_value = float(size_value_str)
    return size_value, size_unit.upper() if size_unit else None


def to_megabytes(size_value: float, size_unit: Optional[str]) -> float:
    """Convert a size value with unit to megabytes.

    Args:
        size_value: The numeric value of the size
        size_unit: The unit of the size (GB, MB, TB, KB)

    Returns:
        The size converted to megabytes
    """
    if size_unit == "GB":
        return size_value * 1024
    elif size_unit == "MB":
        return size_value
    elif size_unit == "TB":
        return size_value * 1024 * 1024
    elif size_unit == "KB":
        return size_value / 1024
    return size_value


def warn_uncompressed_size(download_dir: Path) -> None:
    """Scan all .tar.gz files and warn about total uncompressed size.

    Args:
        download_dir: Directory containing the downloaded archives
    """
    archives = list(download_dir.rglob("*.tar.gz"))
    total_uncompressed_bytes = 0

    print("Analyzing .tar.gz archives for uncompressed size...")
    for archive in tqdm(archives, desc="Analyzing archives", unit="archive"):
        try:
            with tarfile.open(archive, "r:gz") as tar:
                total_uncompressed_bytes += sum(
                    member.size for member in tar.getmembers()
                )
        except (tarfile.TarError, OSError) as e:
            print(f"Skipping {archive} due to read error: {e}")

    total_uncompressed_mb = total_uncompressed_bytes / (1024 * 1024)
    print(
        f"\nWARNING: Potential total uncompressed size is approximately "
        f"{total_uncompressed_mb:.2f} MB ({total_uncompressed_mb / 1024:.2f} GB)."
    )


def untar_datasets(download_dir: Path) -> None:
    """Extract all .tar.gz files under the download directory.

    Args:
        download_dir: Directory containing the downloaded archives
    """
    archives = list(download_dir.rglob("*.tar.gz"))

    print("Extracting .tar.gz archives...")
    for archive in tqdm(archives, desc="Archives", unit="archive"):
        extract_folder = archive.parent / archive.stem
        extract_folder.mkdir(parents=True, exist_ok=True)

        try:
            with tarfile.open(archive, "r:gz") as tar:
                members = tar.getmembers()
                for member in tqdm(
                    members, desc=f"Extracting {archive.name}", leave=False, unit="file"
                ):
                    tar.extract(member, path=extract_folder)

        except (tarfile.TarError, OSError) as e:
            print(f"Failed to extract {archive}: {e}")


def save_urls_json(
    dataset_url_and_filenames: Dict[str, Dict[str, str]],
    download_dir: Path,
    filename: str = "saved_urls.json",
) -> None:
    """Save dataset URLs to a JSON file.

    Args:
        dataset_url_and_filenames: Dictionary containing dataset information
        download_dir: Directory to save the JSON file
        filename: Name of the JSON file
    """
    json_save_path = download_dir / filename
    with json_save_path.open("w", encoding="utf-8") as file_ptr:
        json.dump(dataset_url_and_filenames, file_ptr)
    print(f"Dataset URLs saved to {json_save_path}")


def load_urls_json(file_path: Path) -> Dict[str, Dict[str, str]]:
    """Load dataset URL information from a JSON file.

    Args:
        file_path: Path to the JSON file

    Returns:
        Dictionary containing dataset information
    """
    with file_path.open("r", encoding="utf-8") as fptr:
        dataset_url_and_filenames = json.load(fptr)
    print(f"Dataset URLs read from {file_path}")
    return dataset_url_and_filenames


def get_datasets_to_download(download_dir: Path) -> Dict[str, Dict[str, str]]:
    """Scrape Common Voice website to collect dataset download links.

    Args:
        download_dir: Directory where the dataset information will be saved

    Returns:
        Dictionary with dataset download information
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(
        options=chrome_options, service=ChromeService(ChromeDriverManager().install())
    )
    wait = WebDriverWait(driver, 30)

    total_mb: float = 0.0
    dataset_url_and_filenames: Dict[str, Dict[str, str]] = {}
    print("Collecting dataset download URLs from Common Voice website...")

    try:
        driver.get(DATASETS_URL)
        select_element = wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "select[name='bundleLocale']")
            )
        )
        language_selector = Select(select_element)

        for option in language_selector.options:
            dataset_language_code = option.get_attribute("value")
            dataset_language = option.text
            print(f"Language: {dataset_language}")

            language_selector.select_by_value(dataset_language_code)
            table = wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "table.table.dataset-table.hidden-md-down")
                )
            )

            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                version_text = cells[0].text
                if version_text == f"Common Voice Corpus {COMMON_VOICE_VERSION}":
                    row.click()
                    break

            time.sleep(2)

            email_input = wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "input[name='email']")
                )
            )
            email_input.clear()
            email_input.send_keys(EMAIL)

            checkbox_size = wait.until(
                EC.element_to_be_clickable((By.NAME, "confirmSize"))
            )
            download_size_text = checkbox_size.accessible_name
            size_value, size_unit = parse_size(download_size_text)
            total_mb += to_megabytes(size_value, size_unit)
            if not checkbox_size.is_selected():
                checkbox_size.click()

            checkbox_no_identify = wait.until(
                EC.element_to_be_clickable((By.NAME, "confirmNoIdentify"))
            )
            if not checkbox_no_identify.is_selected():
                checkbox_no_identify.click()

            time.sleep(1)

            download_link_button = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a.download-language.button.rounded")
                )
            )
            dataset_url = download_link_button.get_attribute("href")

            parsed_url = urlparse(dataset_url)
            filename = Path(parsed_url.path).name

            dataset_url_and_filenames[dataset_language] = {
                "dataset_language": dataset_language,
                "href": dataset_url,
                "dataset_archive_filename": filename,
            }

    finally:
        driver.quit()

    print(
        f"Total size in MB (selected datasets): {total_mb:.2f} MB "
        f"({total_mb / 1024:.2f} GB)"
    )
    save_urls_json(dataset_url_and_filenames, download_dir)
    return dataset_url_and_filenames


def create_dataset_directories(
    dataset_url_and_filenames: Dict[str, Dict[str, str]], download_path: Path
) -> None:
    """Create directories for each language dataset and update file paths.

    Args:
        dataset_url_and_filenames: Dictionary with dataset information
        download_path: Base directory for dataset downloads
    """
    for dataset_language, dataset_dict in dataset_url_and_filenames.items():
        language_dir = download_path / dataset_dict["dataset_language"]
        language_dir.mkdir(parents=True, exist_ok=True)

        download_filepath = language_dir / dataset_dict["dataset_archive_filename"]
        dataset_url_and_filenames[dataset_language]["language_download_dir"] = str(
            language_dir
        )
        dataset_url_and_filenames[dataset_language]["download_filepath"] = str(
            download_filepath
        )


def _download_file(entry: Dict[str, str]) -> str:
    """Download a single file with progress bar, supporting resume.

    Args:
        entry: Dictionary containing download information with keys:
              - "href": The remote URL to download
              - "download_filepath": The local file path as a string

    Returns:
        The local file path where the file was saved
    """
    url = entry["href"]
    filepath = Path(entry["download_filepath"])

    # 1) HEAD request to get remote file size
    try:
        head_resp = requests.head(url, allow_redirects=True)
        head_resp.raise_for_status()
        remote_size = int(head_resp.headers.get("content-length", 0))
    except Exception as e:
        print(f"Error retrieving HEAD for {url}: {e}")
        remote_size = 0

    # 2) Check if the file is already complete
    if filepath.exists():
        local_size = filepath.stat().st_size
        if local_size == remote_size and remote_size > 0:
            print(
                f"{filepath} is already fully downloaded ({local_size} bytes). Skipping."
            )
            return str(filepath)
    else:
        local_size = 0

    # 3) Decide whether to attempt partial resume
    headers = {}
    mode = "wb"  # default to write from scratch

    if local_size > 0 and local_size < remote_size:
        # Attempt a partial download
        headers = {"Range": f"bytes={local_size}-"}
        mode = "ab"  # append mode

    # 4) Perform the actual GET request
    try:
        with requests.get(url, stream=True, headers=headers) as response:
            # If partial content is not supported, server might return 200 instead of 206
            if response.status_code in (200, 206):
                total_size_in_bytes = int(response.headers.get("content-length", 0))

                # If we got 206, total_size_in_bytes should be how many bytes remain
                # If 200, we assume the server doesn't support partial. Redownload everything
                if response.status_code == 200:
                    # Start from scratch
                    local_size = 0
                    mode = "wb"

                # TQDM progress bar
                # The total is remote_size if server 200, or the partial remainder if 206
                if response.status_code == 206:
                    total_download_bytes = local_size + total_size_in_bytes
                else:
                    total_download_bytes = total_size_in_bytes

                print(f"Downloading {filepath} (resume={response.status_code == 206})")
                with filepath.open(mode) as file_out, tqdm(
                    total=total_download_bytes,
                    initial=local_size if response.status_code == 206 else 0,
                    unit="B",
                    unit_scale=True,
                    desc=f"Downloading {filepath.name}",
                ) as progress_bar:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            file_out.write(chunk)
                            progress_bar.update(len(chunk))
            else:
                print(f"Unexpected status code {response.status_code} for {url}.")
    except Exception as exc:
        print(f"Error downloading {url}: {exc}")

    return str(filepath)


def download_files(
    dataset_url_and_filenames: Dict[str, Dict[str, str]], concurrency: int = 4
) -> None:
    """Download multiple files in parallel with resume capability.

    Args:
        dataset_url_and_filenames: Dictionary with dataset download information
        concurrency: Number of simultaneous downloads
    """
    entries = list(dataset_url_and_filenames.values())
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_entry = {
            executor.submit(_download_file, entry): entry for entry in entries
        }
        for future in concurrent.futures.as_completed(future_to_entry):
            entry = future_to_entry[future]
            try:
                result_path = future.result()
                print(f"Downloaded: {result_path}")
            except Exception as exc:
                print(f"Error downloading {entry['href']}: {exc}")


def parse_cmd_line_args():
    """Parse the command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments containing the base directory path,
        database path, and optional processing batch size.
    """

    parser = argparse.ArgumentParser(
        description="Common Voice Downloader with Resume Feature."
    )
    parser.add_argument(
        "--download_path",
        help="Path to store dataset archives.",
        required=True,
    )
    parser.add_argument(
        "--file",
        help="Path to a JSON file containing dataset entries (skip scraping and download files directly).",
    )
    parser.add_argument(
        "--warnsize",
        action="store_true",
        help="Warn about uncompressed .tar.gz size.",
    )
    parser.add_argument(
        "--untar",
        action="store_true",
        help="Automatically extract downloaded .tar.gz files.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the Common Voice dataset downloader."""

    args = parse_cmd_line_args()
    download_path = Path(args.download_path)
    download_path.mkdir(parents=True, exist_ok=True)

    if args.file:
        # Load from local JSON file
        file_path = Path(args.file)
        if not file_path.is_file():
            print(f"Error: {file_path} does not exist. Exiting.")
            return
        dataset_map = load_urls_json(file_path)
    else:
        # Scrape from the Common Voice site
        dataset_map = get_datasets_to_download(download_path)

    create_dataset_directories(dataset_map, download_path)

    download_files(dataset_map, concurrency=4)

    if args.warnsize:
        warn_uncompressed_size(download_path)

    if args.untar:
        untar_datasets(download_path)
        print("All archives extracted.")


if __name__ == "__main__":
    main()
