
import gzip
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

PROJECT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
LOG_DIR = PROJECT_DIR / "logs"

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
MAX_HOURS_TO_CHECK = 48
REQUEST_TIMEOUT = 60

RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "ingestion.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)


def build_url(hour: datetime) -> str:
    """Build the URL for one UTC hourly Wikimedia pageview dump."""
    return (
        f"{BASE_URL}/{hour.year}/{hour.strftime('%Y-%m')}/"
        f"pageviews-{hour.strftime('%Y%m%d-%H')}0000.gz"
    )


def validate_gzip(file_path: Path) -> int:
    """Check gzip integrity and count non-empty records."""
    records = 0

    with gzip.open(file_path, "rt", encoding="utf-8", errors="strict") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            fields = line.split()

            if len(fields) != 4:
                raise ValueError(
                    f"Invalid record at line {line_number}: "
                    f"expected 4 fields, received {len(fields)}"
                )

            try:
                views = int(fields[2])
                response_size = int(fields[3])
            except ValueError as error:
                raise ValueError(
                    f"Invalid numeric value at line {line_number}"
                ) from error

            if views < 0 or response_size < 0:
                raise ValueError(
                    f"Negative numeric value at line {line_number}"
                )

            records += 1

    if records == 0:
        raise ValueError("Downloaded file contains no records")

    return records


def download_hour(session: requests.Session, hour: datetime):
    """Download one hourly dump, validate it, and save it to data/raw."""
    url = build_url(hour)
    filename = f"pageviews-{hour.strftime('%Y%m%d-%H')}0000.gz"
    final_path = RAW_DIR / filename
    temp_path = RAW_DIR / f"{filename}.part"

    if final_path.exists():
        try:
            records = validate_gzip(final_path)
            logger.info(
                "Already downloaded: %s (%s records)",
                filename,
                records,
            )
            return final_path, records, url
        except (OSError, EOFError, ValueError):
            logger.warning("Existing file is invalid; downloading again")
            final_path.unlink(missing_ok=True)

    logger.info("Trying Wikimedia dump: %s", url)

    try:
        with session.get(
            url,
            stream=True,
            timeout=REQUEST_TIMEOUT,
        ) as response:
            if response.status_code == 404:
                logger.info("Dump not published: %s", filename)
                return None

            response.raise_for_status()

            with temp_path.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)

        records = validate_gzip(temp_path)
        os.replace(temp_path, final_path)

        logger.info(
            "Downloaded and validated %s (%s records)",
            filename,
            records,
        )

        return final_path, records, url

    except (requests.RequestException, OSError, EOFError, ValueError) as error:
        logger.warning("Could not ingest %s: %s", filename, error)
        temp_path.unlink(missing_ok=True)
        return None


def main():
    now = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )

    # Recent dumps may not be published yet, so try earlier hours.
    with requests.Session() as session:
        session.headers.update(
            {"User-Agent": "WikipediaPageviewsPipeline/1.0 (educational project)"}
        )

        for offset in range(1, MAX_HOURS_TO_CHECK + 1):
            hour = now - timedelta(hours=offset)
            result = download_hour(session, hour)

            if result is None:
                time.sleep(0.2)
                continue

            file_path, records, url = result

            metadata = {
                "source": "Wikimedia Pageviews",
                "source_url": url,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "data_hour_utc": hour.isoformat(),
                "file_name": file_path.name,
                "record_count": records,
                "file_size_bytes": file_path.stat().st_size,
                "format": "gzip",
            }

            metadata_path = file_path.with_suffix(".metadata.json")
            metadata_path.write_text(
                json.dumps(metadata, indent=2),
                encoding="utf-8",
            )

            logger.info("Saved raw data: %s", file_path)
            logger.info("Saved metadata: %s", metadata_path)
            logger.info("Ingestion completed successfully")
            return

    raise RuntimeError(
        f"No valid hourly dump found in the last {MAX_HOURS_TO_CHECK} hours. "
        "Check your internet connection and try again later."
    )


if __name__ == "__main__":
    main()
