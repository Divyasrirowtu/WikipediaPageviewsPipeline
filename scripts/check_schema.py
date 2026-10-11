
from pathlib import Path

INPUT_FILE = Path("data/raw/sample_pageviews.txt")


def validate_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    total_records = 0

    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            fields = line.split()

            if len(fields) != 4:
                raise ValueError(
                    f"Line {line_number}: expected 4 fields, "
                    f"received {len(fields)}"
                )

            domain_code, page_title, views, response_size = fields

            if not domain_code:
                raise ValueError(
                    f"Line {line_number}: domain_code cannot be empty"
                )

            if not page_title:
                raise ValueError(
                    f"Line {line_number}: page_title cannot be empty"
                )

            try:
                view_count = int(views)
                size = int(response_size)
            except ValueError as error:
                raise ValueError(
                    f"Line {line_number}: views and response size "
                    "must be integers"
                ) from error

            if view_count < 0:
                raise ValueError(
                    f"Line {line_number}: view count cannot be negative"
                )

            if size < 0:
                raise ValueError(
                    f"Line {line_number}: response size cannot be negative"
                )

            total_records += 1

    if total_records == 0:
        raise ValueError("Input file contains no valid records")

    print("Schema validation successful!")
    print(f"Input file: {file_path}")
    print(f"Valid records: {total_records}")


if __name__ == "__main__":
    validate_file(INPUT_FILE)
