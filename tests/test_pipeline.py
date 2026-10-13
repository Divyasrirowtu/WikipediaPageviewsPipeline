
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = PROJECT_ROOT / "config" / "schema.yaml"
SAMPLE_FILE = PROJECT_ROOT / "data" / "raw" / "sample_pageviews.txt"


def validate_record(line):
    """Validate one whitespace-separated Wikimedia pageview record."""
    fields = line.split()

    if len(fields) != 4:
        return False

    domain_code, page_title, view_count, response_size = fields

    if not domain_code or not page_title:
        return False

    try:
        views = int(view_count)
        size = int(response_size)
    except ValueError:
        return False

    return views >= 0 and size >= 0


class TestPageviewsPipeline(unittest.TestCase):

    def test_schema_file_exists(self):
        self.assertTrue(
            SCHEMA_FILE.exists(),
            "config/schema.yaml must exist"
        )

    def test_sample_file_exists(self):
        self.assertTrue(
            SAMPLE_FILE.exists(),
            "data/raw/sample_pageviews.txt must exist"
        )

    def test_schema_contains_expected_fields(self):
        schema_text = SCHEMA_FILE.read_text(encoding="utf-8")

        for field in (
            "domain_code",
            "page_title",
            "view_count",
            "response_size",
        ):
            with self.subTest(field=field):
                self.assertIn(field, schema_text)

    def test_sample_records_are_valid(self):
        lines = SAMPLE_FILE.read_text(
            encoding="utf-8"
        ).splitlines()

        records = [line for line in lines if line.strip()]

        self.assertGreater(
            len(records), 0,
            "The sample file should contain records"
        )

        for line_number, line in enumerate(records, start=1):
            with self.subTest(line_number=line_number):
                self.assertTrue(
                    validate_record(line),
                    f"Invalid sample record: {line}"
                )

    def test_valid_record(self):
        self.assertTrue(
            validate_record("en Main_Page 1200 532000")
        )

    def test_record_with_missing_field_is_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page 1200")
        )

    def test_record_with_extra_field_is_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page 1200 532000 extra")
        )

    def test_non_numeric_views_are_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page abc 532000")
        )

    def test_non_numeric_response_size_is_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page 1200 xyz")
        )

    def test_negative_views_are_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page -1 532000")
        )

    def test_negative_response_size_is_rejected(self):
        self.assertFalse(
            validate_record("en Main_Page 1200 -10")
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
