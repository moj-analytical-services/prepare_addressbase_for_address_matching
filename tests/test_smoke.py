"""Smoke tests for ABP Pipeline.

These tests run the pipeline on a tiny synthetic ABP sample to verify
the basic functionality without downloading real data.
"""

from __future__ import annotations

import tempfile
import time
from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from abp_pipeline.settings import OSDownloadSettings, PathSettings, ProcessingSettings, Settings
from abp_pipeline.split_raw import split_raw_to_parquet
from abp_pipeline.to_flatfile import transform_to_flatfile

# Path to sample data
SAMPLE_DATA_DIR = Path(__file__).parent / "data"
SAMPLE_CSV = SAMPLE_DATA_DIR / "sample_abp_lines.csv"


@pytest.fixture
def temp_settings() -> Generator[Settings, None, None]:
    """Create settings pointing to a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create the test input directory and copy sample data
        input_dir = tmpdir_path / "input"
        input_dir.mkdir()

        # Copy sample CSV to input directory
        (input_dir / "sample.csv").write_text(SAMPLE_CSV.read_text())

        paths = PathSettings(
            work_dir=tmpdir_path,
            downloads_dir=tmpdir_path / "downloads",
            extracted_dir=input_dir,  # Point to our input dir
            parquet_dir=tmpdir_path / "parquet",
            output_dir=tmpdir_path / "output",
        )

        # Dummy OS download settings (not used in smoke test)
        os_downloads = OSDownloadSettings(
            package_id="test",
            version_id="test",
            api_key="test",
            api_secret=None,
        )

        processing = ProcessingSettings(
            parquet_compression="zstd",
            parquet_compression_level=1,  # Faster for tests
        )

        settings = Settings(
            paths=paths,
            os_downloads=os_downloads,
            processing=processing,
            config_path=tmpdir_path / "config.yaml",
        )

        yield settings


class TestSplitRaw:
    """Tests for the split_raw module."""

    def test_split_creates_parquet_files(self, temp_settings: Settings) -> None:
        """Test that split creates parquet files for each record type."""
        # Run split
        output_paths = split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Check that expected files exist
        assert "blpu" in output_paths
        assert "lpi" in output_paths
        assert "street_descriptor" in output_paths
        assert output_paths["blpu"].exists()
        assert output_paths["lpi"].exists()
        assert output_paths["street_descriptor"].exists()

    def test_split_parquet_has_correct_columns(self, temp_settings: Settings) -> None:
        """Test that split parquet files have correct columns."""
        output_paths = split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Check BLPU columns using DuckDB without pandas
        con = duckdb.connect()
        result = con.execute(
            f"SELECT * FROM read_parquet('{output_paths['blpu'].as_posix()}') LIMIT 1"
        ).description

        column_names = [col[0] for col in result]
        assert "uprn" in column_names
        assert "postcode_locator" in column_names

        # Check row count
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_paths['blpu'].as_posix()}')"
        ).fetchone()[0]
        assert count == 3  # 3 BLPU records in sample

    def test_split_idempotent(self, temp_settings: Settings) -> None:
        """Test that split is idempotent (skips if outputs exist)."""
        # First run
        output_paths1 = split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Wait a bit to ensure different timestamps if re-written
        time.sleep(0.1)

        # Record which files have data (exist and non-empty)
        files_with_data = {
            name: path for name, path in output_paths1.items()
            if path.exists() and path.stat().st_size > 0
        }

        # Get modification times for files with data
        mtimes1 = {name: path.stat().st_mtime for name, path in files_with_data.items()}

        # Second run without force should skip
        output_paths2 = split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=False,
        )

        # Files should not be modified
        mtimes2 = {name: output_paths2[name].stat().st_mtime for name in files_with_data}
        assert mtimes1 == mtimes2, "Files should not be modified when force=False"


class TestFlatfile:
    """Tests for the to_flatfile module."""

    def test_flatfile_creates_output(self, temp_settings: Settings) -> None:
        """Test that flatfile transformation creates output parquet."""
        # First run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Then run flatfile
        output_path = transform_to_flatfile(temp_settings, force=True)

        assert output_path.exists()

    def test_flatfile_has_expected_columns(self, temp_settings: Settings) -> None:
        """Test that output has expected columns."""
        # First run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Then run flatfile
        output_path = transform_to_flatfile(temp_settings, force=True)

        # Check columns using DuckDB without pandas
        con = duckdb.connect()
        result = con.execute(
            f"SELECT * FROM read_parquet('{output_path.as_posix()}') LIMIT 1"
        ).description

        column_names = [col[0] for col in result]

        expected_columns = [
            "uprn",
            "postcode",
            "address_concat",
            "classification_code",
            "source",
            "variant_label",
            "is_primary",
        ]

        for col in expected_columns:
            assert col in column_names, f"Missing column: {col}"

    def test_flatfile_has_non_zero_rows(self, temp_settings: Settings) -> None:
        """Test that output has non-zero rows."""
        # First run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Then run flatfile
        output_path = transform_to_flatfile(temp_settings, force=True)

        # Check row count
        con = duckdb.connect()
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{output_path.as_posix()}')"
        ).fetchone()[0]

        assert count > 0, "Output should have non-zero rows"

    def test_flatfile_idempotent(self, temp_settings: Settings) -> None:
        """Test that flatfile is idempotent."""
        # First run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # First flatfile run
        output_path1 = transform_to_flatfile(temp_settings, force=True)

        # Wait a bit
        time.sleep(0.1)

        mtime1 = output_path1.stat().st_mtime

        # Second run without force should skip
        output_path2 = transform_to_flatfile(temp_settings, force=False)
        mtime2 = output_path2.stat().st_mtime

        assert mtime1 == mtime2
