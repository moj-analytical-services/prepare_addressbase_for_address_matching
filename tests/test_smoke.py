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

from abp_pipeline.settings import (
    OSDownloadSettings,
    PathSettings,
    ProcessingSettings,
    Settings,
    create_duckdb_connection,
)
from abp_pipeline.split_raw import split_raw_to_parquet
from abp_pipeline.transform.runner import transform_to_flatfile

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
            num_chunks=1,  # Default single chunk
        )

        settings = Settings(
            paths=paths,
            os_downloads=os_downloads,
            processing=processing,
            config_path=tmpdir_path / "config.yaml",
        )

        yield settings


@pytest.fixture
def temp_settings_chunked() -> Generator[Settings, None, None]:
    """Create settings with num_chunks=2 for chunking tests."""
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
            extracted_dir=input_dir,
            parquet_dir=tmpdir_path / "parquet",
            output_dir=tmpdir_path / "output",
        )

        os_downloads = OSDownloadSettings(
            package_id="test",
            version_id="test",
            api_key="test",
            api_secret=None,
        )

        processing = ProcessingSettings(
            parquet_compression="zstd",
            parquet_compression_level=1,
            num_chunks=2,  # Two chunks for testing
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
        con = create_duckdb_connection(temp_settings)
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
        assert count == 1  # 1 BLPU record in official OS sample

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
            name: path
            for name, path in output_paths1.items()
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
        output_paths = transform_to_flatfile(temp_settings, force=True)

        assert len(output_paths) == 1
        assert output_paths[0].exists()

    def test_flatfile_has_expected_columns(self, temp_settings: Settings) -> None:
        """Test that output has expected columns."""
        # First run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Then run flatfile
        output_paths = transform_to_flatfile(temp_settings, force=True)
        output_path = output_paths[0]

        # Check columns using DuckDB without pandas
        con = create_duckdb_connection(temp_settings)
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
        output_paths = transform_to_flatfile(temp_settings, force=True)
        output_path = output_paths[0]

        # Check row count
        con = create_duckdb_connection(temp_settings)
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
        output_paths1 = transform_to_flatfile(temp_settings, force=True)
        output_path1 = output_paths1[0]

        # Wait a bit
        time.sleep(0.1)

        mtime1 = output_path1.stat().st_mtime

        # Second run without force should skip
        output_paths2 = transform_to_flatfile(temp_settings, force=False)
        output_path2 = output_paths2[0]
        mtime2 = output_path2.stat().st_mtime

        assert mtime1 == mtime2


class TestChunking:
    """Tests for UPRN-based chunking functionality."""

    def test_chunked_produces_multiple_files(self, temp_settings_chunked: Settings) -> None:
        """Test that num_chunks=2 produces 2 output files."""
        # First run split
        split_raw_to_parquet(
            temp_settings_chunked,
            input_dir=temp_settings_chunked.paths.extracted_dir,
            force=True,
        )

        # Run flatfile with chunking
        output_paths = transform_to_flatfile(temp_settings_chunked, force=True)

        assert len(output_paths) == 2
        for path in output_paths:
            assert path.exists()

        # Check naming convention
        assert "chunk_000_of_002" in output_paths[0].name
        assert "chunk_001_of_002" in output_paths[1].name

    def test_chunk_union_equals_baseline(self, temp_settings: Settings) -> None:
        """Test that union of chunked outputs equals single-chunk baseline."""
        # Run split
        split_raw_to_parquet(
            temp_settings,
            input_dir=temp_settings.paths.extracted_dir,
            force=True,
        )

        # Run baseline (single chunk)
        baseline_paths = transform_to_flatfile(temp_settings, force=True)
        baseline_path = baseline_paths[0]

        # Get baseline metrics
        con = create_duckdb_connection(temp_settings)
        baseline_stats = con.execute(f"""
            SELECT COUNT(DISTINCT uprn) AS uprn_count, COUNT(*) AS row_count
            FROM read_parquet('{baseline_path.as_posix()}')
        """).fetchone()
        baseline_uprns = baseline_stats[0]
        baseline_rows = baseline_stats[1]

        # Now run with 2 chunks
        temp_settings.processing.num_chunks = 2
        # Clear output directory
        import shutil

        shutil.rmtree(temp_settings.paths.output_dir, ignore_errors=True)

        chunked_paths = transform_to_flatfile(temp_settings, force=True)

        # Combine chunk outputs and compare
        chunk_files = ", ".join([f"'{p.as_posix()}'" for p in chunked_paths])
        chunked_stats = con.execute(f"""
            SELECT COUNT(DISTINCT uprn) AS uprn_count, COUNT(*) AS row_count
            FROM read_parquet([{chunk_files}])
        """).fetchone()
        chunked_uprns = chunked_stats[0]
        chunked_rows = chunked_stats[1]

        assert baseline_uprns == chunked_uprns, (
            f"UPRN count mismatch: baseline={baseline_uprns}, chunked={chunked_uprns}"
        )
        assert baseline_rows == chunked_rows, (
            f"Row count mismatch: baseline={baseline_rows}, chunked={chunked_rows}"
        )

    def test_chunks_are_disjoint(self, temp_settings_chunked: Settings) -> None:
        """Test that chunks have disjoint UPRN sets."""
        # Run split
        split_raw_to_parquet(
            temp_settings_chunked,
            input_dir=temp_settings_chunked.paths.extracted_dir,
            force=True,
        )

        # Run with chunking
        output_paths = transform_to_flatfile(temp_settings_chunked, force=True)

        if len(output_paths) < 2:
            pytest.skip("Not enough chunks to test disjointness")

        con = create_duckdb_connection(temp_settings_chunked)

        # Get UPRNs from each chunk
        chunk0_uprns = con.execute(f"""
            SELECT DISTINCT uprn FROM read_parquet('{output_paths[0].as_posix()}')
        """).fetchall()
        chunk1_uprns = con.execute(f"""
            SELECT DISTINCT uprn FROM read_parquet('{output_paths[1].as_posix()}')
        """).fetchall()

        chunk0_set = {r[0] for r in chunk0_uprns}
        chunk1_set = {r[0] for r in chunk1_uprns}

        intersection = chunk0_set & chunk1_set
        assert len(intersection) == 0, (
            f"Chunks should be disjoint, but found overlapping UPRNs: {intersection}"
        )
