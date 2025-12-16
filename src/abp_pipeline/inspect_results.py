"""Inspect ABP flatfile results.

Utility functions for exploring and validating the generated address matching
flatfile, including summary statistics and random sampling.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from abp_pipeline.settings import Settings

logger = logging.getLogger(__name__)


def _get_flatfile_paths(settings: Settings) -> list[Path]:
    """Get the paths to all flatfile parquet chunks.

    Returns:
        List of paths to chunk files matching the naming pattern.
        Handles both single-chunk and multi-chunk outputs.
    """
    output_dir = settings.paths.output_dir
    # Find all files matching the chunk naming pattern
    chunk_files = sorted(output_dir.glob("abp_for_uk_address_matcher.chunk_*.parquet"))
    return chunk_files


def _get_flatfile_glob_pattern(settings: Settings) -> str:
    """Get the glob pattern for flatfile parquet chunks.

    Returns:
        Glob pattern string like 'dir/abp_for_uk_address_matcher.chunk_*.parquet'
    """
    output_dir = settings.paths.output_dir
    return (output_dir / "abp_for_uk_address_matcher.chunk_*.parquet").as_posix()


def _assert_flatfile_exists(settings: Settings) -> None:
    """Check that at least one flatfile chunk exists."""
    chunk_files = _get_flatfile_paths(settings)
    if not chunk_files:
        raise FileNotFoundError(
            f"No flatfile chunks found in {settings.paths.output_dir}. Run the flatfile step first."
        )


def get_variant_statistics(con: duckdb.DuckDBPyConnection, settings: Settings) -> dict:
    """Show summary statistics of address variants per UPRN.

    Calculates and displays the mean and median number of address variants
    per UPRN in the flatfile.

    Args:
        con: DuckDB connection.
        settings: Application settings.

    Returns:
        Dictionary with statistics: total_uprns, total_variants, mean_variants, median_variants.
    """
    _assert_flatfile_exists(settings)
    flatfile_pattern = _get_flatfile_glob_pattern(settings)

    stats = con.sql(f"""
        WITH variant_counts AS (
            SELECT uprn, COUNT(*) AS variant_count
            FROM read_parquet('{flatfile_pattern}')
            GROUP BY uprn
        )
        SELECT
            COUNT(*) AS total_uprns,
            SUM(variant_count) AS total_variants,
            AVG(variant_count) AS mean_variants,
            MEDIAN(variant_count) AS median_variants,
            MIN(variant_count) AS min_variants,
            MAX(variant_count) AS max_variants
        FROM variant_counts
    """).fetchone()

    result = {
        "total_uprns": stats[0],
        "total_variants": stats[1],
        "mean_variants": round(stats[2], 2),
        "median_variants": stats[3],
        "min_variants": stats[4],
        "max_variants": stats[5],
    }

    return result


def get_random_uprn(con: duckdb.DuckDBPyConnection, settings: Settings) -> duckdb.DuckDBPyRelation:
    """Show a random UPRN and all its address variants.

    Selects a random UPRN from the flatfile and displays all associated
    address variants.

    Args:
        con: DuckDB connection.
        settings: Application settings.

    Returns:
        DuckDB relation containing the variants for the selected UPRN.
    """
    _assert_flatfile_exists(settings)
    flatfile_pattern = _get_flatfile_glob_pattern(settings)

    # Get a random UPRN
    random_uprn = con.sql(f"""
        SELECT DISTINCT uprn
        FROM read_parquet('{flatfile_pattern}')
        ORDER BY RANDOM()
        LIMIT 1
    """).fetchone()[0]

    logger.info("Selected random UPRN: %s", random_uprn)

    # Get all variants for this UPRN
    result = con.sql(f"""
        SELECT
            uprn,
            address_concat,
            postcode,
            source,
            variant_label,
            is_primary,
            classification_code,
            udprn
        FROM read_parquet('{flatfile_pattern}')
        WHERE uprn = {random_uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    return result


def get_random_large_uprn(
    con: duckdb.DuckDBPyConnection, settings: Settings, top_n: int = 100
) -> duckdb.DuckDBPyRelation:
    """Show variants for a randomly selected UPRN from the top N largest.

    Identifies UPRNs with the most address variants, randomly selects one
    from the top N, and displays all its variants.

    Args:
        con: DuckDB connection.
        settings: Application settings.
        top_n: Number of largest UPRNs to consider (default 100).

    Returns:
        DuckDB relation containing the variants for the selected UPRN.
    """
    _assert_flatfile_exists(settings)
    flatfile_pattern = _get_flatfile_glob_pattern(settings)

    # Get a random UPRN from the top N largest
    selected = con.sql(f"""
        WITH variant_counts AS (
            SELECT uprn, COUNT(*) AS variant_count
            FROM read_parquet('{flatfile_pattern}')
            GROUP BY uprn
            ORDER BY variant_count DESC
            LIMIT {top_n}
        )
        SELECT uprn, variant_count
        FROM variant_counts
        ORDER BY RANDOM()
        LIMIT 1
    """).fetchone()

    random_uprn = selected[0]
    variant_count = selected[1]

    logger.info(
        "Selected UPRN %s from top %d largest (has %d variants)", random_uprn, top_n, variant_count
    )

    # Get all variants for this UPRN
    result = con.sql(f"""
        SELECT
            uprn,
            address_concat,
            postcode,
            source,
            variant_label,
            is_primary,
            classification_code,
            udprn
        FROM read_parquet('{flatfile_pattern}')
        WHERE uprn = {random_uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    return result


def get_uprn_variants(
    con: duckdb.DuckDBPyConnection, settings: Settings, uprn: int
) -> duckdb.DuckDBPyRelation:
    """Show all address variants for a specific UPRN.

    Args:
        con: DuckDB connection.
        settings: Application settings.
        uprn: The UPRN to look up.

    Returns:
        DuckDB relation containing all variants for the specified UPRN.
    """
    _assert_flatfile_exists(settings)
    flatfile_pattern = _get_flatfile_glob_pattern(settings)

    result = con.sql(f"""
        SELECT
            uprn,
            address_concat,
            postcode,
            source,
            variant_label,
            is_primary,
            classification_code,
            udprn
        FROM read_parquet('{flatfile_pattern}')
        WHERE uprn = {uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    row_count = result.count("*").fetchone()[0]
    if row_count == 0:
        logger.warning("No variants found for UPRN %s", uprn)
    else:
        logger.info("Found %d variants for UPRN %s", row_count, uprn)

    return result


def get_flatfile(con: duckdb.DuckDBPyConnection, settings: Settings) -> duckdb.DuckDBPyRelation:
    """Load the entire flatfile dataset as a DuckDB relation.

    Returns the full address matching flatfile as a DuckDB relation/dataframe
    for further analysis or querying. Automatically handles both single-chunk
    and multi-chunk outputs by reading all matching chunk files.

    Args:
        con: DuckDB connection.
        settings: Application settings.

    Returns:
        DuckDB relation containing the entire flatfile dataset.
    """
    _assert_flatfile_exists(settings)
    flatfile_pattern = _get_flatfile_glob_pattern(settings)

    return con.read_parquet(flatfile_pattern)
