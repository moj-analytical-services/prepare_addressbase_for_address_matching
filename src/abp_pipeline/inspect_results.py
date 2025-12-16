"""Inspect ABP flatfile results.

Utility functions for exploring and validating the generated address matching
flatfile, including summary statistics and random sampling.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from abp_pipeline.settings import Settings, create_duckdb_connection

logger = logging.getLogger(__name__)


def _get_flatfile_path(settings: Settings) -> Path:
    """Get the path to the flatfile parquet."""
    return settings.paths.output_dir / "abp_for_uk_address_matcher.parquet"


def _assert_flatfile_exists(flatfile_path: Path) -> None:
    """Check that the flatfile exists."""
    if not flatfile_path.exists():
        raise FileNotFoundError(
            f"Flatfile not found: {flatfile_path}. Run the flatfile step first."
        )


def get_variant_statistics(settings: Settings) -> dict:
    """Show summary statistics of address variants per UPRN.

    Calculates and displays the mean and median number of address variants
    per UPRN in the flatfile.

    Args:
        settings: Application settings.

    Returns:
        Dictionary with statistics: total_uprns, total_variants, mean_variants, median_variants.
    """
    flatfile_path = _get_flatfile_path(settings)
    _assert_flatfile_exists(flatfile_path)

    con = create_duckdb_connection(settings)
    stats = con.sql(f"""
        WITH variant_counts AS (
            SELECT uprn, COUNT(*) AS variant_count
            FROM read_parquet('{flatfile_path.as_posix()}')
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


def get_random_uprn(settings: Settings) -> duckdb.DuckDBPyRelation:
    """Show a random UPRN and all its address variants.

    Selects a random UPRN from the flatfile and displays all associated
    address variants.

    Args:
        settings: Application settings.

    Returns:
        DuckDB relation containing the variants for the selected UPRN.
    """
    flatfile_path = _get_flatfile_path(settings)
    _assert_flatfile_exists(flatfile_path)

    con = create_duckdb_connection(settings)

    # Get a random UPRN
    random_uprn = con.sql(f"""
        SELECT DISTINCT uprn
        FROM read_parquet('{flatfile_path.as_posix()}')
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
        FROM read_parquet('{flatfile_path.as_posix()}')
        WHERE uprn = {random_uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    return result


def get_random_large_uprn(settings: Settings, top_n: int = 100) -> duckdb.DuckDBPyRelation:
    """Show variants for a randomly selected UPRN from the top N largest.

    Identifies UPRNs with the most address variants, randomly selects one
    from the top N, and displays all its variants.

    Args:
        settings: Application settings.
        top_n: Number of largest UPRNs to consider (default 100).

    Returns:
        DuckDB relation containing the variants for the selected UPRN.
    """
    flatfile_path = _get_flatfile_path(settings)
    _assert_flatfile_exists(flatfile_path)

    con = create_duckdb_connection(settings)

    # Get a random UPRN from the top N largest
    selected = con.sql(f"""
        WITH variant_counts AS (
            SELECT uprn, COUNT(*) AS variant_count
            FROM read_parquet('{flatfile_path.as_posix()}')
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
        FROM read_parquet('{flatfile_path.as_posix()}')
        WHERE uprn = {random_uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    return result


def get_uprn_variants(settings: Settings, uprn: int) -> duckdb.DuckDBPyRelation:
    """Show all address variants for a specific UPRN.

    Args:
        settings: Application settings.
        uprn: The UPRN to look up.

    Returns:
        DuckDB relation containing all variants for the specified UPRN.
    """
    flatfile_path = _get_flatfile_path(settings)
    _assert_flatfile_exists(flatfile_path)

    con = create_duckdb_connection(settings)
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
        FROM read_parquet('{flatfile_path.as_posix()}')
        WHERE uprn = {uprn}
        ORDER BY is_primary DESC, source, variant_label
    """)

    row_count = result.count("*").fetchone()[0]
    if row_count == 0:
        logger.warning("No variants found for UPRN %s", uprn)
    else:
        logger.info("Found %d variants for UPRN %s", row_count, uprn)

    return result


def get_flatfile(settings: Settings) -> duckdb.DuckDBPyRelation:
    """Load the entire flatfile dataset as a DuckDB relation.

    Returns the full address matching flatfile as a DuckDB relation/dataframe
    for further analysis or querying.

    Args:
        settings: Application settings.

    Returns:
        DuckDB relation containing the entire flatfile dataset.
    """
    flatfile_path = _get_flatfile_path(settings)
    _assert_flatfile_exists(flatfile_path)

    con = create_duckdb_connection(settings)
    return con.read_parquet(flatfile_path.as_posix())
