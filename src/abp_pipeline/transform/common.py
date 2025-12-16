"""Shared utilities for ABP transformation."""

from __future__ import annotations

from pathlib import Path

import duckdb


class ToFlatfileError(Exception):
    """Error during flatfile transformation."""


def assert_inputs_exist(parquet_dir: Path) -> None:
    """Check that required input parquet files exist.

    Args:
        parquet_dir: Directory containing raw parquet files.

    Raises:
        FileNotFoundError: If required files are missing.
    """
    required = [
        "blpu",
        "lpi",
        "street_descriptor",
        "organisation",
        "delivery_point",
        "classification",
    ]
    missing = [name for name in required if not (parquet_dir / f"{name}.parquet").exists()]

    if missing:
        raise FileNotFoundError(
            f"Missing required parquet files: {missing}. Run --step split first."
        )


def register_parquet_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    where_condition: str | None = None,
) -> None:
    """Register a parquet-backed view with optional WHERE predicate."""
    sql = f"SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
    if where_condition:
        sql += f" WHERE {where_condition}"
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")


def create_macros(con: duckdb.DuckDBPyConnection) -> None:
    """Create reusable SQL macros for address rendering."""
    # Build address component (SAO or PAO)
    con.execute("""
        CREATE OR REPLACE MACRO build_component(
            comp_text, comp_start_number, comp_start_suffix, comp_end_number, comp_end_suffix
        ) AS
        TRIM(concat_ws(' ',
            NULLIF(comp_text, ''),
            CASE
                WHEN comp_start_number IS NOT NULL AND comp_end_number IS NULL
                    THEN concat(comp_start_number, COALESCE(comp_start_suffix, ''))
            END,
            CASE
                WHEN comp_start_number IS NOT NULL AND comp_end_number IS NOT NULL
                    THEN concat(
                        comp_start_number, COALESCE(comp_start_suffix, ''), '-',
                        comp_end_number, COALESCE(comp_end_suffix, '')
                    )
            END
        ))
    """)

    # Build full base address
    con.execute("""
        CREATE OR REPLACE MACRO build_base_address(
            sao_text, sao_start_number, sao_start_suffix, sao_end_number, sao_end_suffix,
            pao_text, pao_start_number, pao_start_suffix, pao_end_number, pao_end_suffix,
            street_description, locality_name, town_name, postcode
        ) AS
        TRIM(concat_ws(' ',
            NULLIF(TRIM(concat_ws(' ',
                build_component(sao_text, sao_start_number, sao_start_suffix, sao_end_number, sao_end_suffix),
                build_component(pao_text, pao_start_number, pao_start_suffix, pao_end_number, pao_end_suffix)
            )), ''),
            NULLIF(street_description, ''),
            NULLIF(locality_name, ''),
            NULLIF(town_name, ''),
            NULLIF(postcode, '')
        ))
    """)
