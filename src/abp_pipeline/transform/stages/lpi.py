"""LPI (Land and Property Identifier) transformation stage."""

from __future__ import annotations

import duckdb


def prepare_street_descriptor_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create best street descriptor views (by language and any)."""
    # Best by language
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _sd_best_by_lang AS
        SELECT *
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn, sd.language
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
        )
        WHERE rn = 1
    """)

    # Best any language
    con.execute("""
        CREATE OR REPLACE TEMP VIEW _sd_best_any AS
        SELECT *
        FROM (
            SELECT sd.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY sd.usrn
                       ORDER BY
                         COALESCE(sd.end_date, DATE '9999-12-31') DESC,
                         COALESCE(sd.last_update_date, DATE '0001-01-01') DESC
                   ) AS rn
            FROM street_descriptor sd
        )
        WHERE rn = 1
    """)


def prepare_lpi_base(con: duckdb.DuckDBPyConnection) -> None:
    """Create materialised LPI base tables with address components."""
    con.execute("DROP TABLE IF EXISTS lpi_base_full")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_base_full AS
        SELECT
            l.uprn,
            l.lpi_key,
            l.language,
            l.logical_status,
            l.official_flag,
            l.start_date,
            l.end_date,
            l.last_update_date,
            b.postcode_locator AS postcode,
            b.blpu_state,
            b.addressbase_postal AS postal_address_code,
            b.parent_uprn,
            CASE
                WHEN b.parent_uprn IS NOT NULL THEN 'C'
                WHEN EXISTS (SELECT 1 FROM blpu b2 WHERE b2.parent_uprn = l.uprn) THEN 'P'
                ELSE 'S'
            END AS hierarchy_level,
            l.level,
            COALESCE(sd_lang.street_description, sd_any.street_description) AS street_description,
            COALESCE(sd_lang.locality, sd_any.locality) AS locality_name,
            COALESCE(sd_lang.town_name, sd_any.town_name) AS town_name,
            build_base_address(
                l.sao_text, l.sao_start_number, l.sao_start_suffix, l.sao_end_number, l.sao_end_suffix,
                l.pao_text, l.pao_start_number, l.pao_start_suffix, l.pao_end_number, l.pao_end_suffix,
                COALESCE(sd_lang.street_description, sd_any.street_description),
                COALESCE(sd_lang.locality, sd_any.locality),
                COALESCE(sd_lang.town_name, sd_any.town_name),
                b.postcode_locator
            ) AS base_address,
            CASE l.logical_status
                WHEN 1 THEN 0
                WHEN 3 THEN 1
                WHEN 6 THEN 2
                WHEN 8 THEN 3
                ELSE 9
            END AS status_rank
        FROM lpi l
        JOIN blpu b ON b.uprn = l.uprn
        LEFT JOIN _sd_best_by_lang sd_lang ON sd_lang.usrn = l.usrn AND sd_lang.language = l.language
        LEFT JOIN _sd_best_any sd_any ON sd_any.usrn = l.usrn
        WHERE (b.addressbase_postal != 'N' OR b.addressbase_postal IS NULL)
          AND l.logical_status IN (1, 3, 6, 8)
    """)

    # Deduplicated distinct addresses
    con.execute("DROP TABLE IF EXISTS lpi_base_distinct")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_base_distinct AS
        SELECT DISTINCT
            uprn,
            base_address,
            postcode,
            logical_status,
            official_flag,
            blpu_state,
            postal_address_code,
            parent_uprn,
            hierarchy_level,
            start_date,
            end_date,
            last_update_date,
            status_rank
        FROM lpi_base_full
        WHERE base_address IS NOT NULL AND base_address <> ''
    """)

    # Best current LPI per UPRN
    con.execute("DROP TABLE IF EXISTS lpi_best_current")
    con.execute("""
        CREATE TEMPORARY TABLE lpi_best_current AS
        SELECT *
        FROM (
            SELECT
                uprn,
                base_address,
                postcode,
                logical_status,
                official_flag,
                blpu_state,
                postal_address_code,
                parent_uprn,
                hierarchy_level,
                status_rank,
                last_update_date,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY status_rank, COALESCE(last_update_date, DATE '0001-01-01') DESC
                ) AS rn
            FROM lpi_base_distinct
            WHERE logical_status IN (1, 3, 6)
        )
        WHERE rn = 1
    """)


def render_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create LPI-based address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_lpi_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_lpi_variants AS
        SELECT
            uprn,
            postcode,
            base_address AS raw_address,
            'LPI' AS source,
            logical_status,
            official_flag,
            blpu_state,
            postal_address_code,
            parent_uprn,
            hierarchy_level,
            CASE logical_status
                WHEN 1 THEN 'APPROVED'
                WHEN 3 THEN 'ALTERNATIVE'
                WHEN 6 THEN 'PROVISIONAL'
                WHEN 8 THEN 'HISTORICAL'
            END AS variant_label,
            (logical_status = 1) AS is_primary
        FROM lpi_base_distinct
    """)
