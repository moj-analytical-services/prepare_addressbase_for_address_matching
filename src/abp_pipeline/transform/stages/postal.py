"""Postal (Royal Mail Delivery Point) transformation stage."""

from __future__ import annotations

import duckdb


def prepare_best_delivery(con: duckdb.DuckDBPyConnection) -> None:
    """Create best delivery point per UPRN."""
    con.execute("DROP TABLE IF EXISTS delivery_point_best")
    con.execute("""
        CREATE TEMPORARY TABLE delivery_point_best AS
        SELECT *
        FROM (
            SELECT
                uprn,
                udprn,
                ROW_NUMBER() OVER (
                    PARTITION BY uprn
                    ORDER BY
                        COALESCE(end_date, DATE '9999-12-31') DESC,
                        COALESCE(last_update_date, DATE '0001-01-01') DESC
                ) AS rn
            FROM delivery_point
            WHERE udprn IS NOT NULL
        )
        WHERE rn = 1
    """)


def render_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create Royal Mail delivery point address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_delivery_point_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_delivery_point_variants AS
        WITH delivery_rendered AS (
            SELECT
                d.uprn,
                d.postcode AS postcode,
                TRIM(concat_ws(' ',
                    NULLIF(TRIM(concat_ws(' ',
                        d.department_name, d.organisation_name, d.sub_building_name,
                        d.building_name, d.building_number
                    )), ''),
                    NULLIF(d.dependent_thoroughfare, ''),
                    NULLIF(d.thoroughfare, ''),
                    NULLIF(d.double_dependent_locality, ''),
                    NULLIF(d.dependent_locality, ''),
                    NULLIF(d.post_town, ''),
                    NULLIF(d.postcode, '')
                )) AS raw_address
            FROM delivery_point d
            WHERE d.postcode IS NOT NULL
        )
        SELECT
            uprn,
            postcode,
            raw_address,
            'DELIVERY_POINT' AS source,
            CAST(NULL AS INTEGER) AS logical_status,
            CAST(NULL AS VARCHAR) AS official_flag,
            CAST(NULL AS VARCHAR) AS blpu_state,
            CAST(NULL AS VARCHAR) AS postal_address_code,
            CAST(NULL AS BIGINT) AS parent_uprn,
            CAST(NULL AS VARCHAR) AS hierarchy_level,
            'DELIVERY' AS variant_label,
            FALSE AS is_primary
        FROM delivery_rendered
        WHERE raw_address IS NOT NULL AND raw_address <> ''
    """)
