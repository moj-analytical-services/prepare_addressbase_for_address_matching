"""Business (Organisation) name transformation stage."""

from __future__ import annotations

import duckdb


def render_variants(con: duckdb.DuckDBPyConnection) -> None:
    """Create organisation/business name address variants."""
    con.execute("DROP TABLE IF EXISTS _stage_business_variants")
    con.execute("""
        CREATE TEMPORARY TABLE _stage_business_variants AS
        WITH organisation_clean AS (
            SELECT
                uprn,
                TRIM(organisation) AS organisation_name,
                TRIM(legal_name) AS legal_name,
                start_date,
                end_date
            FROM organisation
        ),
        organisation_candidates AS (
            SELECT uprn, 'ORGANISATION' AS name_source, organisation_name AS name_value, start_date, end_date
            FROM organisation_clean
            WHERE organisation_name IS NOT NULL AND organisation_name <> ''
            UNION ALL
            SELECT uprn, 'LEGAL_NAME' AS name_source, legal_name AS name_value, start_date, end_date
            FROM organisation_clean
            WHERE legal_name IS NOT NULL AND legal_name <> ''
              AND (organisation_name IS NULL OR organisation_name <> legal_name)
        ),
        current_variants AS (
            SELECT
                oc.uprn,
                lb.postcode,
                TRIM(concat_ws(' ', oc.name_value, lb.base_address)) AS raw_address,
                lb.logical_status,
                lb.official_flag,
                lb.blpu_state,
                lb.postal_address_code,
                lb.parent_uprn,
                lb.hierarchy_level,
                CASE WHEN oc.name_source = 'LEGAL_NAME' THEN 'BUSINESS_CURRENT_LEGAL' ELSE 'BUSINESS_CURRENT' END AS variant_label,
                FALSE AS is_primary
            FROM organisation_candidates oc
            JOIN lpi_best_current lb ON lb.uprn = oc.uprn
            WHERE oc.end_date IS NULL
        ),
        historical_variants AS (
            SELECT
                oc.uprn,
                lb.postcode,
                TRIM(concat_ws(' ', oc.name_value, lb.base_address)) AS raw_address,
                lb.logical_status,
                lb.official_flag,
                lb.blpu_state,
                lb.postal_address_code,
                lb.parent_uprn,
                lb.hierarchy_level,
                CASE WHEN oc.name_source = 'LEGAL_NAME' THEN 'BUSINESS_HISTORICAL_LEGAL' ELSE 'BUSINESS_HISTORICAL' END AS variant_label,
                FALSE AS is_primary
            FROM organisation_candidates oc
            JOIN LATERAL (
                SELECT base_address, postcode, logical_status, official_flag, blpu_state,
                       postal_address_code, parent_uprn, hierarchy_level
                FROM lpi_base_distinct lb
                WHERE lb.uprn = oc.uprn
                ORDER BY
                    CASE WHEN (lb.start_date IS NULL OR oc.end_date >= lb.start_date)
                          AND (lb.end_date IS NULL OR oc.start_date <= lb.end_date) THEN 0 ELSE 1 END,
                    status_rank,
                    COALESCE(lb.last_update_date, DATE '0001-01-01') DESC
                LIMIT 1
            ) lb ON TRUE
            WHERE oc.end_date IS NOT NULL
        )
        SELECT uprn, postcode, raw_address, 'ORGANISATION' AS source, logical_status,
               official_flag, blpu_state, postal_address_code, parent_uprn, hierarchy_level,
               variant_label, is_primary
        FROM current_variants
        UNION ALL
        SELECT uprn, postcode, raw_address, 'ORGANISATION' AS source, logical_status,
               official_flag, blpu_state, postal_address_code, parent_uprn, hierarchy_level,
               variant_label, is_primary
        FROM historical_variants
        WHERE raw_address IS NOT NULL AND raw_address <> ''
    """)
