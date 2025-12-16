from pathlib import Path

from abp_pipeline.inspect_results import (
    get_flatfile,
    get_random_large_uprn,
    get_random_uprn,
    get_variant_statistics,
)
from abp_pipeline.settings import create_duckdb_connection, load_settings

settings = load_settings(Path("config.yaml"))

# Create DuckDB connection once and reuse it
con = create_duckdb_connection(settings)

# Get the flatfile
get_flatfile(con, settings)

# Get statistics
get_variant_statistics(con, settings)

# View a random address
get_random_uprn(con, settings)

# View a random "large" address from top 50
get_random_large_uprn(con, settings, top_n=50)
