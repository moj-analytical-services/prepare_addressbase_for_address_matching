from pathlib import Path

from abp_pipeline.inspect_results import (
    get_flatfile,
    get_random_large_uprn,
    get_random_uprn,
    get_variant_statistics,
)
from abp_pipeline.settings import load_settings

settings = load_settings(Path("config.yaml"))
get_flatfile(settings)

# Get statistics
get_variant_statistics(settings)

# View a random address
get_random_uprn(settings)

# View a random "large" address from top 50
get_random_large_uprn(settings, top_n=50)
