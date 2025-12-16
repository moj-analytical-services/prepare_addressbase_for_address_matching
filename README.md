# ABP Pipeline

Transform AddressBase Premium data into a clean flatfile format suitable for UK address matching.

## Overview

This package downloads, extracts, and transforms [AddressBase Premium](https://www.ordnancesurvey.co.uk/products/addressbase-premium) data from the OS Data Hub into a single parquet file optimized for address matching with [uk_address_matcher](https://github.com/RobinL/uk_address_matcher).

AddressBase Premium data is available to many government users under the [PSGA](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement).

## Quick Start

### 1. Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- OS Data Hub API key (get one at https://osdatahub.os.uk/)

### 2. Setup

```bash
# Clone the repository
git clone <repo-url>
cd abp-pipeline

# Install dependencies
uv sync

# Create environment file with your API credentials
cp .env.example .env
# Edit .env and add your OS_PROJECT_API_KEY
```

### 3. Configure

Edit `config.yaml` to customize paths if needed (defaults work out of the box):

```yaml
paths:
  work_dir: ./data
  downloads_dir: ./data/downloads
  extracted_dir: ./data/extracted
  parquet_dir: ./data/parquet
  output_dir: ./data/output

os_downloads:
  package_id: "0040204651"
  version_id: "6758807"  # Update when new data is released
```

### 4. Run

```bash
# Run the full pipeline
uv run python script.py --step all

# Or run individual steps
uv run python script.py --step download    # Download ABP data
uv run python script.py --step extract     # Extract zip files
uv run python script.py --step split       # Split by record type
uv run python script.py --step flatfile    # Create final output
```

## CLI Options

```
uv run python script.py --help

Options:
  --config PATH    Path to config.yaml (default: config.yaml)
  --step STEP      Pipeline step: download, extract, split, flatfile, all
  --force          Force re-run even if outputs exist
  --list           List available downloads without downloading
  --verbose, -v    Enable verbose/debug logging
```

## Pipeline Stages

1. **Download** - Downloads ABP data from OS Data Hub
2. **Extract** - Extracts zip files to CSV
3. **Split** - Splits mixed CSV into separate parquet files by record type
4. **Flatfile** - Transforms into final address matching format

Each stage is **idempotent** - safe to re-run. Use `--force` to overwrite existing outputs.

## Output Format

The final output (`data/output/abp_for_uk_address_matcher.parquet`) contains:

| Column | Description |
|--------|-------------|
| `uprn` | Unique Property Reference Number |
| `postcode` | Postal code |
| `address_concat` | Concatenated address string |
| `classification_code` | Property classification |
| `logical_status` | Address status (1=Approved, 3=Alternative, etc.) |
| `blpu_state` | Building state |
| `postal_address_code` | Postal address indicator |
| `udprn` | Royal Mail delivery point reference |
| `parent_uprn` | Parent UPRN for hierarchical addresses |
| `hierarchy_level` | C=Child, P=Parent, S=Singleton |
| `source` | Data source (LPI, ORGANISATION, DELIVERY_POINT, CUSTOM_LEVEL) |
| `variant_label` | Address variant type |
| `is_primary` | Whether this is the primary address for the UPRN |

## Downloading Files Manually

If you prefer to download manually:
- Log into https://osdatahub.os.uk/
- Create a datapackage
- Download the CSV files manually, or
- [Set up](https://www.ordnancesurvey.co.uk/products/os-downloads-api) an [API key](https://docs.os.uk/os-apis/core-concepts/getting-started-with-an-api-project) and [download using a script](https://docs.os.uk/os-apis/accessing-os-apis/os-downloads-api/getting-started/automating-os-premium-data-downloads)

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Fix lint issues
uv run ruff check --fix .
```

## Project Structure

```
abp-pipeline/
├── script.py              # CLI entrypoint
├── config.yaml            # Configuration
├── pyproject.toml         # Dependencies
├── .env.example           # Environment template
├── src/abp_pipeline/
│   ├── __init__.py
│   ├── settings.py        # Config/env loading
│   ├── os_downloads.py    # Download from OS Data Hub
│   ├── extract.py         # Zip extraction
│   ├── split_raw.py       # Split by record type
│   ├── to_flatfile.py     # Final transformation
│   ├── pipeline.py        # Orchestration
│   └── schemas/
│       └── abp_schema.yaml
└── tests/
    ├── test_smoke.py
    └── data/
        └── sample_abp_lines.csv
```

## License

See LICENSE file

