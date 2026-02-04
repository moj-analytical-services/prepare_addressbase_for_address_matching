# ABP Pipeline

Transform AddressBase Premium data into a clean flatfile format suitable for use with [`uk_address_matcher`](https://github.com/moj-analytical-services/uk_address_matcher).

The key feature is that we output multiple variants of the full address string to increase the likelihood of matching.

## Overview

This package downloads, extracts, and transforms [AddressBase Premium](https://www.ordnancesurvey.co.uk/products/addressbase-premium) data from the OS Data Hub into a single parquet file optimized for address matching with [uk_address_matcher](https://github.com/RobinL/uk_address_matcher).

AddressBase Premium data is available to many government users under the [PSGA](https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement).

## Quick Start

### 1. Prerequisites

- Create a datapackage on the OS Data Hub containing the AddressBase data you need - either full supply or the subset of the country you're interested in
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- OS Data Hub API key (get one at https://osdatahub.os.uk/) — only required for the download step



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

processing:
  # Number of chunks to split flatfile processing into
  # Use higher values (e.g., 10) for lower memory usage on laptops
  num_chunks: 1
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



## Pipeline Stages

1. **Download** - Downloads ABP data from OS Data Hub
2. **Extract** - Extracts zip files to CSV
3. **Split** - Splits mixed CSV into separate parquet files by record type
4. **Flatfile** - Transforms into final address matching format

Each stage is **idempotent** - safe to re-run. Use `--force` to overwrite existing outputs.

## Output Format

The final output is written to `data/output/` as one or more parquet files:

- **Single chunk mode** (`num_chunks: 1`): `abp_for_uk_address_matcher.chunk_001_of_001.parquet`
- **Multi-chunk mode** (`num_chunks: N`): `abp_for_uk_address_matcher.chunk_001_of_00N.parquet`, `chunk_002_of_00N.parquet`, etc.

Chunking reduces memory usage by processing UPRNs in batches. The union of all chunk files equals the single-chunk output. Use a higher `num_chunks` (e.g., 10) for laptops with limited RAM.

Each file contains:

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
- Download the CSV zip file (e.g., AB76GB_CSV.zip)

To run the pipeline from a manual download:

1) Place the zip in the downloads directory configured in [config.yaml](config.yaml).
  - By default this is data/downloads/
  - The extract step looks for *.zip files in this folder

2) Run the pipeline starting from extract:
  - In [script.py](script.py) set STEP = ["extract", "split", "flatfile"]
  - Then run: uv run python script.py

Notes:
- Extraction will create a subfolder under data/extracted/ named after the zip stem
  (e.g., data/extracted/AB76GB_CSV/) and the split step will read **/*.csv from there.

If you prefer to use the OS Downloads API instead:
- [Set up](https://www.ordnancesurvey.co.uk/products/os-downloads-api) an [API key](https://docs.os.uk/os-apis/core-concepts/getting-started-with-an-api-project) and [download using a script](https://docs.os.uk/os-apis/accessing-os-apis/os-downloads-api/getting-started/automating-os-premium-data-downloads)


## Time taken to run:

Excluding the time to download the full 9.6GB zip file, on an M4 Macbook Pro it takes about:
- 1 minute to extract the zip file
- 7 minutes to run the remainder of the build script