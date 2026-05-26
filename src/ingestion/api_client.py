"""
api_client.py
=============
Calls the PubChem REST API to enrich bioprocess product data
with additional compound information (molecular formula, SMILES, etc.).

PubChem is a free, public chemical database maintained by the NIH.
This mirrors what Manus would do with their internal compound database API.

API docs: https://pubchem.ncbi.nlm.nih.gov/docs/pug-rest
"""

import time
import logging
from pathlib import Path

import requests
import pandas as pd

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import PUBCHEM_BASE_URL, API_TIMEOUT, API_RETRY_LIMIT, BRONZE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
log = logging.getLogger(__name__)


def fetch_compound_data(compound_name: str) -> dict:
    """
    Call PubChem API to get compound properties for a given name.

    Returns a dict of compound properties, or empty dict if not found.
    Implements retry logic with exponential backoff.
    """
    url = f"{PUBCHEM_BASE_URL}/{compound_name}/property/MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES/JSON"

    for attempt in range(1, API_RETRY_LIMIT + 1):
        try:
            response = requests.get(url, timeout=API_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                props = data.get("PropertyTable", {}).get("Properties", [{}])[0]
                return {
                    "api_cid": props.get("CID"),
                    "api_molecular_formula": props.get("MolecularFormula"),
                    "api_molecular_weight": props.get("MolecularWeight"),
                    "api_iupac_name": props.get("IUPACName"),
                    "api_canonical_smiles": props.get("CanonicalSMILES"),
                    "api_source": "PubChem",
                    "api_status": "success",
                }

            elif response.status_code == 404:
                log.warning(f"Compound not found in PubChem: {compound_name}")
                return {"api_status": "not_found", "api_source": "PubChem"}

            else:
                log.warning(
                    f"PubChem returned {response.status_code} for {compound_name} (attempt {attempt})"
                )

        except requests.exceptions.Timeout:
            log.warning(
                f"PubChem request timed out for {compound_name} (attempt {attempt})"
            )
        except requests.exceptions.RequestException as e:
            log.warning(
                f"PubChem request error for {compound_name}: {e} (attempt {attempt})"
            )

        # Exponential backoff between retries
        if attempt < API_RETRY_LIMIT:
            wait = 2**attempt
            log.info(f"Retrying in {wait}s...")
            time.sleep(wait)

    return {"api_status": "failed", "api_source": "PubChem"}


def enrich_products(
    df: pd.DataFrame, product_col: str = "product_name"
) -> pd.DataFrame:
    """
    Enrich the dataset by calling PubChem for each unique product.

    Only calls the API once per unique product name — caches results
    and applies them to all matching rows. Efficient and API-friendly.
    """
    if product_col not in df.columns:
        log.warning(f"Column '{product_col}' not found. Skipping API enrichment.")
        return df

    unique_products = df[product_col].dropna().unique()
    log.info(
        f"Fetching PubChem data for {len(unique_products)} unique products: {unique_products}"
    )

    # Cache: compound_name → api result dict
    cache = {}

    for product in unique_products:
        log.info(f"Calling PubChem API for: {product}")
        cache[product] = fetch_compound_data(str(product))
        time.sleep(0.5)  # Be polite to the public API — avoid rate limiting

    # Map API results back onto the dataframe
    api_df = pd.DataFrame(
        [{"product_name": name, **data} for name, data in cache.items()]
    )

    enriched = df.merge(api_df, on="product_name", how="left")
    log.info(f"API enrichment complete. Added {len(api_df.columns) - 1} columns.")
    return enriched


def save_to_bronze(df: pd.DataFrame, filename: str = "api_enrichment.parquet"):
    """Save API-enriched data to Bronze landing zone."""
    from datetime import datetime, timezone

    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_dir = BRONZE_PATH / f"date={date_str}" / "source=pubchem_api"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename
    df.to_parquet(out_path, index=False)
    log.info(f"API-enriched data saved to Bronze: {out_path}")
    return out_path


if __name__ == "__main__":
    # Quick test with known fatty acid compounds from the dataset
    test_compounds = ["octanoic acid", "decanoic acid", "fatty acid"]
    for c in test_compounds:
        result = fetch_compound_data(c)
        print(f"\n{c}: {result}")
