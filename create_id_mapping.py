import constants
import determine_wikidata_id_properties
import json
import re
import requests
from tqdm import tqdm
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query

# ---------------- CONFIG ---------------- #

# Provide your detected mapping properties here
try:
    MAPPING_PROPERTIES = constants.WIKIBASE_WIKIDATA_ID_PROPERTY
except (AttributeError, UnboundLocalError):
    MAPPING_PROPERTIES = determine_wikidata_id_properties.main()

OUTPUT_FILE = "wikibase_to_wikidata_map.json"

HEADERS = {
    "Accept": "application/sparql-results+json"
}

WIKIDATA_ID_PATTERN = re.compile(r"^[QPL]\d+$")

# Configuration for WikibaseIntegrator
wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIBASE_MEDIAWIKI_API_URL
wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIBASE_SPARQL_ENDPOINT
wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT
wbi_config['WIKIBASE_URL'] = constants.WIKIBASE_URL

# Initial login
login = wbi_login.Login(user=constants.WIKIBASE_CREDENTIAL_USERNAME, password=constants.WIKIBASE_CREDENTIAL_PASSWORD)
wbi = WikibaseIntegrator(login=login)

# ---------------- UTIL ---------------- #

def run_sparql(query):
    r = execute_sparql_query(query)
    return r["results"]["bindings"]


def extract_wikidata_id(value):
    """Extract Wikidata ID from raw value (URL or string)"""
    match = WIKIDATA_ID_PATTERN.search(value)
    return match.group(0) if match else None


def fetch_wikidata_entity(wikidata_id):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    try:
        r = requests.get(url)
        data = r.json()
        entity = data["entities"][wikidata_id]

        label = entity["labels"].get("en", {}).get("value")
        aliases = [
            a["value"] for a in entity.get("aliases", {}).get("en", [])
        ]

        return {
            "label": label,
            "aliases": aliases
        }
    except:
        return None


# ---------------- CORE ---------------- #

def get_mapped_items(prop):
    query = f"""
    PREFIX wd: <{constants.WIKIBASE_WD_PREFIX}>
    PREFIX wdt: <{constants.WIKIBASE_WDT_PREFIX}>

    SELECT ?item ?itemLabel ?val WHERE {{
      ?item wdt:{prop} ?val .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    return run_sparql(query)


def build_mapping():
    mapping = {}

    for prop in MAPPING_PROPERTIES:
        print(f"Processing {prop}...")
        results = get_mapped_items(prop)

        for r in tqdm(results):
            item_uri = r["item"]["value"]
            item_id = item_uri.split("/")[-1]
            item_label = r.get("itemLabel", {}).get("value")

            raw_val = r["val"]["value"]
            wikidata_id = extract_wikidata_id(raw_val)

            if not wikidata_id:
                continue

            if item_id not in mapping:
                mapping[item_id] = {
                    "label": item_label,
                    "wikidata": []
                }

            mapping[item_id]["wikidata"].append({
                "wikidata_id": wikidata_id,
                "source_property": prop
            })

    return mapping


def validate_mapping(mapping):
    validated = {}

    for item_id, data in tqdm(mapping.items()):
        item_label = data["label"]

        for wd in data["wikidata"]:
            wikidata_id = wd["wikidata_id"]
            wd_data = fetch_wikidata_entity(wikidata_id)

            if not wd_data:
                continue

            wd_label = wd_data["label"]

            # simple validation: label similarity
            if wd_label and item_label:
                similarity = (
                    wd_label.lower() == item_label.lower()
                    or wd_label.lower() in item_label.lower()
                    or item_label.lower() in wd_label.lower()
                )
            else:
                similarity = False

            if item_id not in validated:
                validated[item_id] = {
                    "label": item_label,
                    "matches": []
                }

            validated[item_id]["matches"].append({
                "wikidata_id": wikidata_id,
                "wd_label": wd_label,
                "aliases": wd_data["aliases"],
                "source_property": wd["source_property"],
                "label_match": similarity
            })

    return validated


# ---------------- MAIN ---------------- #

def main():
    raw_mapping = build_mapping()
    simple_mapping = True
    if not simple_mapping:
        validated_mapping = validate_mapping(raw_mapping)

    if simple_mapping:
        with open(OUTPUT_FILE, "w") as f:
           json.dump(raw_mapping, f, indent=2)
    else:
        with open(OUTPUT_FILE, "w") as f:
           json.dump(validated_mapping, f, indent=2)

    print(f"Saved mapping to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()