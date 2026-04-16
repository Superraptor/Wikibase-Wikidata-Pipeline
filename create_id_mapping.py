import re
import json
import requests
from tqdm import tqdm

# ---------------- CONFIG ---------------- #
WB_URL = "https://your-wikibase.org"
SPARQL_ENDPOINT = f"{WB_URL}/query/sparql"

# Provide your detected mapping properties here
MAPPING_PROPERTIES = ["P123", "P456"]

OUTPUT_FILE = "wikibase_to_wikidata_map.json"

HEADERS = {
    "Accept": "application/sparql-results+json"
}

QID_PATTERN = re.compile(r"Q\d+")

# ---------------- UTIL ---------------- #

def run_sparql(query):
    r = requests.get(SPARQL_ENDPOINT, params={"query": query}, headers=HEADERS)
    r.raise_for_status()
    return r.json()["results"]["bindings"]


def extract_qid(value):
    """Extract QID from raw value (URL or string)"""
    match = QID_PATTERN.search(value)
    return match.group(0) if match else None


def fetch_wikidata_entity(qid):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    try:
        r = requests.get(url)
        data = r.json()
        entity = data["entities"][qid]

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
            qid = extract_qid(raw_val)

            if not qid:
                continue

            if item_id not in mapping:
                mapping[item_id] = {
                    "label": item_label,
                    "wikidata": []
                }

            mapping[item_id]["wikidata"].append({
                "qid": qid,
                "source_property": prop
            })

    return mapping


def validate_mapping(mapping):
    validated = {}

    for item_id, data in tqdm(mapping.items()):
        item_label = data["label"]

        for wd in data["wikidata"]:
            qid = wd["qid"]
            wd_data = fetch_wikidata_entity(qid)

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
                "qid": qid,
                "wd_label": wd_label,
                "aliases": wd_data["aliases"],
                "source_property": wd["source_property"],
                "label_match": similarity
            })

    return validated


# ---------------- MAIN ---------------- #

def main():
    raw_mapping = build_mapping()
    validated_mapping = validate_mapping(raw_mapping)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(validated_mapping, f, indent=2)

    print(f"Saved mapping to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()