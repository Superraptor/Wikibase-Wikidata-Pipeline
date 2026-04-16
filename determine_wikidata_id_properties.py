import constants
import re
import json
import requests
from tqdm import tqdm
from rapidfuzz.fuzz import ratio
from wikibaseintegrator import WikibaseIntegrator

# ---------------- CONFIG ---------------- #

HEADERS = {
    "Accept": "application/sparql-results+json"
}

# Heuristic weights
WEIGHTS = {
    "label_match": 2,
    "value_pattern": 3,
    "url_formatter": 2,
    "semantic_match": 4
}

THRESHOLD = 6  # minimum score to accept property

# Regex for Wikidata IDs
WD_ID_PATTERN = re.compile(r"^[QPL]\d+$")

# ---------------- UTIL FUNCTIONS ---------------- #

def run_sparql(query):
    r = requests.get(constants.WIKIBASE_SPARQL_ENDPOINT, params={"query": query}, headers=HEADERS)
    r.raise_for_status()
    return r.json()["results"]["bindings"]


def get_all_properties():
    query = """
    SELECT ?prop ?propLabel WHERE {
      ?prop a wikibase:Property .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """
    return run_sparql(query)


def get_property_aliases(prop):
    query = f"""
    SELECT ?alias WHERE {{
      wd:{prop} skos:altLabel ?alias .
      FILTER(LANG(?alias)="en")
    }}
    """
    return run_sparql(query)


def sample_property_values(prop, limit=50):
    query = f"""
    SELECT ?val WHERE {{
      ?item wdt:{prop} ?val .
    }} LIMIT {limit}
    """
    return run_sparql(query)


def get_url_formatter(prop):
    query = f"""
    SELECT ?formatter WHERE {{
      wd:{prop} wdt:P1630 ?formatter .
    }}
    """
    results = run_sparql(query)
    return results[0]["formatter"]["value"] if results else None


def fetch_wikidata_label(qid):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    try:
        r = requests.get(url)
        data = r.json()
        return data["entities"][qid]["labels"]["en"]["value"]
    except:
        return None


def get_local_item_label(item_uri):
    query = f"""
    SELECT ?label WHERE {{
      BIND(<{item_uri}> AS ?item)
      ?item rdfs:label ?label .
      FILTER(LANG(?label)="en")
    }} LIMIT 1
    """
    results = run_sparql(query)
    return results[0]["label"]["value"] if results else None


# ---------------- SCORING ---------------- #

def score_property(prop_id, prop_label):
    score = 0
    evidence = {}

    # (a) Label / alias match
    keywords = ["wikidata", "qid", "wikibase item", "wd id"]
    if any(k in prop_label.lower() for k in keywords):
        score += WEIGHTS["label_match"]
        evidence["label_match"] = True

    aliases = get_property_aliases(prop_id)
    if any(any(k in a["alias"]["value"].lower() for k in keywords) for a in aliases):
        score += WEIGHTS["label_match"]
        evidence["alias_match"] = True

    # (b) Value pattern match
    values = sample_property_values(prop_id)
    qid_like = [v["val"]["value"].split("/")[-1] for v in values]

    if any(WD_ID_PATTERN.match(v) for v in qid_like):
        score += WEIGHTS["value_pattern"]
        evidence["value_pattern"] = True

    # (c) URL formatter check
    formatter = get_url_formatter(prop_id)
    if formatter and "wikidata.org" in formatter:
        score += WEIGHTS["url_formatter"]
        evidence["url_formatter"] = formatter

    # (d) Semantic validation
    matches = 0
    for v in values[:10]:
        val = v["val"]["value"].split("/")[-1]
        if not WD_ID_PATTERN.match(val):
            continue

        wd_label = fetch_wikidata_label(val)
        local_label = get_local_item_label(v["val"]["value"])

        if wd_label and local_label:
            similarity = ratio(wd_label.lower(), local_label.lower())
            if similarity > 80:
                matches += 1

    if matches >= 3:
        score += WEIGHTS["semantic_match"]
        evidence["semantic_match"] = matches

    return score, evidence


# ---------------- MAIN ---------------- #

def main():
    props = get_all_properties()
    mapping = {}

    for p in tqdm(props):
        prop_uri = p["prop"]["value"]
        prop_id = prop_uri.split("/")[-1]
        prop_label = p.get("propLabel", {}).get("value", "")

        score, evidence = score_property(prop_id, prop_label)

        if score >= THRESHOLD:
            mapping[prop_id] = {
                "label": prop_label,
                "score": score,
                "evidence": evidence
            }

    with open(constants.MAPPING_FILE, "w") as f:
        json.dump(mapping, f, indent=2)

    print(f"Saved {len(mapping)} candidate mappings to {constants.MAPPING_FILE}")


if __name__ == "__main__":
    main()