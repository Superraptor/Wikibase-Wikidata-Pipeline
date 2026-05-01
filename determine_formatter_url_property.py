import constants
import re
import json
import requests
from tqdm import tqdm
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query

# ---------------- CONFIG ---------------- #

HEADERS = {
    "Accept": "application/sparql-results+json"
}

# Heuristic weights
WEIGHTS = {
    "url_pattern": 3,
    "placeholder": 3,
    "used_on_property": 2,
    "resolves": 4
}

THRESHOLD = 6

# Regex patterns
URL_PATTERN = re.compile(r"https?://")
PLACEHOLDER_PATTERN = re.compile(r"(\$1|\{.+?\}|%s)")

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


def get_all_properties():
    query = """
    SELECT ?prop ?propLabel WHERE {
      ?prop a wikibase:Property .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """
    return run_sparql(query)


def sample_property_values(prop, limit=20):
    query = f"""
    PREFIX wd: <{constants.WIKIBASE_WD_PREFIX}>
    PREFIX wdt: <{constants.WIKIBASE_WDT_PREFIX}>

    SELECT ?val WHERE {{
      ?entity wdt:{prop} ?val .
    }} LIMIT {limit}
    """
    return run_sparql(query)


def is_used_on_properties(prop):
    query = f"""
    PREFIX wd: <{constants.WIKIBASE_WD_PREFIX}>
    PREFIX wdt: <{constants.WIKIBASE_WDT_PREFIX}>

    ASK {{
      ?p a wikibase:Property .
      ?p wdt:{prop} ?val .
    }}
    """
    r = requests.get(constants.WIKIBASE_SPARQL_ENDPOINT, params={"query": query}, headers=HEADERS)
    return r.json()["boolean"]


def get_candidate_ids(limit=20):
    """Find identifier-like values from other properties"""
    query = f"""
    SELECT ?val WHERE {{
      ?item ?p ?val .
      FILTER(REGEX(STR(?val), "^[A-Za-z0-9_-]+$"))
    }} LIMIT {limit}
    """
    return [r["val"]["value"] for r in run_sparql(query)]


def test_url_resolution(template, test_values):
    successes = 0

    for val in test_values[:5]:
        try:
            url = template.replace("$1", val).replace("%s", val)
            r = requests.head(url, allow_redirects=True, timeout=5)
            if r.status_code < 400:
                successes += 1
        except:
            continue

    return successes


# ---------------- SCORING ---------------- #

def score_property(prop_id, prop_label):
    score = 0
    evidence = {}

    values = sample_property_values(prop_id)
    vals = [v["val"]["value"] for v in values if "val" in v]

    if not vals:
        return 0, {}

    # (a) URL pattern
    if any(URL_PATTERN.search(v) for v in vals):
        score += WEIGHTS["url_pattern"]
        evidence["url_pattern"] = True

    # (b) Placeholder detection
    templates = [v for v in vals if PLACEHOLDER_PATTERN.search(v)]
    if templates:
        score += WEIGHTS["placeholder"]
        evidence["placeholder_examples"] = templates[:3]

    # (c) Used on properties
    if is_used_on_properties(prop_id):
        score += WEIGHTS["used_on_property"]
        evidence["used_on_property"] = True

    # (d) Resolution test
    if templates:
        test_vals = get_candidate_ids()
        successes = test_url_resolution(templates[0], test_vals)

        if successes >= 2:
            score += WEIGHTS["resolves"]
            evidence["resolution_successes"] = successes

    return score, evidence


# ---------------- MAIN ---------------- #

def main():
    props = get_all_properties()
    results = {}

    for p in tqdm(props):
        prop_uri = p["prop"]["value"]
        prop_id = prop_uri.split("/")[-1]
        prop_label = p.get("propLabel", {}).get("value", "")

        if "url" in prop_label.lower():

            score, evidence = score_property(prop_id, prop_label)

            if score >= THRESHOLD:
                results[prop_id] = {
                    "label": prop_label,
                    "score": score,
                    "evidence": evidence
                }

    with open(constants.MAPPING_FILE, "w+") as f:
        json.dump(results, f, indent=2)

    lines_str = ""
    with open("constants.py", "r") as f:
        lines_str = str(f.readlines())

    with open("constants.py", "a+") as f:
        if "WIKIBASE_FORMATTER_URL_PROPERTY=" not in lines_str:
            f.write('\nWIKIBASE_FORMATTER_URL_PROPERTY="%s"' % list(results.keys())[0])

    print(f"Saved {len(results)} formatter candidates to {constants.MAPPING_FILE}")

    return list(results.keys())[0]


if __name__ == "__main__":
    main()