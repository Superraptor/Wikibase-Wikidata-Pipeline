import constants
import determine_formatter_url_property
import re
import json
import requests
from tqdm import tqdm
from rapidfuzz.fuzz import ratio
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query

# ---------------- CONFIG ---------------- #

HEADERS = {
    "Accept": "application/sparql-results+json"
}

# Heuristic weights
WEIGHTS = {
    "label_match": 2,
    "value_pattern": 3,
    "url_pattern": 3,
    "datatype": 3,
    "semantic_match": 4
}

THRESHOLD = 2  # minimum score to accept property

# Regex for Wikidata IDs
WD_ID_PATTERN = re.compile(r"^[QPL]\d+$")

# Regex for URL datatypes
URL_PATTERN = re.compile(r"https?://")

# Configuration for WikibaseIntegrator
wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIBASE_MEDIAWIKI_API_URL
wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIBASE_SPARQL_ENDPOINT
wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT
wbi_config['WIKIBASE_URL'] = constants.WIKIBASE_URL

# Initial login
login = wbi_login.Login(user=constants.WIKIBASE_CREDENTIAL_USERNAME, password=constants.WIKIBASE_CREDENTIAL_PASSWORD)
wbi = WikibaseIntegrator(login=login)

# Set up keywords
keywords = {
    'reference URL': ["reference url", "reference link", "ref url", "ref link", "webref", "source url", "url for reference", "stated at url"],
    'stated in': ["stated in", "is stated in", "source of claim", "stated at", "cited in", "stated on"]
}

# ---------------- UTIL FUNCTIONS ---------------- #

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


def get_property_aliases(prop):
    query = f"""
    PREFIX wd: <{constants.WIKIBASE_WD_PREFIX}>

    SELECT ?alias WHERE {{
      wd:{prop} skos:altLabel ?alias .
      FILTER(LANG(?alias)="en")
    }}
    """
    return run_sparql(query)


def sample_property_values(prop, limit=50):
    query = f"""
    PREFIX wdt: <{constants.WIKIBASE_WDT_PREFIX}>

    SELECT ?val WHERE {{
      ?item wdt:{prop} ?val .
    }} LIMIT {limit}
    """
    return run_sparql(query)


def get_property_type(prop):
    query = f"""
    PREFIX wd: <{constants.WIKIBASE_WD_PREFIX}>

    SELECT ?datatype WHERE {{
      VALUES ?property {{ wd:{prop} }}
      ?property wikibase:propertyType ?datatype .

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """
    return run_sparql(query)


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

def score_property(prop_id, prop_label, keywords):
    score = 0
    evidence = {}

    # (a) Label / alias match
    if any(k in prop_label.lower() for k in keywords):
        score += WEIGHTS["label_match"]
        evidence["label_match"] = True

    if "label_match" in evidence:

        aliases = get_property_aliases(prop_id)
        if any(any(k in a["alias"]["value"].lower() for k in keywords) for a in aliases):
            score += WEIGHTS["label_match"]
            evidence["alias_match"] = True

        values = sample_property_values(prop_id)
        vals = [v["val"]["value"] for v in values if "val" in v]

        if not vals:
            return 0, {}

        # (b) Property type check
          # For "stated in", should be item
        if "stated in" in keywords:
            result = get_property_type(prop_id)        
            datatype_value = result[0]['datatype']['value']
            if "WikibaseItem" in datatype_value:
                score += WEIGHTS["datatype"]
                evidence["datatype"] = True

          # For "reference URL" should be URL, use URL pattern
        elif "reference url" in keywords:
            if any(URL_PATTERN.search(v) for v in vals):
                score += WEIGHTS["url_pattern"]
                evidence["url_pattern"] = True

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


# ---------------- RETURN HIGHEST SCORE 

# ---------------- MAIN ---------------- #

def main():
    props = get_all_properties()
    mapping = {}

    for x in ["reference URL", "stated in"]:
        keywords_to_use = keywords[x]
        mapping[x] = {}

        for p in tqdm(props):
            prop_uri = p["prop"]["value"]
            prop_id = prop_uri.split("/")[-1]
            prop_label = p.get("propLabel", {}).get("value", "")

            score, evidence = score_property(prop_id, prop_label, keywords_to_use)

            if score >= THRESHOLD:
                mapping[x][prop_id] = {
                    "label": prop_label,
                    "score": score,
                    "evidence": evidence
                }

    with open(constants.MAPPING_FILE, "w+") as f:
        json.dump(mapping, f, indent=2)

    lines_str = ""
    with open("constants.py", "r") as f:
        lines_str = str(f.readlines())

    with open("constants.py", "a+") as f:
        if "WIKIBASE_REFERENCE_URL_PROPERTY=" not in lines_str:
            f.write('\nWIKIBASE_REFERENCE_URL_PROPERTY="%s"' % list(mapping["reference URL"].keys())[0])
        if "WIKIBASE_STATED_IN_PROPERTY=" not in lines_str:
            f.write('\nWIKIBASE_STATED_IN_PROPERTY="%s"' % list(mapping["stated in"].keys())[0])

    print(f"Saved {len(mapping)} candidate mappings to {constants.MAPPING_FILE}")

    return list(mapping.keys())[0], list(mapping.keys())[1] 

if __name__ == "__main__":
    main()