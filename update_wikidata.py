import constants
import json
import sys
from tqdm import tqdm
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.datatypes import String, Item, URL
from wikibaseintegrator.models import Reference
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_helpers import execute_sparql_query

# =========================
# CONFIG
# =========================

def set_wikibase_config():
    wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIBASE_MEDIAWIKI_API_URL
    wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIBASE_SPARQL_ENDPOINT
    wbi_config['WIKIBASE_URL'] = constants.WIKIBASE_URL
    wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT

def set_wikidata_config():
    wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIDATA_MEDIAWIKI_API_URL
    wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIDATA_SPARQL_ENDPOINT
    wbi_config['WIKIBASE_URL'] = constants.WIKIDATA_URL
    wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT

# =========================
# LOAD MAPPING
# =========================
def load_mapping(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# =========================
# CHECK EXISTING STATEMENT
# =========================
def statement_exists(item, prop, value):
    if prop not in item.claims:
        return False

    for claim in item.claims.get(prop):
        try:
            existing_value = claim.mainsnak.datavalue["value"]

            # Handle entity IDs vs strings
            if isinstance(existing_value, dict) and "id" in existing_value:
                existing_value = existing_value["id"]

            if existing_value == value:
                return True
        except Exception:
            continue

    return False

# =========================
# RESOLVE SOURCE → QID (OPTIONAL EXTENSION)
# =========================
def resolve_to_qid(source_value):
    """
    If your pipeline already knows mappings from your Wikibase entities
    to Wikidata QIDs, plug that logic in here.

    For now:
    - If value already looks like QID → return it
    - Else return None
    """
    if isinstance(source_value, str) and source_value.startswith("Q"):
        return source_value
    return None

# =========================
# BUILD REFERENCES
# =========================
def build_reference_from_value(source_value):
    ref = Reference()

    # CASE 1: already a Wikidata QID → use "stated in" (P248)
    if isinstance(source_value, str) and source_value.startswith("Q"):
        ref.add(Item(prop_nr="P248", value=source_value))
        return ref

    # CASE 2: fallback → reference URL (P854)
    if isinstance(source_value, str) and source_value.startswith("http"):
        ref.add(URL(prop_nr="P854", value=source_value))
        return ref

    return None

# =========================
# CREATE CLAIM OBJECT
# =========================
def create_claim_for_wikidata(prop, value, datatype):
    if datatype == "string":
        return String(prop_nr=prop, value=value)
    elif datatype == "url":
        return URL(prop_nr=prop, value=value)
    elif datatype == "item":
        return Item(prop_nr=prop, value=value)
    else:
        raise ValueError(f"Unsupported datatype: {datatype}")

# =========================
# ADD STATEMENT IF MISSING
# =========================
def statement_exists(item, prop, value):
    if prop not in item.claims:
        return False

    for claim in item.claims.get(prop):
        try:
            existing_value = claim.mainsnak.datavalue["value"]

            if isinstance(existing_value, dict) and "id" in existing_value:
                existing_value = existing_value["id"]

            if existing_value == value:
                return True
        except Exception:
            continue

    return False

# =========================
# MAIN PROCESS FUNCTION
# =========================
def process_entity(wikibase_id, wikidata_id, mappings):

    # Wikidata login
    set_wikidata_config()
    wikidata_login = wbi_login.Login(user=constants.WIKIDATA_CREDENTIAL_USERNAME, password=constants.WIKIDATA_CREDENTIAL_PASSWORD)
    wdi = WikibaseIntegrator(login=wikidata_login)
    wikidata_item = wdi.item.get(entity_id=wikidata_id)
    wikidata_claims = wikidata_item.claims

    # Wikibase login
    set_wikibase_config()
    wikibase_login = wbi_login.Login(user=constants.WIKIBASE_CREDENTIAL_USERNAME, password=constants.WIKIBASE_CREDENTIAL_PASSWORD)
    wbi = WikibaseIntegrator(login=wikibase_login)
    wikibase_item = wbi.item.get(entity_id=wikibase_id)
    wikibase_claims = wikibase_item.claims

    # Loop through ALL claims on the Wikibase item
    claims_mapping_list = []
    for claim in wikibase_claims:
        claim_mapping_dict = {
            'subject': {
                'wikibase_id': wikibase_id,
                'wikidata_id': wikidata_id
            },
            'predicate': {},
            'object': {}
        }

        claim_json = claim.get_json()
        if 'mainsnak' in claim_json:
            if 'property' in claim_json['mainsnak']:
                claim_mapping_dict['predicate']['wikibase_id'] = claim_json['mainsnak']['property']
                if claim_mapping_dict['predicate']['wikibase_id'] != constants.WIKIBASE_WIKIDATA_ID_PROPERTY: # Do not include mappings to Wikidata here
                    if claim_mapping_dict['predicate']['wikibase_id'] in mappings:
                        print(claim_mapping_dict['predicate']['wikibase_id'])
                        claim_mapping_dict['predicate']['wikidata_id'] = mappings[claim_mapping_dict['predicate']['wikibase_id']]["wikidata"][0]["wikidata_id"]
                    if 'datavalue' in claim_json['mainsnak']:
                        if 'value' in claim_json['mainsnak']['datavalue']:
                            if 'id' in claim_json['mainsnak']['datavalue']['value']:
                                claim_mapping_dict['object']['wikibase_id'] = claim_json['mainsnak']['datavalue']['value']['id']
                                if claim_mapping_dict['object']['wikibase_id'] in mappings:
                                    claim_mapping_dict['object']['wikidata_id'] = mappings[claim_mapping_dict['object']['wikibase_id']]["wikidata"][0]["wikidata_id"]

        if len(claim_mapping_dict['predicate'].keys()) == 2 and len(claim_mapping_dict['object'].keys()) == 2:
            claims_mapping_list.append(claim_mapping_dict)

    for claim in wikidata_claims:
        claim_json = claim.get_json()
        print()

        # Skip if property not mapped
        #if wb_prop not in mappings:
        #    continue

        #mapping_entry = mappings[wb_prop]

        # There may be multiple WD mappings per WB property
        #for wd_map in mapping_entry.get("wikidata", []):
        #    wd_prop = wd_map["wikidata_id"]
        #    source_prop = wd_map.get("source_property")

        #    for claim in wb_claims:
        #        try:
        #            datavalue = claim.mainsnak.datavalue["value"]
        #        except Exception:
        #            continue

                # -------------------------
                # Normalize value
                # -------------------------
        #        if isinstance(datavalue, dict):
        #            if "id" in datavalue:
        #                value = datavalue["id"]  # entity (QID)
        #                datatype = "item"
        #            else:
        #                value = datavalue
        #                datatype = "string"
        #        else:
        #            value = datavalue
        #            datatype = "string"

        #        if isinstance(value, str) and value.startswith("http"):
        #            datatype = "url"

                # -------------------------
                # Get source value (if exists)
                # -------------------------
        #        source_value = None
        #        if source_prop and source_prop in wikibase_item.claims:
        #            try:
        #                source_claim = wikibase_item.claims[source_prop][0]
        #                source_value = source_claim.mainsnak.datavalue["value"]
        #            except Exception:
        #                source_value = None

                # -------------------------
                # Build references
                # -------------------------
        #        references = []
        #        if source_value:
        #            ref = build_reference_from_value(source_value)
        #            if ref:
        #                references.append(ref)

                # -------------------------
                # Add statement if missing
                # -------------------------
        #        if not statement_exists(wikidata_item, wd_prop, value):
        #            new_claim = create_claim_for_wikidata(wd_prop, value, datatype)

        #            for ref in references:
        #                new_claim.references.add(ref)

        #            wikidata_item.claims.add(new_claim)
        #            print(f"+ {wikidata_id}: {wd_prop} = {value}")
        #        else:
        #            print(f"✔ Exists: {wikidata_id}: {wd_prop} = {value}")

    return wikidata_item

# =========================
# SAVE ITEM
# =========================
def save_item(item, login):
    item.write(login=login)
    print("💾 Item saved")

# =========================
# ENTRY POINT
# =========================
def main():
    mapping_path = constants.WIKIBASE_TO_WIKIDATA_MAPPING_FILE
    mappings = load_mapping(mapping_path)

    for wikibase_id, wikidata_dict in tqdm(mappings.items()):
        wikidata_id = wikidata_dict["wikidata"][0]["wikidata_id"]
        print(wikidata_id)

        if "Q" in wikidata_id:
            item = process_entity(wikibase_id, wikidata_id, mappings)
            print(item)
            exit()
            #save_item(item, wikidata_login)
        elif "L" in wikidata_id:
            print()
        elif "P" in wikidata_id:
            print()
        else:
            pass

if __name__ == "__main__":
    main()