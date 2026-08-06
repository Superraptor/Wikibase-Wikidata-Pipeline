import argparse
import constants
import json
import sys
import time
from tqdm import tqdm
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.datatypes import String, Item, URL, Time

from wikibaseintegrator.models import references
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_enums import ActionIfExists
from wikibaseintegrator.wbi_helpers import execute_sparql_query


# =========================
# CONFIG
# =========================

def set_wikibase_config():
    wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIBASE_MEDIAWIKI_API_URL
    wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIBASE_SPARQL_ENDPOINT
    wbi_config['WIKIBASE_URL'] = constants.WIKIBASE_URL
    wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT
    wbi_config['MAXLAG'] = 0

def set_wikidata_config():
    wbi_config['MEDIAWIKI_API_URL'] = constants.WIKIDATA_MEDIAWIKI_API_URL
    wbi_config['SPARQL_ENDPOINT_URL'] = constants.WIKIDATA_SPARQL_ENDPOINT
    wbi_config['WIKIBASE_URL'] = constants.WIKIDATA_URL
    wbi_config['USER_AGENT'] = constants.WIKIBASE_USER_AGENT
    wbi_config['MAXLAG'] = 0

# =========================
# LAZY SINGLETON AUTHENTICATION MANAGER
# =========================
_WIKIDATA_LOGIN_INSTANCE = None
_WIKIBASE_LOGIN_INSTANCE = None

def get_wikidata_login(force_new=False):
    global _WIKIDATA_LOGIN_INSTANCE
    if _WIKIDATA_LOGIN_INSTANCE is None or force_new:
        set_wikidata_config()
        for attempt in range(3):
            try:
                _WIKIDATA_LOGIN_INSTANCE = wbi_login.Login(
                    user=constants.WIKIDATA_CREDENTIAL_USERNAME,
                    password=constants.WIKIDATA_CREDENTIAL_PASSWORD
                )
                break
            except Exception as e:
                print(f"Wikidata Login attempt {attempt+1} failed: {e}. Retrying in 3 seconds...")
                time.sleep(3)
    return _WIKIDATA_LOGIN_INSTANCE

def get_wikibase_login(force_new=False):
    global _WIKIBASE_LOGIN_INSTANCE
    if _WIKIBASE_LOGIN_INSTANCE is None or force_new:
        set_wikibase_config()
        for attempt in range(3):
            try:
                _WIKIBASE_LOGIN_INSTANCE = wbi_login.Login(
                    user=constants.WIKIBASE_CREDENTIAL_USERNAME,
                    password=constants.WIKIBASE_CREDENTIAL_PASSWORD
                )
                break
            except Exception as e:
                print(f"Wikibase Login attempt {attempt+1} failed: {e}. Retrying in 3 seconds...")
                time.sleep(3)
    return _WIKIBASE_LOGIN_INSTANCE





# =========================
# LOAD MAPPING
# =========================
def load_mapping(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================
# CACHING & INVERTED INDEX
# =========================
def build_inverted_mapping(mappings):
    """Builds a map from wikidata_id -> list of corresponding wikibase_ids for O(1) lookups."""
    wd_to_wb = {}
    for wb_id, data in mappings.items():
        if isinstance(data, dict) and "wikidata" in data and len(data["wikidata"]) > 0:
            wd_id = data["wikidata"][0].get("wikidata_id")
            if wd_id:
                if wd_id not in wd_to_wb:
                    wd_to_wb[wd_id] = []
                wd_to_wb[wd_id].append(wb_id)
    return wd_to_wb


def load_processed_cache(cache_path=None):
    if cache_path is None:
        cache_path = constants.PROCESSED_CACHE_FILE
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_processed_cache(cache, cache_path=None):
    if cache_path is None:
        cache_path = constants.PROCESSED_CACHE_FILE
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def is_processed(wikibase_id, cache):
    return wikibase_id in cache and cache[wikibase_id].get("status") == "success"


def mark_processed(wikibase_id, wikidata_id, cache, cache_path=None):
    cache[wikibase_id] = {
        "wikidata_id": wikidata_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "success"
    }
    save_processed_cache(cache, cache_path)


def load_statement_history(path=None):
    if path is None:
        path = constants.ADDED_STATEMENTS_LOG_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_statement_history(history, path=None):
    if path is None:
        path = constants.ADDED_STATEMENTS_LOG_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def is_statement_already_added(wikidata_id, prop_nr, obj_val, history):
    key = f"{wikidata_id} {prop_nr} {obj_val}"
    return key in history


def mark_statement_added(wikidata_id, prop_nr, obj_val, history, path=None):
    key = f"{wikidata_id} {prop_nr} {obj_val}"
    history[key] = {
        "wikidata_id": wikidata_id,
        "property": prop_nr,
        "value": obj_val,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    save_statement_history(history, path)


def load_last_checkpoint(path=None):
    if path is None:
        path = constants.LAST_CHECKPOINT_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("last_processed_entity")
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_last_checkpoint(wikibase_id, wikidata_id=None, path=None):
    if path is None:
        path = constants.LAST_CHECKPOINT_FILE
    data = {
        "last_processed_entity": wikibase_id,
        "wikidata_id": wikidata_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)




# =========================
# STATED-IN URL RESOLUTION & CONCEPT-THING HELPERS
# =========================
_ENTITY_CACHE = {}

def resolve_stated_in_url(stated_in_wb_id, wbi):
    """
    Look up the 'stated in' entity on Wikibase (e.g. Q7498) and check for property
    P62 ('available at' / work available at URL). Returns URL string if found.
    """
    if stated_in_wb_id in _ENTITY_CACHE:
        entity = _ENTITY_CACHE[stated_in_wb_id]
    else:
        try:
            entity = wbi.item.get(entity_id=stated_in_wb_id)
            _ENTITY_CACHE[stated_in_wb_id] = entity
        except Exception:
            return None

    if entity and hasattr(entity, 'claims'):
        for claim in entity.claims:
            claim_json = claim.get_json()
            mainsnak = claim_json.get('mainsnak', {})
            prop_id = mainsnak.get('property')
            # Check specifically for P62 ('available at' / Wikidata P953) or reference URL property
            if prop_id == constants.WIKIBASE_WORK_AVAILABLE_AT_URL_PROPERTY or prop_id == constants.WIKIBASE_REFERENCE_URL_PROPERTY:
                datavalue = mainsnak.get('datavalue', {})
                val = datavalue.get('value')
                if isinstance(val, str) and val.startswith("http"):
                    return val
    return None


def get_sibling_wikibase_claims(wikibase_id, wikibase_item, wbi, mappings, inverted_map):
    """
    Collect claims from the primary Wikibase item as well as any concept/thing sibling items.
    Handles P1150 (corresponding class), P1149 (corresponding concept),
    P1148 (identifies), P1142 (identified by), and dual mappings.
    """
    all_claims = list(wikibase_item.claims)
    sibling_wb_ids = set()

    # 1. Check direct mapping duplicates (multiple WB items mapping to same WD QID)
    if wikibase_id in mappings:
        wd_id = mappings[wikibase_id]["wikidata"][0]["wikidata_id"]
        if inverted_map and wd_id in inverted_map:
            for s_id in inverted_map[wd_id]:
                if s_id != wikibase_id:
                    sibling_wb_ids.add(s_id)

    # 2. Check concept/thing relationship claims on wikibase_item
    for claim in wikibase_item.claims:
        claim_json = claim.get_json()
        mainsnak = claim_json.get('mainsnak', {})
        prop = mainsnak.get('property')
        if prop in [
            constants.WIKIBASE_CORRESPONDING_CLASS_PROPERTY,
            constants.WIKIBASE_CORRESPONDING_CONCEPT_PROPERTY,
            constants.WIKIBASE_IDENTIFIES_PROPERTY,
            constants.WIKIBASE_IDENTIFIED_BY_PROPERTY
        ]:
            val = mainsnak.get('datavalue', {}).get('value', {})
            if isinstance(val, dict) and 'id' in val:
                sibling_wb_ids.add(val['id'])

    # Fetch claims from all sibling items
    for s_id in sibling_wb_ids:
        if s_id not in _ENTITY_CACHE:
            try:
                set_wikibase_config()
                _ENTITY_CACHE[s_id] = wbi.item.get(entity_id=s_id)
            except Exception:
                continue
        s_item = _ENTITY_CACHE.get(s_id)
        if s_item and hasattr(s_item, 'claims'):
            all_claims.extend(s_item.claims)

    return all_claims



# =========================
# SAVE CLAIMS TO ITEM
# =========================
def save_item(wikidata_id, formatted_claims_to_add):
    
    # Wikidata login
    set_wikidata_config()
    wikidata_login = wbi_login.Login(user=constants.WIKIDATA_CREDENTIAL_USERNAME, password=constants.WIKIDATA_CREDENTIAL_PASSWORD)
    wdi = WikibaseIntegrator(login=wikidata_login)

    wikidata_item = None
    for claim in formatted_claims_to_add:
        if wikidata_item is None:
            wikidata_item = wdi.item.get(entity_id=wikidata_id)
        wikidata_item.claims.add(claim, action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)

    if wikidata_item is not None:
        wikidata_item.write()
        print("Saved claims for %s to Wikidata." % wikidata_id)


# =========================
# CREATE CLAIM OBJECT
# =========================
def create_claims_for_wikidata(claims, refs, statement_history=None, is_human_entity=False):
    if statement_history is None:
        statement_history = load_statement_history()

    formatted_claims = []
    excluded_qids = getattr(constants, 'EXCLUDED_INSTANCE_OF_QIDS', {"Q151885", "Q21500366", "Q16333", "Q85796231", "Q223557", "Q64762"})
    human_qid = getattr(constants, 'WIKIDATA_HUMAN_QID', 'Q5')
    instance_of_prop = getattr(constants, 'WIKIDATA_INSTANCE_OF_PROPERTY', 'P31')

    ref_url_prop = getattr(constants, 'WIKIDATA_REFERENCE_URL_PROPERTY', 'P854')
    stated_in_prop = getattr(constants, 'WIKIDATA_STATED_IN_PROPERTY', 'P248')
    archived_url_prop = getattr(constants, 'WIKIDATA_ARCHIVED_URL_PROPERTY', 'P1065')
    date_archived_prop = getattr(constants, 'WIKIDATA_DATE_ARCHIVED_PROPERTY', 'P2960')

    # Detect if any candidate claim is instance of human (P31 -> Q5)
    if not is_human_entity:
        for claim in claims:
            if claim.get('predicate', {}).get('wikidata_id') == instance_of_prop and claim.get('object', {}).get('wikidata_id') == human_qid:
                is_human_entity = True
                break

    for claim in claims:
        wb_triple_key = make_triple_key(claim, wikibase=True)
        wd_triple_key = make_triple_key(claim, wikibase=False)
        
        wikidata_id = claim['subject']['wikidata_id']
        prop_wd = claim['predicate']['wikidata_id']
        obj_wd = claim['object']['wikidata_id']

        # Rule 1: Prevent adding "instance of" (P31) "concept" or "real-world object" to Wikidata
        if prop_wd == instance_of_prop and obj_wd in excluded_qids:
            print(f"Skipping excluded 'instance of' statement ({prop_wd} -> {obj_wd}) for Wikidata.")
            continue

        # Rule 2: Prevent adding non-human "instance of" (P31) statements to human items (Q5)
        if is_human_entity and prop_wd == instance_of_prop and obj_wd != human_qid:
            print(f"Skipping non-human 'instance of' statement ({prop_wd} -> {obj_wd}) for human item {wikidata_id}.")
            continue

        # Rule 3: Once something is added, don't add it again!
        if is_statement_already_added(wikidata_id, prop_wd, obj_wd, statement_history):
            continue

        # Rule 4: Don't add anything that doesn't have a reference!
        if not refs:
            continue

        ref_list_data = refs.get(wb_triple_key) or refs.get(wd_triple_key)
        if not ref_list_data:
            continue

        formatted_refs_list = references.References()
        for ref_dict in ref_list_data:
            # Construct a SINGULAR reference combining reference URL, stated-in, archived URL, and date archived
            formatted_ref = references.Reference()
            
            # 1. Reference URL (P854)
            if ref_url_prop in ref_dict and ref_dict[ref_url_prop]:
                formatted_ref.add(URL(prop_nr=ref_url_prop, value=ref_dict[ref_url_prop]))
            
            # 2. Stated in (P248)
            if stated_in_prop in ref_dict and ref_dict[stated_in_prop]:
                formatted_ref.add(Item(prop_nr=stated_in_prop, value=ref_dict[stated_in_prop]))

            # 3. Archived URL (P1065)
            if archived_url_prop in ref_dict and ref_dict[archived_url_prop]:
                formatted_ref.add(URL(prop_nr=archived_url_prop, value=ref_dict[archived_url_prop]))

            # 4. Date archived (P2960)
            if date_archived_prop in ref_dict and ref_dict[date_archived_prop]:
                date_val = ref_dict[date_archived_prop]
                if isinstance(date_val, dict) and 'time' in date_val:
                    formatted_ref.add(Time(prop_nr=date_archived_prop, time=date_val['time']))
                elif isinstance(date_val, str):
                    if date_val.startswith("+") or date_val.startswith("-"):
                        formatted_ref.add(Time(prop_nr=date_archived_prop, time=date_val))
                    else:
                        formatted_ref.add(String(prop_nr=date_archived_prop, value=date_val))


            if len(formatted_ref) > 0:
                formatted_refs_list.add(formatted_ref)

        # STRICT REQUIREMENT: Only add claim IF non-empty reference list is attached!
        if len(formatted_refs_list) > 0:
            try:
                formatted_claim = Item(
                    prop_nr=prop_wd,
                    value=obj_wd,
                    references=formatted_refs_list
                )
                formatted_claims.append(formatted_claim)
            except ValueError:
                pass # Skips non-item objects for now

    return formatted_claims




def is_duplicate_reference(candidate_ref, existing_wd_refs):
    """
    Checks if candidate_ref is a duplicate of any reference in existing_wd_refs:
    1) Exact match: All snak properties and values in candidate_ref exist in the Wikidata ref.
    2) 'Stated in' match: Candidate reference has a 'stated in' (P248) that matches a Wikidata ref's
       'stated in', AND the Wikidata ref has equal or more information (total snak count).
    """
    cand_snak_count = len(candidate_ref)
    cand_stated_in = candidate_ref.get('P248')

    for wd_ref in existing_wd_refs:
        wd_snaks = wd_ref.get('snaks', {})
        wd_stated_in = wd_ref.get('stated_in', set())
        wd_snak_count = wd_ref.get('total_snak_count', 0)

        # Rule 1: Exact reference match
        exact_match = True
        for prop, val in candidate_ref.items():
            if prop not in wd_snaks or val not in wd_snaks[prop]:
                exact_match = False
                break
        if exact_match:
            return True

        # Rule 2: 'Stated in' match where Wikidata ref has equal or more information
        if cand_stated_in and cand_stated_in in wd_stated_in:
            if wd_snak_count >= cand_snak_count:
                return True

    return False


# =========================
# MAIN PROCESS FUNCTION
# =========================
def process_entity(wikibase_id, wikidata_id, mappings, inverted_map=None, only_return_if_ref=True, return_if_exists_but_no_ref=True):

    if inverted_map is None:
        inverted_map = build_inverted_mapping(mappings)

    # Wikidata login
    set_wikidata_config()
    wikidata_login = get_wikidata_login()
    wdi = WikibaseIntegrator(login=wikidata_login)
    try:
        wikidata_item = wdi.item.get(entity_id=wikidata_id)
    except Exception as e:
        if "Login" in str(e) or "session" in str(e).lower():
            print("Session error on Wikidata fetch. Refreshing login session...")
            wikidata_login = get_wikidata_login(force_new=True)
            wdi = WikibaseIntegrator(login=wikidata_login)
            wikidata_item = wdi.item.get(entity_id=wikidata_id)
        else:
            raise e

    wikidata_claims = wikidata_item.claims
    wikidata_ref_dict = {}

    # Wikibase login
    set_wikibase_config()
    wikibase_login = get_wikibase_login()
    wbi = WikibaseIntegrator(login=wikibase_login)
    try:
        wikibase_item = wbi.item.get(entity_id=wikibase_id)
    except Exception as e:
        if "Login" in str(e) or "session" in str(e).lower():
            print("Session error on Wikibase fetch. Refreshing login session...")
            wikibase_login = get_wikibase_login(force_new=True)
            wbi = WikibaseIntegrator(login=wikibase_login)
            wikibase_item = wbi.item.get(entity_id=wikibase_id)
        else:
            raise e

    
    # Collect claims from primary item as well as concept/thing sibling items
    wikibase_claims = get_sibling_wikibase_claims(wikibase_id, wikibase_item, wbi, mappings, inverted_map)
    wikibase_ref_dict = {}

    # Loop through ALL claims on the Wikibase item(s)
    wikibase_claims_mapping_list = []
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
                        claim_mapping_dict['predicate']['wikidata_id'] = mappings[claim_mapping_dict['predicate']['wikibase_id']]["wikidata"][0]["wikidata_id"]
                    if 'datavalue' in claim_json['mainsnak']:
                        if 'value' in claim_json['mainsnak']['datavalue']:
                            if 'id' in claim_json['mainsnak']['datavalue']['value']:
                                if type(claim_json['mainsnak']['datavalue']['value']) != str:
                                    claim_mapping_dict['object']['wikibase_id'] = claim_json['mainsnak']['datavalue']['value']['id']
                                    if claim_mapping_dict['object']['wikibase_id'] in mappings:
                                        claim_mapping_dict['object']['wikidata_id'] = mappings[claim_mapping_dict['object']['wikibase_id']]["wikidata"][0]["wikidata_id"]

        if len(claim_mapping_dict['predicate'].keys()) == 2 and len(claim_mapping_dict['object'].keys()) == 2:
            wikibase_claims_mapping_list.append(claim_mapping_dict)
            wikibase_ref_list = []
            for reference in claim.references:
                reference_json = reference.get_json()
                if 'snaks' in reference_json:
                    ref_url = None
                    stated_in_wb = None
                    archive_url = None
                    archive_date = None

                    for wikibase_prop_id, wikibase_prop_dict in reference_json['snaks'].items():
                        for x in wikibase_prop_dict:
                            if 'datavalue' in x and 'value' in x['datavalue']:
                                val = x['datavalue']['value']
                                if wikibase_prop_id == constants.WIKIBASE_REFERENCE_URL_PROPERTY:
                                    ref_url = val
                                elif wikibase_prop_id == constants.WIKIBASE_STATED_IN_PROPERTY:
                                    stated_in_wb = val.get('id') if isinstance(val, dict) else val
                                elif wikibase_prop_id == constants.WIKIBASE_ARCHIVED_URL_PROPERTY:
                                    archive_url = val
                                elif wikibase_prop_id == constants.WIKIBASE_DATE_ARCHIVED_PROPERTY:
                                    archive_date = val

                    stated_in_wd = None
                    if stated_in_wb and stated_in_wb in mappings:
                        try:
                            stated_in_wd = mappings[stated_in_wb]["wikidata"][0]["wikidata_id"]
                        except (KeyError, IndexError):
                            stated_in_wd = None

                    if not ref_url and stated_in_wb and not stated_in_wd:
                        resolved_url = resolve_stated_in_url(stated_in_wb, wbi)
                        if resolved_url:
                            ref_url = resolved_url

                    ref_snak_dict = {}
                    if ref_url:
                        ref_snak_dict['P854'] = ref_url
                    if stated_in_wd:
                        ref_snak_dict['P248'] = stated_in_wd
                    if archive_url:
                        ref_snak_dict['P1065'] = archive_url
                    if archive_date:
                        ref_snak_dict['P2960'] = archive_date

                    if ref_snak_dict:
                        wikibase_ref_list.append(ref_snak_dict)

            if len(wikibase_ref_list) > 0:
                wikibase_ref_dict[make_triple_key(claim_mapping_dict)] = wikibase_ref_list

    # Loop through ALL claims on the Wikidata item
    wikidata_claims_mapping_list = []
    for claim in wikidata_claims:
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
                claim_mapping_dict['predicate']['wikidata_id'] = claim_json['mainsnak']['property']
                if 'datavalue' in claim_json['mainsnak']:
                    if 'value' in claim_json['mainsnak']['datavalue']:
                        if 'id' in claim_json['mainsnak']['datavalue']['value']:
                            try:
                                claim_mapping_dict['object']['wikidata_id'] = claim_json['mainsnak']['datavalue']['value']['id']
                                
                                # Fast O(1) lookup using inverted mapping index
                                pred_wd = claim_mapping_dict['predicate']['wikidata_id']
                                if pred_wd in inverted_map and len(inverted_map[pred_wd]) > 0:
                                    claim_mapping_dict['predicate']['wikibase_id'] = inverted_map[pred_wd][0]
                                
                                obj_wd = claim_mapping_dict['object']['wikidata_id']
                                if obj_wd in inverted_map and len(inverted_map[obj_wd]) > 0:
                                    claim_mapping_dict['object']['wikibase_id'] = inverted_map[obj_wd][0]
                            except TypeError:
                                pass

        if len(claim_mapping_dict['predicate'].keys()) == 2 and len(claim_mapping_dict['object'].keys()) == 2:
            wikidata_claims_mapping_list.append(claim_mapping_dict)

            wikidata_ref_blocks = []
            for reference in claim.references:
                reference_json = reference.get_json()
                if 'snaks' in reference_json:
                    snaks_map = {}
                    stated_in_set = set()
                    total_snaks = 0

                    for wikidata_prop_id, wikidata_prop_dict in reference_json['snaks'].items():
                        snaks_map[wikidata_prop_id] = set()
                        for x in wikidata_prop_dict:
                            total_snaks += 1
                            if 'datavalue' in x and 'value' in x['datavalue']:
                                val = x['datavalue']['value']
                                val_str = val['id'] if isinstance(val, dict) and 'id' in val else str(val)
                                snaks_map[wikidata_prop_id].add(val_str)
                                if wikidata_prop_id == 'P248':
                                    stated_in_set.add(val_str)

                    wikidata_ref_blocks.append({
                        'snaks': snaks_map,
                        'stated_in': stated_in_set,
                        'total_snak_count': total_snaks
                    })

            if len(wikidata_ref_blocks) > 0:
                wikidata_ref_dict[make_triple_key(claim_mapping_dict)] = wikidata_ref_blocks

    # Set subtraction to get final list of claims
    if return_if_exists_but_no_ref:
        claims_to_add_dict1 = triples_diff(wikibase_claims_mapping_list, wikidata_claims_mapping_list)
        claims_to_add_dict2 = triples_intersection(wikibase_claims_mapping_list, wikidata_claims_mapping_list)
        claims_to_add_dict = claims_to_add_dict1 + [x for x in claims_to_add_dict2 if x not in claims_to_add_dict1]
    else:
        claims_to_add_dict = triples_intersection(wikibase_claims_mapping_list, wikidata_claims_mapping_list)

    # Detect if entity is an instance of human (Q5) on Wikidata
    is_human_entity = False
    instance_of_prop = getattr(constants, 'WIKIDATA_INSTANCE_OF_PROPERTY', 'P31')
    human_qid = getattr(constants, 'WIKIDATA_HUMAN_QID', 'Q5')

    p31_claims = wikidata_claims.get(instance_of_prop)
    if p31_claims:
        for c in p31_claims:
            c_json = c.get_json()
            if 'mainsnak' in c_json and 'datavalue' in c_json['mainsnak']:
                val = c_json['mainsnak']['datavalue'].get('value', {})
                if isinstance(val, dict) and val.get('id') == human_qid:
                    is_human_entity = True
                    break



    # Filter Wikibase references against existing Wikidata references to prevent duplicate/redundant additions
    refs_to_add_dict = {}
    if wikibase_ref_dict:
        for triple_key, cand_ref_list in wikibase_ref_dict.items():
            existing_wd_refs = wikidata_ref_dict.get(triple_key, [])
            valid_refs = []
            for cand_ref in cand_ref_list:
                if not is_duplicate_reference(cand_ref, existing_wd_refs):
                    valid_refs.append(cand_ref)
                else:
                    print(f"Skipping duplicate/redundant reference for {triple_key}: {cand_ref}")
            if valid_refs:
                refs_to_add_dict[triple_key] = valid_refs

    return claims_to_add_dict, refs_to_add_dict, is_human_entity





def make_triple_key(triple, wikibase=True):
    if wikibase:
        return triple['subject']['wikibase_id'] + " " + triple['predicate']['wikibase_id'] + " " + triple['object']['wikibase_id']
    else:
        return triple['subject']['wikidata_id'] + " " + triple['predicate']['wikidata_id'] + " " + triple['object']['wikidata_id']

def normalize_triple(triple):
    return (
        triple['subject']['wikibase_id'],
        triple['subject']['wikidata_id'],
        triple['predicate']['wikibase_id'],
        triple['predicate']['wikidata_id'],
        triple['object']['wikibase_id'],
        triple['object']['wikidata_id'],
    )

def normalize_po(item):
    return (
        item['predicate']['wikibase_id'],
        item['predicate']['wikidata_id'],
        item['object']['wikibase_id'],
        item['object']['wikidata_id'],
    )

def triples_diff(wikibase_list, wikidata_list):
    wb_set = {normalize_triple(t) for t in wikibase_list}
    wd_set = {normalize_triple(t) for t in wikidata_list}
    diff = wb_set - wd_set
    
    result = []
    for t in wikibase_list:
        if normalize_triple(t) in diff:
            result.append(t)
    
    return result

def triples_intersection(wikibase_list, wikidata_list):
    wb_set = {normalize_triple(t) for t in wikibase_list}
    wd_set = {normalize_triple(t) for t in wikidata_list}
    intersect = wb_set.intersection(wd_set)
    
    result = []
    for t in wikibase_list:
        if normalize_triple(t) in intersect:
            result.append(t)
    
    return result

def diff_pred_obj(dict1, dict2):
    result = {}

    for key in dict1:
        list1 = dict1.get(key, [])
        list2 = dict2.get(key, [])

        set1 = {normalize_po(x) for x in list1}
        set2 = {normalize_po(x) for x in list2}

        diff = set1 - set2

        if diff:
            result[key] = [
                {
                    'predicate': {
                        'wikibase_id': p_wb,
                        'wikidata_id': p_wd
                    },
                    'object': {
                        'wikibase_id': o_wb,
                        'wikidata_id': o_wd
                    }
                }
                for (p_wb, p_wd, o_wb, o_wd) in diff
            ]

    return result


# =========================
# SAVE CLAIMS TO ITEM
# =========================
def save_item(wikidata_id, formatted_claims_to_add, statement_history=None):
    if statement_history is None:
        statement_history = load_statement_history()

    # Wikidata login
    set_wikidata_config()
    wbi_config['MAXLAG'] = 0
    wikidata_login = get_wikidata_login()
    wdi = WikibaseIntegrator(login=wikidata_login)

    wikidata_item = None
    saved_statements = []
    for claim in formatted_claims_to_add:
        if wikidata_item is None:
            try:
                wikidata_item = wdi.item.get(entity_id=wikidata_id)
            except Exception as e:
                if "Login" in str(e) or "session" in str(e).lower():
                    print("Session error on Wikidata save fetch. Refreshing login session...")
                    wikidata_login = get_wikidata_login(force_new=True)
                    wdi = WikibaseIntegrator(login=wikidata_login)
                    wikidata_item = wdi.item.get(entity_id=wikidata_id)
                else:
                    wikidata_item = wdi.item.get(entity_id=wikidata_id)
        wikidata_item.claims.add(claim, action_if_exists=ActionIfExists.MERGE_REFS_OR_APPEND)
        
        prop_nr = getattr(claim, 'mainsnak', None) and claim.mainsnak.property_number or getattr(claim, 'prop_nr', 'Unknown')
        val = getattr(claim, 'mainsnak', None) and str(claim.mainsnak.datavalue) or getattr(claim, 'value', 'Unknown')
        saved_statements.append({
            'wikidata_id': wikidata_id,
            'property': prop_nr,
            'value': val
        })

    if wikidata_item is not None:
        try:
            wikidata_item.write()
            print("Saved %d claims for %s to Wikidata." % (len(saved_statements), wikidata_id))
            for stmt in saved_statements:
                mark_statement_added(stmt['wikidata_id'], stmt['property'], stmt['value'], statement_history)
        except Exception as e:
            if "Login" in str(e) or "session" in str(e).lower():
                print("Session expired during write for %s. Re-authenticating and retrying write..." % wikidata_id)
                wikidata_login = get_wikidata_login(force_new=True)
                wikidata_item.write(login=wikidata_login)
                print("Saved %d claims for %s to Wikidata after re-authentication." % (len(saved_statements), wikidata_id))
                for stmt in saved_statements:
                    mark_statement_added(stmt['wikidata_id'], stmt['property'], stmt['value'], statement_history)
            else:
                print("Error saving claims for %s to Wikidata: %s" % (wikidata_id, e))
                return []

    return saved_statements



# =========================
# ENTRY POINT
# =========================
def main(limit=None, max_statements=None, max_new_statements=None, start_from=None, resume=False, force=False):
    parser = argparse.ArgumentParser(description="Wikibase to Wikidata Sync Pipeline")
    parser.add_argument("--limit", "--max-items", type=int, default=None, help="Maximum number of items to process")
    parser.add_argument("--max-statements", type=int, default=None, help="Maximum total statements to upload")
    parser.add_argument("--max-new-statements", "--new-statements", type=int, default=None, help="Maximum new statements to upload")
    parser.add_argument("--start-from", "--start-at", "--start-item", type=str, default=None, help="Wikibase entity ID to start processing from (e.g. Q100 or Q19205)")
    parser.add_argument("--resume", action="store_true", help="Resume from the last processed entity checkpoint")
    parser.add_argument("--force", action="store_true", help="Force processing regardless of cache")
    
    # Parse CLI args if running from command line
    if len(sys.argv) > 1:
        args, _ = parser.parse_known_args()
        if args.limit is not None:
            limit = args.limit
        if args.max_statements is not None:
            max_statements = args.max_statements
        if args.max_new_statements is not None:
            max_new_statements = args.max_new_statements
        if args.start_from is not None:
            start_from = args.start_from
        if args.resume:
            resume = args.resume
        if args.force:
            force = args.force

    if max_new_statements is not None and max_statements is None:
        max_statements = max_new_statements

    if resume and not start_from:
        last_entity = load_last_checkpoint()
        if last_entity:
            start_from = last_entity
            print(f"Resuming pipeline from last checkpoint entity: {start_from}")
        else:
            print("No previous checkpoint found. Starting from beginning.")

    mapping_path = constants.WIKIBASE_TO_WIKIDATA_MAPPING_FILE
    mappings = load_mapping(mapping_path)
    inverted_map = build_inverted_mapping(mappings)
    cache = load_processed_cache()
    statement_history = load_statement_history()

    items_processed = 0
    total_statements_added = 0
    added_statements_log = []

    # If start_from is specified, skip items until we reach start_from
    reached_start = True if not start_from else False

    print(f"Starting pipeline run (Limit: {limit}, Max Statements: {max_statements}, Start From: {start_from}, Resume: {resume}, Force: {force})...\n")

    for wikibase_id, wikidata_dict in tqdm(mappings.items()):
        wikidata_id = wikidata_dict["wikidata"][0]["wikidata_id"]

        if not reached_start:
            if wikibase_id == start_from:
                reached_start = True
                if resume:
                    # If resuming, skip the checkpoint entity itself and start from the next entity
                    save_last_checkpoint(wikibase_id, wikidata_id)
                    continue
            else:
                continue

        if not force and is_processed(wikibase_id, cache):
            save_last_checkpoint(wikibase_id, wikidata_id)
            continue

        if limit is not None and items_processed >= limit:
            print(f"Reached item limit of {limit}. Stopping pipeline run.")
            break

        if max_statements is not None and total_statements_added >= max_statements:
            print(f"Reached statement limit of {max_statements}. Stopping pipeline run.")
            break

        print("Uploading claims for Wikibase ID %s to Wikidata ID %s..." % (wikibase_id, wikidata_id))

        if "Q" in wikidata_id:
            claims_to_add, refs_to_add, is_human_entity = process_entity(wikibase_id, wikidata_id, mappings, inverted_map=inverted_map)
            if claims_to_add or refs_to_add:
                formatted_claims_to_add = create_claims_for_wikidata(claims_to_add, refs_to_add, statement_history=statement_history, is_human_entity=is_human_entity)
                if formatted_claims_to_add:
                    if max_statements is not None:
                        remaining_quota = max_statements - total_statements_added
                        formatted_claims_to_add = formatted_claims_to_add[:remaining_quota]
                    
                    statements_saved = save_item(wikidata_id, formatted_claims_to_add, statement_history=statement_history)
                    total_statements_added += len(statements_saved)
                    added_statements_log.extend(statements_saved)



            
            mark_processed(wikibase_id, wikidata_id, cache)
            save_last_checkpoint(wikibase_id, wikidata_id)
            items_processed += 1
        elif "L" in wikidata_id:
            save_last_checkpoint(wikibase_id, wikidata_id)
            print("Not yet implemented. Exiting...")
        elif "P" in wikidata_id:
            save_last_checkpoint(wikibase_id, wikidata_id)
            print("Not yet implemented. Exiting...")
        else:
            save_last_checkpoint(wikibase_id, wikidata_id)

        time.sleep(constants.RATE_LIMIT_DELAY)

    print("\n==========================================")
    print("PIPELINE RUN SUMMARY")
    print("==========================================")
    print(f"Total Items Processed: {items_processed}")
    print(f"Total Statements Added: {total_statements_added}")
    if added_statements_log:
        print("\nDetails of Added Statements:")
        for idx, stmt in enumerate(added_statements_log, 1):
            print(f"  {idx}. Wikidata ID: {stmt['wikidata_id']}, Property: {stmt['property']}, Value: {stmt['value']}")
    print("==========================================\n")

    return added_statements_log

if __name__ == "__main__":
    main()