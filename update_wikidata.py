import constants
import json
import sys
import time
from tqdm import tqdm
from wikibaseintegrator import wbi_login, WikibaseIntegrator
from wikibaseintegrator.datatypes import String, Item, URL
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
    exit()


# =========================
# CREATE CLAIM OBJECT
# =========================
def create_claims_for_wikidata(claims, refs): # Currently assumes that claim is totally missing; need another version that adds references if claim exists but reference is missing
    
    formatted_claims = []
    formatted_refs = {}

    for claim in claims:
        triple_key = make_triple_key(claim)
        if triple_key in refs.keys():
            claim_refs = refs[triple_key]
            formatted_refs_list = references.References()
            for ref in claim_refs:
                formatted_ref = references.Reference()
                if ref['predicate']['wikidata_id'] == 'P854':
                    formatted_ref.add(URL(prop_nr='P854', value=ref['object']['wikidata_id']))
                elif ref['predicate']['wikidata_id'] == 'P248':
                    formatted_ref.add(Item(prop_nr='P248', value=ref['object']['wikidata_id']))
                if formatted_ref.__len__() > 0:
                    formatted_refs_list.add(formatted_ref)
            if formatted_refs_list.__len__() > 0:
                formatted_refs[triple_key] = formatted_refs_list

            formatted_claim = None
            try: 
                formatted_claim = Item(prop_nr=claim['predicate']['wikidata_id'], value=claim['object']['wikidata_id'], references=formatted_refs_list)
                formatted_claims.append(formatted_claim)
            except ValueError:
                pass # This skips certain things for now, such as adding claims where the object is something other than an item.

    return formatted_claims

# =========================
# MAIN PROCESS FUNCTION
# =========================
def process_entity(wikibase_id, wikidata_id, mappings, only_return_if_ref=True, return_if_exists_but_no_ref=True):

    # Wikidata login
    set_wikidata_config()
    wikidata_login = wbi_login.Login(user=constants.WIKIDATA_CREDENTIAL_USERNAME, password=constants.WIKIDATA_CREDENTIAL_PASSWORD)
    wdi = WikibaseIntegrator(login=wikidata_login)
    wikidata_item = wdi.item.get(entity_id=wikidata_id)
    wikidata_claims = wikidata_item.claims
    wikidata_ref_dict = {}

    # Wikibase login
    set_wikibase_config()
    wikibase_login = wbi_login.Login(user=constants.WIKIBASE_CREDENTIAL_USERNAME, password=constants.WIKIBASE_CREDENTIAL_PASSWORD)
    wbi = WikibaseIntegrator(login=wikibase_login)
    wikibase_item = wbi.item.get(entity_id=wikibase_id)
    wikibase_claims = wikibase_item.claims
    wikibase_ref_dict = {}

    # Loop through ALL claims on the Wikibase item
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
                    ref_val1 = None
                    ref_val2 = None
                    for wikibase_prop_id, wikibase_prop_dict in reference_json['snaks'].items():
                        for x in wikibase_prop_dict:
                            if wikibase_prop_id == constants.WIKIBASE_REFERENCE_URL_PROPERTY:
                                ref_val1 = x['datavalue']['value']
                            elif wikibase_prop_id == constants.WIKIBASE_STATED_IN_PROPERTY:
                                ref_val2 = x['datavalue']['value']['id']
                    if ref_val1 or ref_val2:
                        if ref_val1 and ref_val2:
                            print(ref_val1)
                            print(ref_val2)
                            print("Issue: need to fix if both a reference-URL and stated-in property are present. Exiting...")
                            exit()
                        elif ref_val1:
                            wikibase_ref_list.append({
                                'predicate': {
                                    'wikibase_id': constants.WIKIBASE_REFERENCE_URL_PROPERTY,
                                    'wikidata_id': mappings[constants.WIKIBASE_REFERENCE_URL_PROPERTY]["wikidata"][0]["wikidata_id"]
                                },
                                'object': {
                                    'wikibase_id': ref_val1,
                                    'wikidata_id': ref_val1
                                }
                            })
                        elif ref_val2:
                            try:
                                wikibase_ref_list.append({
                                    'predicate': {
                                        'wikibase_id': constants.WIKIBASE_STATED_IN_PROPERTY,
                                        'wikidata_id': mappings[constants.WIKIBASE_STATED_IN_PROPERTY]["wikidata"][0]["wikidata_id"]
                                    },
                                    'object': {
                                        'wikibase_id': ref_val2,
                                        'wikidata_id': mappings[ref_val2]["wikidata"][0]["wikidata_id"]
                                    }
                                })
                            except KeyError: # This shouldn't be necessary but I'm getting an error and can't figure out where if I don't include it.
                                pass

            if len(wikibase_ref_list) > 0:
                wikibase_ref_dict[make_triple_key(claim_mapping_dict)] = wikibase_ref_list

    # Loop through ALL claims on the Wikidata item
    wikidata_claims_mapping_list = []
    for claim in wikidata_claims:
        claim_json = claim.get_json()
        
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
                                
                                # This can absolutely be made more efficient, it's basically glorified pseudocode.
                                for wikibase_id_2, wikibase_mapping_dict in mappings.items():
                                    wikidata_id_2 = wikibase_mapping_dict["wikidata"][0]["wikidata_id"]
                                    if claim_mapping_dict['predicate']['wikidata_id'] == wikidata_id_2:
                                        claim_mapping_dict['predicate']['wikibase_id'] = wikibase_id_2
                                    if claim_mapping_dict['object']['wikidata_id'] == wikidata_id_2:
                                        claim_mapping_dict['object']['wikibase_id'] = wikibase_id_2
                            except TypeError:
                                pass

        if len(claim_mapping_dict['predicate'].keys()) == 2 and len(claim_mapping_dict['object'].keys()) == 2:
            wikidata_claims_mapping_list.append(claim_mapping_dict)

            wikidata_ref_list = []
            for reference in claim.references:
                reference_json = reference.get_json()
                if 'snaks' in reference_json:
                    ref_val1 = None
                    ref_val2 = None
                    for wikidata_prop_id, wikidata_prop_dict in reference_json['snaks'].items():
                        for x in wikidata_prop_dict:
                            if wikidata_prop_id == 'P854':
                                ref_val1 = x['datavalue']['value']
                            elif wikidata_prop_id == 'P248':
                                ref_val2 = x['datavalue']['value']['id']
                    if ref_val1 or ref_val2:
                        if ref_val1 and ref_val2:
                            print(ref_val1)
                            print(ref_val2)
                            print("Issue: need to fix if both a reference-URL and stated-in property are present. Exiting...")
                            exit()
                        elif ref_val1:
                            wikidata_ref_list.append({
                                'predicate': {
                                    'wikibase_id': constants.WIKIBASE_REFERENCE_URL_PROPERTY, 
                                    'wikidata_id': 'P854'
                                },
                                'object': {
                                    'wikibase_id': ref_val1,
                                    'wikidata_id': ref_val1
                                }
                            })
                        elif ref_val2:
                            # This can absolutely be made more efficient, it's basically glorified pseudocode.
                            object_wikibase_id = None
                            for wikibase_id_2, wikibase_mapping_dict in mappings.items():
                                wikidata_id_2 = wikibase_mapping_dict["wikidata"][0]["wikidata_id"]
                                if ref_val2 == wikidata_id_2:
                                    object_wikibase_id = wikibase_id_2

                            if object_wikibase_id:
                                wikibase_ref_list.append({
                                    'predicate': {
                                        'wikibase_id': constants.WIKIBASE_STATED_IN_PROPERTY,
                                        'wikidata_id': 'P248'
                                    },
                                    'object': {
                                        'wikibase_id': object_wikibase_id,
                                        'wikidata_id': ref_val2
                                    }
                                })

            if len(wikidata_ref_list) > 0:
                wikidata_ref_dict[make_triple_key(claim_mapping_dict)] = wikibase_ref_list

    # Set subtraction to get final list of claims
    if return_if_exists_but_no_ref:
        claims_to_add_dict1 = triples_diff(wikibase_claims_mapping_list, wikidata_claims_mapping_list)
        claims_to_add_dict2 = triples_intersection(wikibase_claims_mapping_list, wikidata_claims_mapping_list)
        claims_to_add_dict = claims_to_add_dict1 + [x for x in claims_to_add_dict2 if x not in claims_to_add_dict1]
    else:
        claims_to_add_dict = triples_intersection(wikibase_claims_mapping_list, wikidata_claims_mapping_list)

    # Add references to final list?
    refs_to_add_dict = {}
    has_ref = False
    if wikibase_ref_dict:
        refs_to_add_dict = diff_pred_obj(wikibase_ref_dict, wikidata_ref_dict)
        if only_return_if_ref:
            has_ref = True

    if has_ref and only_return_if_ref:
        return claims_to_add_dict, refs_to_add_dict
    else:
        return claims_to_add_dict, refs_to_add_dict
    

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
    
    # Convert back to original structure if needed
    result = []
    for t in wikibase_list:
        if normalize_triple(t) in diff:
            result.append(t)
    
    return result

def triples_intersection(wikibase_list, wikidata_list):
    wb_set = {normalize_triple(t) for t in wikibase_list}
    wd_set = {normalize_triple(t) for t in wikidata_list}
    intersect = wb_set.intersection(wd_set)
    
    # Convert back to original structure if needed
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
            # convert back to original structure
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
# ENTRY POINT
# =========================
def main():
    mapping_path = constants.WIKIBASE_TO_WIKIDATA_MAPPING_FILE
    mappings = load_mapping(mapping_path)

    for wikibase_id, wikidata_dict in tqdm(mappings.items()):
        wikidata_id = wikidata_dict["wikidata"][0]["wikidata_id"]
        print("Uploading claims for Wikibase ID %s to Wikidata ID %s..." % (wikibase_id, wikidata_id))

        if "Q" in wikidata_id:
            claims_to_add, refs_to_add = process_entity(wikibase_id, wikidata_id, mappings)
            if refs_to_add:
                formatted_claims_to_add = create_claims_for_wikidata(claims_to_add, refs_to_add)
                if formatted_claims_to_add:
                    save_item(wikidata_id, formatted_claims_to_add)
        elif "L" in wikidata_id:
            print("Not yet implemented. Exiting...")
        elif "P" in wikidata_id:
            print("Not yet implemented. Exiting...")
        else:
            pass

        # Be polite.
        time.sleep(2)

if __name__ == "__main__":
    main()