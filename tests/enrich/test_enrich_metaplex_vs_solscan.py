import json
import os

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def find_metaplex_enrich_block(parsed_instructions):
    for instr in parsed_instructions:
        if instr.get('program_id', '').startswith('metaqbxx'):
            enrich = instr.get('details', {}).get('enrich')
            if enrich:
                return enrich
    return None

def compare_dicts(d1, d2, path=''):
    mismatches = []
    for k in set(d1.keys()).union(d2.keys()):
        v1, v2 = d1.get(k), d2.get(k)
        if isinstance(v1, dict) and isinstance(v2, dict):
            mismatches += compare_dicts(v1, v2, path + '.' + k)
        elif v1 != v2:
            mismatches.append(f"Mismatch at {path}.{k}: {v1} != {v2}")
    return mismatches

def main():
    parsed = load_json('parsed_instructions_current.json')
    solscan = load_json('solscan_reference/4JpccouRXyZqsrRFdkTpX2nxFRXoH6UwW7zjR2QHsd51F2chC9PxK3y7pPyQv2pjESFgJiA5xRTCgfLb7Bqdt3B.json')
    enrich = find_metaplex_enrich_block(parsed)
    if not enrich:
        print('No Metaplex enrich block found!')
        return
    # Сравниваем только ключевые блоки
    solscan_instr = None
    for instr in solscan.get('instructions', []):
        if instr.get('program') == 'Metaplex Token Metadata' and instr.get('type') == 'createMetadataAccountV3':
            solscan_instr = instr
            break
    if not solscan_instr:
        print('No createMetadataAccountV3 in Solscan reference!')
        return
    # details.nft_metadata
    mismatches = compare_dicts(
        enrich.get('details', {}).get('nft_metadata', {}),
        solscan_instr.get('data', {}) or {}
    )
    # participants (mint, update_authority, ...)
    for k in ['mint', 'update_authority']:
        v1 = enrich.get('participants', {}).get(k)
        v2 = solscan_instr.get(k)
        if v1 != v2:
            mismatches.append(f"Mismatch at participants.{k}: {v1} != {v2}")
    # activity_pattern.collection_mint
    v1 = enrich.get('activity_pattern', {}).get('collection_mint')
    solscan_collection = (solscan_instr.get('data', {}) or {}).get('collection')
    v2 = solscan_collection.get('key') if isinstance(solscan_collection, dict) else None
    if v1 != v2:
        mismatches.append(f"Mismatch at activity_pattern.collection_mint: {v1} != {v2}")
    if mismatches:
        print('MISMATCHES:')
        for m in mismatches:
            print('  -', m)
    else:
        print('All key fields match Solscan reference!')

if __name__ == '__main__':
    main() 