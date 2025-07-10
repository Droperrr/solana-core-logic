import os
from utils.signature_handler import fetch_signatures_for_token

POC_DIR = 'data/poc_signature_lists'
TOKENS_FILE = 'tokens.txt'
MAIN_TOKEN = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
MAX_TOKENS = 20
TX_LIMIT = 50000
FETCH_LIMIT_PER_CALL = 1000
DIRECTION = 'b'

def load_token_list():
    tokens = []
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, 'r', encoding='utf-8') as f:
            tokens = [line.strip() for line in f if line.strip()]
    # Гарантируем, что основной токен первый
    tokens = [MAIN_TOKEN] + [t for t in tokens if t != MAIN_TOKEN]
    return tokens[:MAX_TOKENS]

def save_signatures(mint: str, signatures: list):
    out_path = os.path.join(POC_DIR, f'{mint}.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        for sig in signatures:
            f.write(sig + '\n')

def main():
    os.makedirs(POC_DIR, exist_ok=True)
    tokens = load_token_list()
    print(f'Will process {len(tokens)} tokens:')
    for t in tokens:
        print('  ', t)
    for mint in tokens:
        print(f'--- Processing {mint} ---')
        sig_infos = fetch_signatures_for_token(
            token_mint_address=mint,
            fetch_limit_per_call=FETCH_LIMIT_PER_CALL,
            total_tx_limit=TX_LIMIT,
            direction=DIRECTION
        )
        sigs = [s['signature'] for s in sig_infos if s.get('signature')]
        save_signatures(mint, sigs)
        print(f'  Saved {len(sigs)} signatures for {mint}')
    print('Done.')

if __name__ == '__main__':
    main() 