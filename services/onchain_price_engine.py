import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sqlite3
import time
from typing import List, Dict, Any, Optional, Tuple
from borsh_construct import CStruct, U64, Bytes
import base64
from rpc.client import RPCClient
import logging
from analysis.data_provider import get_all_events_for_token

DB_PATH = "db/solana_db.sqlite"
DEX_NAME = "Raydium"
RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9Miu3giyaSxXZLo6av5qgGXpDfHxDunA8q1vMb"
QUOTE_TOKENS = [
    "So11111111111111111111111111111111111111112",  # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
]

# Минимальная структура Raydium AMM V4 (только нужные поля, с Bytes-пропусками)
AmmInfoLayout = CStruct(
    "status" / U64,
    "nonce" / U64,
    "skip1" / Bytes[32],  # пропуск
    "coin_decimals" / U64,
    "pc_decimals" / U64,
    "skip2" / Bytes[80],  # пропуск
    "pool_coin_token_account" / Bytes[32],
    "pool_pc_token_account" / Bytes[32],
    "coin_mint_address" / Bytes[32],
    "pc_mint_address" / Bytes[32],
    # ... структура длиннее, но нам нужны только эти поля
)

# Константы
NATIVE_SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000
SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

logger = logging.getLogger(__name__)

class OnChainPriceEngine:
    def __init__(self, db_path: str = DB_PATH, rpc_client=None):
        self.db_path = db_path
        self.rpc_client = rpc_client or RPCClient()

    def find_pools_for_token(self, token_address: str) -> List[Dict[str, Any]]:
        """
        Сначала ищет пулы в локальном кэше (dex_pools_registry).
        Если не найдено — делает RPC-запрос, сохраняет результат в кэш и возвращает.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # 1. Проверяем кэш
        cursor.execute("""
            SELECT pool_address, dex_name, token_a_mint, token_b_mint, token_a_vault, token_b_vault, last_updated
            FROM dex_pools_registry
            WHERE token_a_mint = ? OR token_b_mint = ?
        """, (token_address, token_address))
        rows = cursor.fetchall()
        if rows:
            pools = [
                {
                    "pool_address": r[0],
                    "dex_name": r[1],
                    "token_a_mint": r[2],
                    "token_b_mint": r[3],
                    "token_a_vault": r[4],
                    "token_b_vault": r[5],
                    "last_updated": r[6],
                } for r in rows
            ]
            conn.close()
            return pools
        # 2. Если нет в кэше — ищем через RPC
        pools = self._rpc_discover_pools(token_address)
        if not pools:
            print(f"[WARN] No pools found for token {token_address} (RPC may have failed or no pools exist)")
            conn.close()
            return []
        now = int(time.time())
        for pool in pools:
            cursor.execute("""
                INSERT OR REPLACE INTO dex_pools_registry
                (pool_address, dex_name, token_a_mint, token_b_mint, token_a_vault, token_b_vault, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                pool["pool_address"],
                pool.get("dex_name", DEX_NAME),
                pool["token_a_mint"],
                pool["token_b_mint"],
                pool["token_a_vault"],
                pool["token_b_vault"],
                now
            ))
        conn.commit()
        conn.close()
        return pools

    def _rpc_discover_pools(self, token_address: str) -> List[Dict[str, Any]]:
        # Для каждого quote_token делаем два запроса с memcmp по offset 69 и 101
        for quote_token in QUOTE_TOKENS:
            # 1. Поиск token_address в coinMintAddress (offset 69)
            filters = [
                {"dataSize": 691},
                {"memcmp": {"offset": 69, "bytes": token_address}}
            ]
            result = self.rpc_client.get_program_accounts(RAYDIUM_AMM_V4_PROGRAM_ID, filters=filters)
            if not result:
                continue
            pool = self._parse_first_pool(result, token_address, quote_token)
            if pool:
                return [pool]
            # 2. Поиск token_address в pcMintAddress (offset 101)
            filters = [
                {"dataSize": 691},
                {"memcmp": {"offset": 101, "bytes": token_address}}
            ]
            result = self.rpc_client.get_program_accounts(RAYDIUM_AMM_V4_PROGRAM_ID, filters=filters)
            if not result:
                continue
            pool = self._parse_first_pool(result, quote_token, token_address)
            if pool:
                return [pool]
        return []

    def _get_program_accounts_with_filters(self, program_id: str, filters: List[Dict[str, Any]]):
        # Используем реальный rpc_client
        return self.rpc_client.get_program_accounts(program_id, filters=filters)

    def _parse_first_pool(self, result, token_a, token_b):
        # result — список аккаунтов из getProgramAccounts
        for acc in result:
            data_b64 = acc["account"]["data"][0] if isinstance(acc["account"]["data"], list) else acc["account"]["data"]
            data = base64.b64decode(data_b64)
            parsed = AmmInfoLayout.parse(data)
            # Проверяем, что mint-адреса совпадают с ожидаемыми
            coin_mint = self._to_base58(parsed.coin_mint_address)
            pc_mint = self._to_base58(parsed.pc_mint_address)
            if (coin_mint == token_a and pc_mint == token_b) or (coin_mint == token_b and pc_mint == token_a):
                return {
                    "pool_address": acc["pubkey"],
                    "dex_name": DEX_NAME,
                    "token_a_mint": coin_mint,
                    "token_b_mint": pc_mint,
                    "token_a_vault": self._to_base58(parsed.pool_coin_token_account),
                    "token_b_vault": self._to_base58(parsed.pool_pc_token_account),
                }
        return None

    def _to_base58(self, b: bytes) -> str:
        # Преобразует 32-байтовый адрес в base58-строку
        import base58
        return base58.b58encode(b).decode()

    def fetch_pool_state(self, pool_address: str) -> Optional[Dict[str, Any]]:
        """
        Заглушка: получить состояние пула (резервы vault-ов) через RPC.
        """
        # TODO: реализовать
        return None

    def calculate_price(self, pool_state: Dict[str, Any], token_address: str) -> Optional[float]:
        """
        Заглушка: рассчитать цену токена на основе резервов пула.
        """
        # TODO: реализовать
        return None

    def _find_balance(self, balances: list, vault_address: str) -> Optional[dict]:
        """Вспомогательная функция для поиска баланса по адресу хранилища."""
        for balance in balances:
            if balance.get('pubkey') == vault_address:
                return balance.get('uiTokenAmount')
        return None

    def _get_swap_details_from_inner_instructions(
        self, inner_instructions: List[Dict[str, Any]], account_keys: List[str], signature_for_log: str
    ) -> Optional[Dict[str, Any]]:
        """
        Улучшенный анализ внутренних инструкций для поиска деталей обмена.
        Возвращает словарь с деталями и ИНДЕКС родительской инструкции.
        """
        logger.info(f"[{signature_for_log}] ===== DETAILED INNER INSTRUCTIONS ANALYSIS =====")
        logger.info(f"[{signature_for_log}] Initiator (fee_payer): {account_keys[0] if account_keys else 'None'}")
        logger.info(f"[{signature_for_log}] Total account_keys: {len(account_keys)}")
        logger.info(f"[{signature_for_log}] Total inner_instructions groups: {len(inner_instructions)}")

        initiator = account_keys[0] if account_keys else None

        if not initiator:
            logger.warning(f"[{signature_for_log}] Could not determine initiator from transaction account keys.")
            return None
            
        if not inner_instructions:
            logger.debug(f"[{signature_for_log}] No inner instructions found.")
            return None

        for group_idx, inner_instruction_group in enumerate(inner_instructions):
            parent_index = inner_instruction_group.get("index")
            instructions = inner_instruction_group.get("instructions", [])
            logger.info(f"[{signature_for_log}] --- Group {group_idx + 1} (parent_index: {parent_index}) ---")
            logger.info(f"[{signature_for_log}] Instructions in group: {len(instructions)}")
            
            transfers = []
            for i, instruction in enumerate(instructions):
                logger.info(f"[{signature_for_log}]   Instruction {i + 1}: {instruction}")
                
                # Проверяем, что инструкция относится к программе токенов
                program_id_index = instruction.get('programIdIndex')
                program_id = account_keys[program_id_index] if program_id_index is not None and program_id_index < len(account_keys) else None
                
                logger.info(f"[{signature_for_log}]     program_id_index: {program_id_index}")
                logger.info(f"[{signature_for_log}]     program_id: {program_id}")
                logger.info(f"[{signature_for_log}]     SPL_TOKEN_PROGRAM_ID: {SPL_TOKEN_PROGRAM_ID}")
                
                if program_id != SPL_TOKEN_PROGRAM_ID:
                    logger.info(f"[{signature_for_log}]     SKIP: Not SPL token program")
                    continue

                parsed_instr = instruction.get("parsed", {})
                logger.info(f"[{signature_for_log}]     parsed_instr: {parsed_instr}")
                
                if not parsed_instr or parsed_instr.get("type") not in ("transfer", "transferChecked"):
                    logger.info(f"[{signature_for_log}]     SKIP: Not a transfer instruction")
                    continue

                info = parsed_instr.get("info", {})
                amount = int(info.get("amount", "0"))
                if amount == 0:
                    logger.info(f"[{signature_for_log}]     SKIP: Zero amount")
                    continue

                mint = info.get("mint", NATIVE_SOL_MINT)
                
                transfer_details = {
                    "source": info.get("source"),
                    "destination": info.get("destination"),
                    "amount": amount,
                    "mint": mint,
                }
                transfers.append(transfer_details)
                logger.info(f"[{signature_for_log}]     ✅ Found valid token transfer: {transfer_details}")

            logger.info(f"[{signature_for_log}]   Total transfers found in group: {len(transfers)}")
            
            if len(transfers) != 2:
                logger.info(f"[{signature_for_log}]   SKIP: Expected 2 transfers, found {len(transfers)}")
                continue

            t1, t2 = transfers
            logger.info(f"[{signature_for_log}]   Transfer 1: {t1}")
            logger.info(f"[{signature_for_log}]   Transfer 2: {t2}")
            
            # Check if transfers form a direct exchange
            is_direct_exchange = (t1["source"] == t2["destination"] and t2["source"] == t1["destination"])
            logger.info(f"[{signature_for_log}]   Direct exchange check: {is_direct_exchange}")
            
            if not is_direct_exchange:
                logger.info(f"[{signature_for_log}]   SKIP: Transfers are not a direct exchange")
                continue

            # Determine direction based on initiator
            if t1["source"] == initiator:
                token_in, amount_in, token_out, amount_out = t1["mint"], t1["amount"], t2["mint"], t2["amount"]
                logger.info(f"[{signature_for_log}]   ✅ Initiator is source in transfer 1")
            elif t2["source"] == initiator:
                token_in, amount_in, token_out, amount_out = t2["mint"], t2["amount"], t1["mint"], t1["amount"]
                logger.info(f"[{signature_for_log}]   ✅ Initiator is source in transfer 2")
            else:
                logger.info(f"[{signature_for_log}]   SKIP: Initiator '{initiator}' not found as source in any transfer")
                continue
            
            result = {
                "token_in": token_in, 
                "amount_in": amount_in,
                "token_out": token_out, 
                "amount_out": amount_out,
                "parent_instruction_index": parent_index,
                "initiator": initiator
            }
            logger.info(f"[{signature_for_log}] ✅ SUCCESS: SWAP details identified: {result}")
            return result

        logger.info(f"[{signature_for_log}] ❌ FAILED: No SWAP pattern found in any instruction group")
        return None
    
    def _get_swap_details_from_token_balances(
        self, pre_token_balances: List[Dict], post_token_balances: List[Dict], initiator: str, signature_for_log: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fallback-механизм: определяет детали обмена на основе изменения 
балансов по owner'ам.
        """
        logger.info(f"[{signature_for_log}] ===== FALLBACK: TOKEN BALANCE ANALYSIS BY OWNER =====")
        logger.info(f"[{signature_for_log}] Initiator: {initiator}")
        logger.info(f"[{signature_for_log}] Pre-token-balances count: {len(pre_token_balances)}")
        logger.info(f"[{signature_for_log}] Post-token-balances count: {len(post_token_balances)}")

        # 1. Создаем словари балансов по owner и mint
        logger.info(f"[{signature_for_log}] --- Building owner-based balance maps ---")
        
        pre_balances = {}  # {owner: {mint: amount}}
        post_balances = {}  # {owner: {mint: amount}}
        
        # Обрабатываем pre-balances
        for balance in pre_token_balances:
            if balance.get('uiTokenAmount'):
                mint = balance.get('mint')
                amount = int(balance.get('uiTokenAmount', {}).get('amount', '0'))
                owner = balance.get('owner')
                if amount > 0:
                    if owner not in pre_balances:
                        pre_balances[owner] = {}
                    pre_balances[owner][mint] = amount
                    logger.info(f"[{signature_for_log}] Pre: owner={owner}, mint={mint}, amount={amount}")
        
        # Обрабатываем post-balances
        for balance in post_token_balances:
            if balance.get('uiTokenAmount'):
                mint = balance.get('mint')
                amount = int(balance.get('uiTokenAmount', {}).get('amount', '0'))
                owner = balance.get('owner')
                if amount > 0:
                    if owner not in post_balances:
                        post_balances[owner] = {}
                    post_balances[owner][mint] = amount
                    logger.info(f"[{signature_for_log}] Post: owner={owner}, mint={mint}, amount={amount}")

        logger.info(f"[{signature_for_log}] Pre-balances by owner: {pre_balances}")
        logger.info(f"[{signature_for_log}] Post-balances by owner: {post_balances}")

        if not pre_balances and not post_balances:
            logger.info(f"[{signature_for_log}] ❌ FAILED: No token balances found")
            return None

        # 2. Вычисляем изменения балансов по каждому owner
        logger.info(f"[{signature_for_log}] --- Calculating balance changes by owner ---")
        
        all_owners = set(pre_balances.keys()) | set(post_balances.keys())
        all_mints = set()
        for owner_balances in pre_balances.values():
            all_mints.update(owner_balances.keys())
        for owner_balances in post_balances.values():
            all_mints.update(owner_balances.keys())
        
        logger.info(f"[{signature_for_log}] All owners: {all_owners}")
        logger.info(f"[{signature_for_log}] All mints: {all_mints}")
        
        owner_changes = {}  # {owner: {mint: change}}
        
        for owner in all_owners:
            owner_changes[owner] = {}
            for mint in all_mints:
                pre_amount = pre_balances.get(owner, {}).get(mint, 0)
                post_amount = post_balances.get(owner, {}).get(mint, 0)
                change = post_amount - pre_amount
                if change != 0:
                    owner_changes[owner][mint] = change
                    logger.info(f"[{signature_for_log}] Owner {owner}, mint {mint}: pre={pre_amount}, post={post_amount}, change={change}")

        logger.info(f"[{signature_for_log}] Owner changes: {owner_changes}")

        # 3. Ищем owner'ов с swap-паттерном (ровно 1 inflow и 1 outflow)
        logger.info(f"[{signature_for_log}] --- Looking for swap patterns ---")
        
        swap_owners = []
        for owner, changes in owner_changes.items():
            inflows = [(mint, change) for mint, change in changes.items() if change > 0]
            outflows = [(mint, change) for mint, change in changes.items() if change < 0]
            
            logger.info(f"[{signature_for_log}] Owner {owner}: inflows={inflows}, outflows={outflows}")
            
            if len(inflows) == 1 and len(outflows) == 1:
                swap_owners.append({
                    'owner': owner,
                    'inflow_mint': inflows[0][0],
                    'inflow_amount': inflows[0][1],
                    'outflow_mint': outflows[0][0],
                    'outflow_amount': abs(outflows[0][1])
                })
                logger.info(f"[{signature_for_log}] ✅ Found swap pattern for owner {owner}")

        logger.info(f"[{signature_for_log}] Swap owners found: {swap_owners}")

        # 4. Определяем направление обмена на основе инициатора
        if len(swap_owners) == 0:
            logger.info(f"[{signature_for_log}] ❌ FAILED: No swap patterns found")
            return None
        
        # Ищем swap, где инициатор участвует
        initiator_swap = None
        for swap in swap_owners:
            if swap['owner'] == initiator:
                initiator_swap = swap
                break
        
        if not initiator_swap:
            # Если инициатор не найден, берем первый swap
            initiator_swap = swap_owners[0]
            logger.info(f"[{signature_for_log}] Initiator not found in swaps, using first swap: {initiator_swap}")

        # Определяем направление: что инициатор отдал (outflow) - это token_in
        token_in = initiator_swap['outflow_mint']
        amount_in = initiator_swap['outflow_amount']
        token_out = initiator_swap['inflow_mint']
        amount_out = initiator_swap['inflow_amount']

        logger.info(f"[{signature_for_log}] ✅ SUCCESS: Swap direction determined")
        logger.info(f"[{signature_for_log}] Token in: {token_in} (amount: {amount_in})")
        logger.info(f"[{signature_for_log}] Token out: {token_out} (amount: {amount_out})")

        result = {
            "token_in": token_in,
            "amount_in": amount_in,
            "token_out": token_out,
            "amount_out": amount_out,
            "source": "token_balances_by_owner" # Источник данных для отладки
        }
        logger.info(f"[{signature_for_log}] ✅ SUCCESS: Fallback SWAP details identified: {result}")
        return result

    def _get_token_decimals(self, mint_address: str, raw_tx: Dict[str, Any]) -> Optional[int]:
        """Получает 'decimals' для SPL-токена из метаданных транзакции."""
        if mint_address == NATIVE_SOL_MINT:
            return 9 # У SOL 9 знаков после запятой
            
        # Ищем в preTokenBalances
        for balance in raw_tx.get('meta', {}).get('preTokenBalances', []):
            if balance.get('mint') == mint_address:
                return balance.get('uiTokenAmount', {}).get('decimals')
        # Ищем в postTokenBalances
        for balance in raw_tx.get('meta', {}).get('postTokenBalances', []):
            if balance.get('mint') == mint_address:
                return balance.get('uiTokenAmount', {}).get('decimals')

        return None

    def calculate_price_from_swap(self, raw_tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Рассчитывает цену исполнения и объем в SOL на основе `innerInstructions`.
        """
        meta = raw_tx.get("meta", {})
        inner_instructions = meta.get("innerInstructions")
        if not inner_instructions:
            logger.debug(f"TX {raw_tx.get('transaction', {}).get('signatures', ['unknown'])[0]}: No inner instructions found.")
            return None

        account_keys = raw_tx.get('transaction', {}).get('message', {}).get('accountKeys', [])
        signature = raw_tx.get('transaction', {}).get('signatures', ['unknown'])[0]
        swap_details = self._get_swap_details_from_inner_instructions(inner_instructions, account_keys, signature)
        if not swap_details:
            logger.debug(f"TX {raw_tx.get('transaction', {}).get('signatures', ['unknown'])[0]}: Could not extract swap details from inner instructions.")
            return None

        token_in = swap_details['token_in']
        amount_in = swap_details['amount_in']
        token_out = swap_details['token_out']
        amount_out = swap_details['amount_out']

        price_in_sol = None
        volume_in_sol = None
        
        token_in_decimals = self._get_token_decimals(token_in, raw_tx)
        token_out_decimals = self._get_token_decimals(token_out, raw_tx)

        if token_in_decimals is None or token_out_decimals is None:
            logger.warning(f"Could not determine decimals for swap in tx.")
            return None

        norm_amount_in = amount_in / (10 ** token_in_decimals)
        norm_amount_out = amount_out / (10 ** token_out_decimals)

        if token_in == NATIVE_SOL_MINT:
            volume_in_sol = norm_amount_in
            if norm_amount_out > 0:
                price_in_sol = norm_amount_in / norm_amount_out  # SOL per Token
        elif token_out == NATIVE_SOL_MINT:
            volume_in_sol = norm_amount_out
            if norm_amount_in > 0:
                price_in_sol = norm_amount_out / norm_amount_in  # SOL per Token
        
        if price_in_sol is None:
            return None

        return {
            "price_in_sol": price_in_sol,
            "volume_in_sol": volume_in_sol,
            "token_in": token_in,
            "amount_in": amount_in,
            "token_out": token_out,
            "amount_out": amount_out,
        }

    def is_significant_dump(
        self, current_price_in_sol: float, previous_price_in_sol: float, volume_in_sol: float
    ) -> bool:
        """
        Определяет, является ли изменение цены значимым дампом.
        """
        if previous_price_in_sol == 0:
            return False

        price_drop_percentage = (previous_price_in_sol - current_price_in_sol) / previous_price_in_sol

        if price_drop_percentage < 0.30:
            return False

        if volume_in_sol < 0.1:
            return False

        if price_drop_percentage > 0.5 and volume_in_sol > 5:
            return True
        
        if 0.3 <= price_drop_percentage <= 0.5 and volume_in_sol > 20:
            return True
            
        return False

    def analyze_transaction_for_dump(self, signature: str, token_address: str, threshold: float = 0.5):
        """
        Анализирует транзакцию по сигнатуре: если цена токена резко упала (дамп), возвращает (True, dump_data).
        """
        # Получаем raw_json транзакции из БД
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
        row = cursor.fetchone()
        if not row:
            return False, None
        import json
        raw_tx = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        # Для простоты: ищем swap_event в enriched_data (если есть)
        swap_event = None
        enriched = raw_tx.get('enriched_data') or raw_tx.get('enriched')
        if isinstance(enriched, list):
            for event in enriched:
                if event.get('event_type') == 'SWAP' and (
                    event.get('token_a_mint') == token_address or event.get('token_b_mint') == token_address):
                    swap_event = event
                    break
        if not swap_event:
            return False, None
        # Получаем цену до и после (или используем calculate_price_from_swap)
        price = self.calculate_price_from_swap(raw_tx)
        # Для демо: если цена < threshold, считаем дампом
        if price is not None and price['price_in_sol'] < threshold:
            dump_data = {'price_drop_percent': round(100 * (1 - price['price_in_sol']), 2)}
            return True, dump_data
        return False, None

    def find_first_dump(self, token_address: str, dump_threshold: float = 0.5) -> Optional[dict]:
        """
        Находит первую транзакцию-дамп для токена и сохраняет результат в token_lifecycle.
        Возвращает словарь с деталями дампа или None.
        """
        logger.info(f"Начало анализа для токена {token_address}")
        # Проверка идемпотентности
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT first_dump_signature FROM token_lifecycle WHERE token_address = ?", (token_address,))
        row = cursor.fetchone()
        if row and row[0]:
            logger.info(f"Дамп уже найден ранее для токена {token_address}: {row[0]}")
            conn.close()
            return None
        # Получаем все события
        try:
            df = get_all_events_for_token(token_address)
        except Exception as e:
            logger.error(f"Ошибка при получении событий: {e}")
            conn.close()
            return None
        if df.empty:
            logger.warning(f"Нет событий для токена {token_address}")
            conn.close()
            return None
        swaps = df[df['event_type'] == 'SWAP'].sort_values('block_time')
        logger.info(f"Найдено {len(swaps)} SWAP-событий для токена {token_address}")
        previous_price = None
        previous_signature = None
        previous_block_time = None
        for idx, row in swaps.iterrows():
            signature = row['signature']
            block_time = int(row['block_time'].timestamp()) if hasattr(row['block_time'], 'timestamp') else int(row['block_time'])
            # Получаем сырую транзакцию
            try:
                import json
                raw_tx = None
                # Пытаемся извлечь raw_json из event_data_raw (или из БД при необходимости)
                if 'event_data_raw' in row and row['event_data_raw']:
                    try:
                        raw_tx = json.loads(row['event_data_raw']) if isinstance(row['event_data_raw'], str) else row['event_data_raw']
                    except Exception as e:
                        logger.warning(f"Не удалось распарсить event_data_raw для {signature}: {e}")
                if raw_tx is None:
                    # Фоллбек: грузим из таблицы transactions
                    cur2 = conn.cursor()
                    cur2.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
                    tx_row = cur2.fetchone()
                    if tx_row and tx_row[0]:
                        raw_tx = json.loads(tx_row[0]) if isinstance(tx_row[0], str) else tx_row[0]
                if raw_tx is None:
                    logger.warning(f"Не удалось получить raw_tx для {signature}, пропуск")
                    continue
                price = self.calculate_price_from_swap(raw_tx)
                if price is None:
                    logger.warning(f"Не удалось рассчитать цену для транзакции {signature}")
                    continue
            except Exception as e:
                logger.warning(f"Ошибка при обработке транзакции {signature}: {e}")
                continue
            if previous_price is not None:
                if self.is_significant_dump(price['price_in_sol'], previous_price['price_in_sol'], price['volume_in_sol']):
                    price_drop_percent = round(100 * (1 - price['price_in_sol'] / previous_price['price_in_sol']), 2)
                    logger.info(f"!!! DUMP DETECTED for token {token_address} in transaction {signature} !!!")
                    logger.info(f"Dump: prev_price={previous_price['price_in_sol']}, dump_price={price['price_in_sol']}, drop={price_drop_percent}%")
                    # Сохраняем в token_lifecycle
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO token_lifecycle (
                                token_address, first_dump_signature, first_dump_time, first_dump_price_drop_percent, last_processed_signature
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            token_address,
                            signature,
                            block_time,
                            price_drop_percent,
                            signature
                        ))
                        conn.commit()
                    except Exception as e:
                        logger.error(f"Ошибка при записи в token_lifecycle: {e}")
                    result = {
                        'signature': signature,
                        'block_time': block_time,
                        'price_drop_percent': price_drop_percent,
                        'previous_price': previous_price['price_in_sol'],
                        'dump_price': price['price_in_sol']
                    }
                    conn.close()
                    return result
            previous_price = price
            previous_signature = signature
            previous_block_time = block_time
        logger.info(f"Дамп не найден для токена {token_address}")
        conn.close()
        return None

if __name__ == "__main__":
    # Тестовый запуск поиска пула для токена группы Б
    test_token = "Fm163V3Miu3giyaSxXZLo6av5qgGXpDfHxDunA8q1vMb"
    engine = OnChainPriceEngine()
    pools = engine.find_pools_for_token(test_token)
    if pools:
        print("Пул найден:")
        for k, v in pools[0].items():
            print(f"  {k}: {v}")
    else:
        print("Пул не найден для токена", test_token) 