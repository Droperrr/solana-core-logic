"""
update_registry.py
Скрипт для полуавтоматического обновления PROGRAM_REGISTRY в parser/constants.py.
Позволяет добавлять новые программы/протоколы из внешних источников (SolanaFM, Solscan, Jupiter API и др.).
"""

import json
from pathlib import Path

# TODO: реализовать загрузку известных программ из внешних источников
# Пример: solana-labs/program-registry, Solscan API, собственные списки

REGISTRY_PATH = Path(__file__).parent.parent / 'parser' / 'constants.py'

if __name__ == '__main__':
    print("[update_registry] TODO: Реализовать обновление PROGRAM_REGISTRY в parser/constants.py")
    # Пример: загрузить новые программы, вывести diff, предложить добавить вручную 