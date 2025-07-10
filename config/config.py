import os
from dotenv import load_dotenv

# Загружаем переменные из config/.env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

# --- Пулы ключей для всех RPC-провайдеров ---
ALCHEMY_API_KEYS = os.getenv("ALCHEMY_API_KEYS", "").split(",")
QUICKNODE_API_KEYS = os.getenv("QUICKNODE_API_KEYS", "").split(",")
HELIUS_API_KEYS = os.getenv("HELIUS_API_KEYS", "").split(",")
# Убираем пустые строки
ALCHEMY_API_KEYS = [k.strip() for k in ALCHEMY_API_KEYS if k.strip()]
QUICKNODE_API_KEYS = [k.strip() for k in QUICKNODE_API_KEYS if k.strip()]
HELIUS_API_KEYS = [k.strip() for k in HELIUS_API_KEYS if k.strip()]

# Переменные для подключения к БД
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

LOG_DIR = 'logs'

# --- Feature-флаги ---
USE_RAW_FIRST_PIPELINE = os.getenv("USE_RAW_FIRST_PIPELINE", "False").lower() == "true"
SHADOW_MODE_ENABLED = os.getenv("SHADOW_MODE_ENABLED", "True").lower() == "true"

JUPITER_PRICE_API_URL = "https://quote-api.jup.ag/v6/price"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/solana/market_chart/range"