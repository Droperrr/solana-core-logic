# services

Модуль сервисных компонентов для ценового анализа и on-chain данных BANT системы.

## 🎯 Назначение

Предоставляет специализированные сервисы для:
- **On-chain ценовых данных** и анализа рыночной активности
- **Интеграции с внешними API** для ценовой информации
- **Real-time мониторинга цен** и ликвидности
- **Поддержки аналитических модулей** ценовыми данными

## 🏗️ Архитектура и компоненты

### Ключевые сервисы
- **`onchain_price_engine.py`** - Движок анализа on-chain ценовых данных
- **`price_data_provider.py`** - Провайдер ценовых данных из различных источников

## 💹 On-Chain Price Engine

### Назначение
Анализирует ценовые движения токенов на основе on-chain активности, транзакций свопов и изменений ликвидности.

### Ключевые функции
- **Расчет цен** на основе swap-транзакций
- **Анализ price impact** крупных сделок
- **Детекция ценовых аномалий** и дампов
- **Корреляция** ценовых движений с событиями ликвидности

### Использование
```python
from services.onchain_price_engine import OnChainPriceEngine

engine = OnChainPriceEngine()

# Анализ ценовых движений токена
price_analysis = engine.analyze_token_price_movements(
    token_mint="So11111111111111111111111111111111111111112",
    time_range="24h"
)

# Детекция дампов по порогу изменения цены
dump_events = engine.detect_price_dumps(
    threshold_percent=20,
    time_window="1h"
)

# Анализ price impact крупных свопов
impact_analysis = engine.analyze_swap_price_impact(
    min_usd_value=10000
)
```

### Метрики и аналитика
- **OHLCV данные** по временным интервалам
- **Volume Weighted Average Price (VWAP)**
- **Price Impact** для крупных транзакций
- **Slippage анализ** по DEX протоколам
- **Liquidity depth** в различных пулах

## 📊 Price Data Provider

### Назначение
Унифицированный интерфейс для получения ценовых данных из различных источников (on-chain, внешние API, агрегаторы).

### Поддерживаемые источники
- **On-chain swaps** - Raydium, Jupiter, Orca
- **External APIs** - CoinGecko, CoinMarketCap, DexScreener
- **DEX aggregators** - Jupiter API, 1inch API
- **WebSocket feeds** - Real-time ценовые потоки

### Использование
```python
from services.price_data_provider import PriceDataProvider

provider = PriceDataProvider()

# Текущая цена токена из различных источников
current_price = provider.get_current_price(
    token_mint="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    vs_currency="USDC",
    sources=["onchain", "coingecko", "jupiter"]
)

# Исторические OHLCV данные
historical_data = provider.get_historical_ohlcv(
    token_mint="So11111111111111111111111111111111111111112",
    interval="1h",
    days=7
)

# Real-time price updates
price_stream = provider.get_realtime_price_stream(
    tokens=["SOL", "USDC", "BONK"],
    callback=price_update_handler
)
```

### Кэширование и оптимизация
- **In-memory cache** для часто запрашиваемых данных
- **Rate limiting** для внешних API
- **Fallback механизмы** при недоступности источников
- **Batch запросы** для оптимизации производительности

## 🔧 Конфигурация и настройка

### Основные настройки
```python
# config/services.py
PRICE_SERVICES_CONFIG = {
    'onchain_price_engine': {
        'min_liquidity_usd': 1000,
        'price_impact_threshold': 0.05,
        'dump_detection_threshold': 0.20,
        'analysis_interval_minutes': 5
    },
    
    'price_data_provider': {
        'cache_ttl_seconds': 60,
        'api_rate_limit_per_minute': 100,
        'fallback_timeout_seconds': 5,
        'preferred_sources': ['onchain', 'jupiter', 'coingecko']
    },
    
    'external_apis': {
        'coingecko': {
            'api_key': 'YOUR_API_KEY',
            'base_url': 'https://api.coingecko.com/api/v3'
        },
        'jupiter': {
            'base_url': 'https://price.jup.ag/v4'
        }
    }
}
```

### Источники данных
```python
DATA_SOURCES = {
    'onchain': {
        'priority': 1,
        'description': 'On-chain swap данные',
        'latency': 'real-time',
        'coverage': 'все токены с активностью'
    },
    'jupiter': {
        'priority': 2, 
        'description': 'Jupiter price API',
        'latency': '~1min',
        'coverage': 'популярные токены'
    },
    'coingecko': {
        'priority': 3,
        'description': 'CoinGecko API',
        'latency': '~5min',
        'coverage': 'листинговые токены'
    }
}
```

## 🚀 Интеграция с аналитикой

### Feature Engineering
Сервисы предоставляют ценовые признаки для ML:
```python
# Ценовые признаки для feature store
PRICE_FEATURES = {
    'price_change_1h': float,
    'price_change_24h': float,
    'volume_change_1h': float,
    'price_volatility_24h': float,
    'max_price_impact_1h': float,
    'liquidity_depth_change': float,
    'dump_probability_score': float
}
```

### Real-time алерты
```python
# Интеграция с мониторингом
from services.onchain_price_engine import OnChainPriceEngine
from monitoring.unknown_event_alerter import send_alert

engine = OnChainPriceEngine()

# Мониторинг дампов в real-time
def monitor_price_dumps():
    dumps = engine.detect_recent_dumps(threshold=0.15)
    for dump in dumps:
        send_alert({
            'type': 'PRICE_DUMP_DETECTED',
            'token': dump['token'],
            'price_change': dump['change_percent'],
            'time': dump['timestamp']
        })
```

## 📈 Примеры использования

### Анализ корреляций с событиями ликвидности
```python
from services.onchain_price_engine import OnChainPriceEngine
from analysis.feature_library import get_liquidity_events

engine = OnChainPriceEngine()

# Анализ связи создания пулов с ценовыми движениями
def analyze_pool_creation_impact():
    liquidity_events = get_liquidity_events(days=7)
    
    for event in liquidity_events:
        price_before = engine.get_price_at_time(
            token=event['token'],
            timestamp=event['timestamp'] - timedelta(hours=1)
        )
        price_after = engine.get_price_at_time(
            token=event['token'],
            timestamp=event['timestamp'] + timedelta(hours=24)
        )
        
        price_impact = (price_after - price_before) / price_before
        print(f"Pool creation impact: {price_impact:.2%}")
```

### Детекция pre-dump активности
```python
def detect_predump_signals():
    # Поиск токенов с резким ростом объема
    volume_spikes = engine.detect_volume_spikes(threshold=5.0)
    
    # Анализ последующих ценовых движений
    for spike in volume_spikes:
        future_price_change = engine.get_future_price_change(
            token=spike['token'],
            from_time=spike['timestamp'],
            duration_hours=4
        )
        
        if future_price_change < -0.20:  # Дамп > 20%
            print(f"Pre-dump signal detected for {spike['token']}")
```

## 🔧 Развитие и расширение

### Добавление нового источника данных
1. Реализуйте интерфейс `PriceDataSource` в `price_data_provider.py`
2. Добавьте конфигурацию в `config/services.py`
3. Интегрируйте с fallback механизмом
4. Добавьте тесты в `/tests/services/`

### Создание кастомных метрик
```python
from services.onchain_price_engine import OnChainPriceEngine

class CustomPriceAnalyzer(OnChainPriceEngine):
    def calculate_custom_metric(self, token_mint: str) -> float:
        # Ваша кастомная логика анализа
        return 0.0
    
    def detect_custom_pattern(self) -> List[dict]:
        # Детекция кастомных паттернов
        return []
```

## 📚 API Reference

### OnChainPriceEngine
- `analyze_token_price_movements(token_mint, time_range)` - Анализ ценовых движений
- `detect_price_dumps(threshold_percent, time_window)` - Детекция дампов
- `get_ohlcv_data(token_mint, interval, days)` - OHLCV данные
- `calculate_vwap(token_mint, time_window)` - Volume weighted average price

### PriceDataProvider  
- `get_current_price(token_mint, vs_currency, sources)` - Текущая цена
- `get_historical_ohlcv(token_mint, interval, days)` - Исторические данные
- `get_realtime_price_stream(tokens, callback)` - Real-time поток

## 🧪 Тестирование

```bash
# Тестирование сервисов
pytest tests/services/

# Тестирование ценового движка
pytest tests/services/test_onchain_price_engine.py

# Тестирование провайдера данных
pytest tests/services/test_price_data_provider.py

# Integration тесты с внешними API
pytest tests/services/test_external_apis.py
```

## 📊 Мониторинг и метрики

Сервисы интегрированы с системой мониторинга и предоставляют метрики:
- Latency запросов к внешним API
- Success rate получения ценовых данных
- Cache hit rate для кэшированных данных
- Количество детектированных ценовых аномалий

Логи сервисов сохраняются в `/logs/services/` для анализа и отладки. 