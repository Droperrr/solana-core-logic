from typing import List, Dict

def get_all_features() -> List[Dict]:
    """
    Возвращает статичный список ML-признаков для демонстрации.
    В будущем эта функция будет генерировать их на основе feature_library.
    """
    features = [
        {
            "name": "initial_liquidity_sol",
            "type": "numerical",
            "category": "liquidity",
            "description": "Initial liquidity provided in SOL when first pool is created",
            "importance": 0.298,
            "correlation": 0.67,
            "nullRate": 0.02,
            "uniqueValues": 1247,
            "distribution": {
                "min": 0.1,
                "max": 1250.5,
                "mean": 12.4,
                "median": 3.2,
                "std": 45.7
            },
        },
        {
            "name": "dex_diversity_score",
            "type": "numerical",
            "category": "network",
            "description": "Number of different DEXs where token has liquidity pools",
            "importance": 0.342,
            "correlation": 0.78,
            "nullRate": 0.0,
            "uniqueValues": 8,
            "distribution": {
                "min": 1,
                "max": 7,
                "mean": 2.1,
                "median": 2,
                "std": 1.3
            },
        },
        {
            "name": "time_to_first_pool_minutes",
            "type": "numerical",
            "category": "timing",
            "description": "Minutes between token creation and first liquidity pool",
            "importance": 0.189,
            "correlation": -0.34,
            "nullRate": 0.05,
            "uniqueValues": 2156,
            "distribution": {
                "min": 0,
                "max": 4320,
                "mean": 127.5,
                "median": 45,
                "std": 234.8
            },
        },
    ]
    return features 