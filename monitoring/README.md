# monitoring

Модуль мониторинга и алертинга для BANT системы анализа блокчейн-данных Solana.

## 🎯 Назначение

Обеспечивает real-time мониторинг и автоматическое оповещение о:
- **Неизвестных событиях** и новых типах транзакций
- **Качестве данных** и аномалиях в пайплайне
- **Производительности системы** и узких местах
- **Критических ошибках** требующих немедленного внимания

## 🔧 Архитектура и компоненты

### Ключевые модули
- **`unknown_event_alerter.py`** - Детекция и алертинг неизвестных событий в транзакциях

### Основные функции
1. **Event Monitoring** - Отслеживание новых типов инструкций и событий
2. **Quality Assurance** - Мониторинг качества enrichment и парсинга
3. **Performance Tracking** - Контроль производительности обработки
4. **Alert System** - Автоматические уведомления о проблемах

## 🚨 Unknown Event Alerter

### Назначение
Система автоматического обнаружения и оповещения о неизвестных типах транзакций, инструкций и событий в блокчейне Solana.

### Ключевые возможности
- **Детекция новых программ** и неподдерживаемых инструкций
- **Анализ частоты** появления неизвестных событий
- **Приоритизация** по важности и частоте встречаемости
- **Автоматические алерты** для разработчиков

### Запуск мониторинга
```bash
# Запуск continuous мониторинга
python monitoring/unknown_event_alerter.py --continuous --interval 300

# Разовая проверка за последний час
python monitoring/unknown_event_alerter.py --check-recent --hours 1

# Анализ конкретного токена
python monitoring/unknown_event_alerter.py --token [TOKEN_ADDRESS]

# Генерация отчета по неизвестным событиям
python monitoring/unknown_event_alerter.py --report --days 7
```

### Конфигурация алертов
```python
# Пороги для алертинга
ALERT_THRESHOLDS = {
    'unknown_programs': 5,      # Новых программ за час
    'unknown_instructions': 10,  # Новых инструкций за час
    'error_rate': 0.05,         # 5% ошибок в обработке
    'processing_delay': 300     # Задержка обработки > 5 минут
}

# Каналы уведомлений
NOTIFICATION_CHANNELS = {
    'slack': True,
    'email': True,
    'file_log': True,
    'webhook': False
}
```

## 📊 Типы мониторинга

### 1. Real-time Event Detection
- Мониторинг новых типов транзакций в real-time
- Анализ паттернов неизвестных инструкций
- Детекция аномальной активности

### 2. Data Quality Monitoring
- Проверка целостности enriched данных
- Валидация QC тегов и их соответствия
- Мониторинг пропусков в обработке

### 3. Performance Monitoring
- Отслеживание времени обработки транзакций
- Мониторинг использования ресурсов
- Детекция узких мест в пайплайне

### 4. System Health Monitoring
- Проверка доступности RPC endpoints
- Мониторинг состояния базы данных
- Контроль свободного места и памяти

## 📈 Метрики и дашборды

### Ключевые метрики
```python
CORE_METRICS = {
    # Обработка данных
    'transactions_processed_per_hour': int,
    'enrichment_success_rate': float,
    'unknown_events_detected': int,
    'qc_failures_count': int,
    
    # Производительность
    'avg_processing_time_ms': float,
    'rpc_response_time_ms': float,
    'database_query_time_ms': float,
    
    # Качество данных
    'data_completeness_ratio': float,
    'enrichment_coverage_ratio': float,
    'validation_errors_count': int,
    
    # Системные ресурсы
    'cpu_usage_percent': float,
    'memory_usage_percent': float,
    'disk_space_available_gb': float
}
```

### Визуализация
- **Grafana дашборды** для real-time мониторинга
- **Slack боты** для критических алертов
- **Email отчеты** с ежедневной сводкой
- **Log файлы** для детального анализа

## 🔔 Система алертинга

### Уровни критичности
1. **CRITICAL** - Система недоступна или критические ошибки
2. **HIGH** - Значительное снижение производительности
3. **MEDIUM** - Обнаружены неизвестные события
4. **LOW** - Информационные уведомления

### Типы алертов
```python
ALERT_TYPES = {
    'unknown_program_detected': {
        'level': 'MEDIUM',
        'description': 'Обнаружена новая программа',
        'action': 'Добавить парсер для программы'
    },
    'enrichment_failure_spike': {
        'level': 'HIGH', 
        'description': 'Резкий рост ошибок enrichment',
        'action': 'Проверить логику обогащения'
    },
    'rpc_endpoint_down': {
        'level': 'CRITICAL',
        'description': 'RPC endpoint недоступен',
        'action': 'Переключиться на backup endpoint'
    },
    'processing_delay': {
        'level': 'HIGH',
        'description': 'Задержка в обработке данных',
        'action': 'Проверить нагрузку на систему'
    }
}
```

## ⚙️ Конфигурация

### Основные настройки
```python
# config/monitoring.py
MONITORING_CONFIG = {
    'check_interval_seconds': 60,
    'retention_days': 30,
    'alert_cooldown_minutes': 15,
    'max_alerts_per_hour': 10,
    
    'rpc_endpoints': [
        'https://api.helius.xyz/v0/rpc',
        'https://api.mainnet-beta.solana.com'
    ],
    
    'database': {
        'max_connection_time_ms': 5000,
        'query_timeout_seconds': 30
    }
}
```

### Интеграции
- **Slack Webhooks** для команды разработки
- **Email SMTP** для управления
- **Webhook URLs** для внешних систем
- **Log aggregators** (ELK Stack, Grafana Loki)

## 🚀 Запуск и развертывание

### Production развертывание
```bash
# Запуск в фоновом режиме
nohup python monitoring/unknown_event_alerter.py --continuous &

# С использованием systemd
sudo systemctl start bant-monitoring
sudo systemctl enable bant-monitoring

# Docker контейнер
docker run -d --name bant-monitoring \
  -v /path/to/config:/app/config \
  -v /path/to/logs:/app/logs \
  bant:monitoring
```

### Development режим
```bash
# Локальный запуск с debug логами
python monitoring/unknown_event_alerter.py --debug --check-recent --hours 1

# Тестирование алертов
python monitoring/unknown_event_alerter.py --test-alerts

# Dry-run режим (без отправки алертов)
python monitoring/unknown_event_alerter.py --dry-run --continuous
```

## 📝 Логирование

### Структура логов
```
logs/monitoring/
├── unknown_events_YYYY-MM-DD.log      # Неизвестные события
├── performance_YYYY-MM-DD.log         # Метрики производительности  
├── alerts_YYYY-MM-DD.log              # Отправленные алерты
├── system_health_YYYY-MM-DD.log       # Состояние системы
└── errors_YYYY-MM-DD.log              # Ошибки мониторинга
```

### Формат логов
```json
{
  "timestamp": "2024-06-24T10:30:00Z",
  "level": "INFO",
  "component": "unknown_event_alerter", 
  "event_type": "new_program_detected",
  "details": {
    "program_id": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
    "transaction_signature": "5Nv...",
    "frequency": 15,
    "first_seen": "2024-06-24T10:25:00Z"
  },
  "action": "ALERT_SENT"
}
```

## 🔧 Расширение и кастомизация

### Добавление нового типа мониторинга
1. Создайте новый модуль в `/monitoring/`
2. Реализуйте базовые интерфейсы мониторинга
3. Добавьте конфигурацию в `config/monitoring.py`
4. Интегрируйте с системой алертинга
5. Добавьте тесты в `/tests/monitoring/`

### Создание кастомных алертов
```python
from monitoring.alerter import BaseAlerter

class CustomEventAlerter(BaseAlerter):
    def check_condition(self) -> bool:
        # Ваша логика детекции
        return True
    
    def create_alert(self) -> AlertMessage:
        return AlertMessage(
            level='HIGH',
            title='Custom Event Detected',
            description='...',
            action='...'
        )
```

## 📚 Troubleshooting

### Частые проблемы
1. **Ложные срабатывания** - настройте пороги алертинга
2. **Пропущенные события** - проверьте интервалы мониторинга
3. **Высокая нагрузка** - оптимизируйте частоту проверок
4. **Недоступность RPC** - настройте fallback endpoints

### Диагностика
```bash
# Проверка состояния мониторинга
python monitoring/unknown_event_alerter.py --health-check

# Тестирование подключений
python monitoring/unknown_event_alerter.py --test-connections

# Анализ производительности
python monitoring/unknown_event_alerter.py --performance-report
``` 