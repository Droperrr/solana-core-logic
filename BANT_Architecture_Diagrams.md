# Архитектурные диаграммы проекта BANT

Данный документ содержит подробные Mermaid диаграммы архитектуры проекта BANT (Behavioral Analysis of Network Transactions) для анализа поведенческих паттернов алгоритмических кошельков на блокчейне Solana.

## 1. Общая архитектура системы

Данная диаграмма показывает все основные компоненты системы и их взаимосвязи:

- **Входные данные**: конфигурация, список токенов, адреса кошельков
- **Слой получения данных**: RPC клиент, обработка сигнатур, поддержка ATA
- **Основной пайплайн обработки**: Universal Parser с поддержкой CPI
- **Архитектура декодера**: нормализация → парсинг → резолвинг → обогащение
- **Поддерживаемые протоколы**: Raydium, Jupiter, Orca, SPL Token, Metaplex
- **Слой хранения**: SQLite с SSOT архитектурой
- **Аналитика и ML**: обработка паттернов, Jupyter notebooks
- **QA и тестирование**: тесты, фикстуры, контроль качества

```mermaid
graph TB
    %% Входные данные и конфигурация
    subgraph "Input Data & Configuration"
        CONFIG["config.py<br/>🔧 Helius API Key<br/>🔧 Database Settings"]
        TOKENS["tokens.txt<br/>📄 Target Token List<br/>~3000 addresses"]
        WALLETS["Wallet Addresses<br/>📄 Algorithmic Wallets<br/>Behavioral Heuristics"]
    end

    %% RPC и получение данных
    subgraph "Data Ingestion Layer"
        RPC["rpc/<br/>🌐 RPC Client<br/>Helius API Integration"]
        SIGFETCH["utils/signature_handler.py<br/>📡 Signature Collection<br/>ATA Support"]
        DATAFETCH["🔄 Transaction Fetching<br/>Rate Limiting<br/>Error Handling"]
    end

    %% Основная обработка
    subgraph "Core Processing Pipeline"
        PROCESSOR["processing/<br/>transaction_processor.py<br/>🔄 Main Processing Loop<br/>Error Capture"]
        
        subgraph "Universal Parser System"
            UPARSER["parser/<br/>universal_parser.py<br/>🎯 Main Entry Point<br/>CPI Support"]
            
            subgraph "Decoder Architecture"
                NORMALIZER["decoder/normalizer/<br/>📋 Transaction Normalization<br/>Format Standardization"]
                PARSERS["decoder/parsers/<br/>🔍 Instruction Parsing<br/>Protocol Recognition"]
                RESOLVER["decoder/resolver/<br/>🎯 Business Event Resolution<br/>Semantic Mapping"]
                ENRICHERS["decoder/enrichers/<br/>✨ ML Enhancement<br/>Feature Extraction"]
            end
        end
    end

    %% Поддерживаемые протоколы
    subgraph "Supported Protocols"
        RAYDIUM["🌊 Raydium<br/>AMM Swaps<br/>Liquidity Pools"]
        JUPITER["🪐 Jupiter<br/>Aggregator Swaps<br/>Multi-hop Routes"]
        ORCA["🐋 Orca<br/>Concentrated Liquidity<br/>Position Management"]
        SPL["🪙 SPL Token<br/>Transfers<br/>Account Management"]
        METAPLEX["🎨 Metaplex<br/>NFT Operations<br/>Metadata"]
    end

    %% База данных
    subgraph "Data Storage Layer"
        SQLITE["db/<br/>📊 SQLite Database<br/>SSOT Architecture"]
        
        subgraph "Database Tables"
            RAWTX["transactions<br/>📝 Raw Transaction Data<br/>raw_json + enriched_data"]
            MLEVENTS["ml_ready_events<br/>🤖 ML-Ready Events<br/>Denormalized View"]
            FEATURES["feature_store_daily<br/>📈 Daily Features<br/>Aggregated Metrics"]
        end
    end

    %% Аналитика и ML
    subgraph "Analytics & ML Pipeline"
        ANALYSIS["analysis/<br/>📊 Pattern Analysis<br/>Behavioral Detection"]
        NOTEBOOKS["analysis/notebooks/<br/>📓 Jupyter Analysis<br/>Hypothesis Testing"]
        REPORTS["reports/<br/>📋 Generated Reports<br/>Pattern Insights"]
    end

    %% Тестирование и QC
    subgraph "Quality Assurance"
        TESTS["tests/<br/>🧪 Test Suite<br/>Unit + Integration"]
        FIXTURES["tests/fixtures/<br/>📦 Golden Dataset<br/>Regression Tests"]
        QC["qc/<br/>✅ Quality Control<br/>Data Validation"]
    end

    %% Утилиты и скрипты
    subgraph "Utilities & Scripts"
        BATCH["scripts/<br/>batch_process_*.py<br/>🔄 Batch Processing"]
        DBSCRIPTS["scripts/<br/>DB Management<br/>🗄️ Schema & Migrations"]
        UTILS["utils/<br/>🛠️ Helper Functions<br/>Common Operations"]
    end

    %% Мониторинг и логи
    subgraph "Monitoring & Logging"
        LOGS["logs/<br/>📋 Application Logs<br/>Error Tracking"]
        MEMBANK["memory-bank/<br/>🧠 Task Management<br/>Progress Tracking"]
    end

    %% Соединения потоков данных
    CONFIG --> RPC
    TOKENS --> SIGFETCH
    WALLETS --> SIGFETCH
    
    RPC --> DATAFETCH
    SIGFETCH --> DATAFETCH
    DATAFETCH --> PROCESSOR
    
    PROCESSOR --> UPARSER
    UPARSER --> NORMALIZER
    NORMALIZER --> PARSERS
    PARSERS --> RESOLVER
    RESOLVER --> ENRICHERS
    
    %% Протоколы подключены к парсерам
    PARSERS --> RAYDIUM
    PARSERS --> JUPITER
    PARSERS --> ORCA
    PARSERS --> SPL
    PARSERS --> METAPLEX
    
    %% Результаты в базу данных
    ENRICHERS --> SQLITE
    SQLITE --> RAWTX
    SQLITE --> MLEVENTS
    SQLITE --> FEATURES
    
    %% Аналитика из базы
    SQLITE --> ANALYSIS
    ANALYSIS --> NOTEBOOKS
    NOTEBOOKS --> REPORTS
    
    %% QC процессы
    ENRICHERS --> QC
    QC --> FIXTURES
    FIXTURES --> TESTS
    
    %% Утилиты работают с базой
    BATCH --> PROCESSOR
    DBSCRIPTS --> SQLITE
    UTILS --> PROCESSOR
    
    %% Мониторинг
    PROCESSOR --> LOGS
    BATCH --> MEMBANK

    %% Стилизация
    classDef inputStyle fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    classDef processingStyle fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef storageStyle fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px
    classDef analyticsStyle fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef protocolStyle fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    classDef qaStyle fill:#f1f8e9,stroke:#33691e,stroke-width:2px
    classDef utilStyle fill:#e0f2f1,stroke:#004d40,stroke-width:2px
    classDef monitorStyle fill:#fafafa,stroke:#424242,stroke-width:2px

    class CONFIG,TOKENS,WALLETS inputStyle
    class RPC,SIGFETCH,DATAFETCH,PROCESSOR,UPARSER,NORMALIZER,PARSERS,RESOLVER,ENRICHERS processingStyle
    class SQLITE,RAWTX,MLEVENTS,FEATURES storageStyle
    class ANALYSIS,NOTEBOOKS,REPORTS analyticsStyle
    class RAYDIUM,JUPITER,ORCA,SPL,METAPLEX protocolStyle
    class TESTS,FIXTURES,QC qaStyle
    class BATCH,DBSCRIPTS,UTILS utilStyle
    class LOGS,MEMBANK monitorStyle
```

## 2. Последовательность обработки данных

Диаграмма последовательности показывает детальный поток выполнения операций в системе.

```mermaid
sequenceDiagram
    participant U as User/Script
    participant BP as Batch Processor
    participant SH as Signature Handler
    participant RPC as RPC Client
    participant TP as Transaction Processor
    participant UP as Universal Parser
    participant D as Decoder Pipeline
    participant DB as SQLite Database
    participant QC as Quality Control

    Note over U,QC: BANT Data Processing Flow

    U->>BP: Start batch processing
    BP->>SH: Fetch signatures for tokens
    SH->>RPC: Get signatures from Helius API
    RPC-->>SH: Return signature list
    SH-->>BP: Filtered signatures (ATA support)

    loop For each signature batch
        BP->>RPC: Fetch transaction details
        RPC-->>BP: Raw transaction data
        BP->>TP: Process transaction
        
        TP->>UP: Parse with CPI support
        UP->>D: Decode transaction
        
        Note over D: Decoder Pipeline Steps
        D->>D: 1. Normalize format
        D->>D: 2. Parse instructions
        D->>D: 3. Resolve to business events
        D->>D: 4. Enrich for ML analysis
        
        D-->>UP: Enriched events
        UP-->>TP: Parsed results
        
        alt Successful processing
            TP->>DB: Save enriched data + raw JSON
            DB-->>TP: Confirm save
        else Processing error
            TP->>DB: Save raw JSON only
            TP->>LOGS: Write to logs/db_write_errors.log
            TP->>LOGS: Save failed_normalization_*.json
            TP->>QC: Log error for analysis
        end
        
        TP-->>BP: Processing complete
    end

    BP->>DB: Refresh analytics views
    DB->>DB: Update ml_ready_events
    DB->>DB: Update feature_store_daily
    DB-->>BP: Views refreshed

    BP->>QC: Run quality checks
    QC->>DB: Validate data integrity
    QC-->>BP: QC report

    BP-->>U: Processing complete with stats

    Note over U,QC: Key Features:<br/>• SSOT: Raw data always preserved<br/>• CPI: Full nested instruction support<br/>• Fault-tolerant: Errors don't stop pipeline<br/>• ML-ready: Structured output for analysis
```

## 3. Детальная архитектура декодера

Данная диаграмма детализирует внутреннее устройство 4-этапного пайплайна декодера.

```mermaid
graph TD
    subgraph "Transaction Input"
        RAWTX["Raw Solana Transaction<br/>📄 JSON Format<br/>Helius API Response"]
    end

    subgraph "Decoder Pipeline Architecture"
        subgraph "Stage 1: Normalization"
            NORM["Normalizer<br/>📋 Format Standardization<br/>Field Mapping"]
            NORMTX["Normalized Transaction<br/>✅ Standard Format<br/>Validated Structure"]
        end

        subgraph "Stage 2: Instruction Parsing"
            ROUTER["Router<br/>🎯 Program ID Detection<br/>Parser Selection"]
            
            subgraph "Protocol Parsers"
                RAYP["Raydium Parser<br/>🌊 AMM Instructions<br/>Swap Detection"]
                JUPP["Jupiter Parser<br/>🪐 Aggregator Routes<br/>Multi-hop Analysis"]
                ORCP["Orca Parser<br/>🐋 Whirlpool Instructions<br/>Position Management"]
                SPLP["SPL Parser<br/>🪙 Token Operations<br/>Transfer/Mint/Burn"]
                METAP["Metaplex Parser<br/>🎨 NFT Instructions<br/>Metadata Operations"]
                UNKNP["Unknown Parser<br/>❓ Fallback Handler<br/>Generic Processing"]
            end
        end

        subgraph "Stage 3: Business Event Resolution"
            RESOLVER["Resolver Engine<br/>🎯 Semantic Mapping<br/>Event Generation"]
            
            subgraph "Event Types"
                SWAPE["SWAP Events<br/>💱 Token Exchanges<br/>Price Impact Analysis"]
                TRANSE["TRANSFER Events<br/>📤 Token Movements<br/>Flow Tracking"]
                LIQE["LIQUIDITY Events<br/>💧 Pool Operations<br/>Position Changes"]
                NFTE["NFT Events<br/>🎨 Metadata Operations<br/>Ownership Changes"]
                UNKNE["UNKNOWN Events<br/>❓ Unrecognized Patterns<br/>Metadata Preserved"]
            end
        end

        subgraph "Stage 4: ML Enrichment"
            subgraph "Enrichers (Currently Implemented)"
                NETFLOW["Net Token Flow<br/>📊 Balance Changes<br/>Direction Analysis"]
                COMPUTE["Compute Units<br/>⚡ Resource Usage<br/>Complexity Metrics"]
                SEQUENCE["Sequence Analysis<br/>🔄 Instruction Order<br/>Pattern Detection"]
            end
            
            subgraph "Enrichers (Planned)"
                TIMING["Timing Analysis<br/>⏱️ Block Time<br/>Temporal Patterns"]
                CONTEXT["Context Enricher<br/>🔍 Cross-reference<br/>Historical Patterns"]
            end
            
            ENRICHED["Enriched Events<br/>✨ ML-Ready Data<br/>Feature Vectors"]
        end
    end

    subgraph "CPI Support Layer"
        CPI["CPI Processor<br/>🔗 Nested Instructions<br/>Inner Instruction Analysis"]
        BALANCE["Balance Analyzer<br/>⚖️ Pre/Post Token Balances<br/>True Effect Calculation"]
        MULTIHOP["Multi-hop Detector<br/>🛤️ Complex Route Analysis<br/>Aggregator Support"]
    end

    subgraph "Output & Storage"
        EVENTS["Business Events<br/>📋 Structured Data<br/>Timestamped & Typed"]
        DBSAVE["Database Storage<br/>💾 SQLite SSOT<br/>Raw + Enriched"]
    end

    %% Main flow
    RAWTX --> NORM
    NORM --> NORMTX
    NORMTX --> ROUTER
    
    %% Router to parsers
    ROUTER --> RAYP
    ROUTER --> JUPP
    ROUTER --> ORCP
    ROUTER --> SPLP
    ROUTER --> METAP
    ROUTER --> UNKNP
    
    %% Parsers to resolver
    RAYP --> RESOLVER
    JUPP --> RESOLVER
    ORCP --> RESOLVER
    SPLP --> RESOLVER
    METAP --> RESOLVER
    UNKNP --> RESOLVER
    
    %% Resolver to events
    RESOLVER --> SWAPE
    RESOLVER --> TRANSE
    RESOLVER --> LIQE
    RESOLVER --> NFTE
    RESOLVER --> UNKNE
    
    %% Events to enrichers
    SWAPE --> NETFLOW
    TRANSE --> NETFLOW
    LIQE --> NETFLOW
    NFTE --> NETFLOW
    UNKNE --> NETFLOW
    
    NETFLOW --> COMPUTE
    COMPUTE --> SEQUENCE
    SEQUENCE --> TIMING
    TIMING --> CONTEXT
    CONTEXT --> ENRICHED
    
    %% CPI Support
    NORMTX --> CPI
    CPI --> BALANCE
    BALANCE --> MULTIHOP
    MULTIHOP --> RESOLVER
    
    %% Final output
    ENRICHED --> EVENTS
    EVENTS --> DBSAVE

    %% Styling
    classDef inputStyle fill:#e3f2fd,stroke:#0277bd,stroke-width:2px
    classDef normStyle fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef parseStyle fill:#e8f5e8,stroke:#388e3c,stroke-width:2px
    classDef resolveStyle fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef enrichStyle fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef cpiStyle fill:#e0f2f1,stroke:#00695c,stroke-width:2px
    classDef outputStyle fill:#f1f8e9,stroke:#689f38,stroke-width:2px

    class RAWTX inputStyle
    class NORM,NORMTX normStyle
    class ROUTER,RAYP,JUPP,ORCP,SPLP,METAP,UNKNP parseStyle
    class RESOLVER,SWAPE,TRANSE,LIQE,NFTE,UNKNE resolveStyle
    class NETFLOW,COMPUTE,SEQUENCE,TIMING,CONTEXT,ENRICHED enrichStyle
    class CPI,BALANCE,MULTIHOP cpiStyle
    class EVENTS,DBSAVE outputStyle

    subgraph Resolvers [Приоритетная цепочка резолверов]
        direction LR
        R_JUP[("JupiterResolver<br>(IDL-based)")] --> R_GEN
        R_GEN[("GenericSwapResolver<br>(Hybrid: CPI + Balances)")] --> R_RAY
        R_RAY[RaydiumResolver] --> R_TRN
        R_TRN[TransferResolver] --> R_UNK
        R_UNK[UnknownResolver]
    end
```

## Ключевые архитектурные принципы

### 1. Single Source of Truth (SSOT)
- Сырые данные транзакций всегда сохраняются в поле `raw_json`
- Обогащенные данные сохраняются в поле `enriched_data`
- Даже при ошибках обработки сырые данные остаются доступными

### 2. Отказоустойчивость
- Ошибки в одной части пайплайна не останавливают обработку других компонентов
- Неподдерживаемые инструкции маркируются как `UNKNOWN` с сохранением метаданных
- Система способна обрабатывать любые инструкции Solana

### 3. Поддержка CPI (Cross-Program Invocation)
- Полная обработка вложенных инструкций (inner instructions)
- Формирование семантических событий из сложных многоуровневых транзакций
- Анализ изменений токенов на основе preTokenBalances/postTokenBalances
- Поддержка мультихоп свапов через различные агрегаторы

### 4. ML-готовность
- Структурированный вывод данных для машинного обучения
- Нормализованные и дедуплицированные данные
- Подготовка признаков для анализа поведенческих паттернов
- Поддержка итеративного экспериментирования

### 5. Модульная архитектура
- Легко расширяемая система протоколов
- Четкое разделение ответственности между компонентами
- Независимые модули для парсинга, резолвинга и обогащения
- Поддержка добавления новых протоколов без изменения основной логики

### 6. Контроль качества
- Комплексная система тестирования (unit + integration)
- Golden Dataset для регрессионных тестов
- Автоматизированная валидация данных
- Мониторинг и логирование всех операций

## Описание архитектурных компонентов

### Входные данные и конфигурация
- **config.py**: Содержит настройки подключения к Helius API и параметры базы данных
- **tokens.txt**: Список целевых токенов для анализа (~3000 адресов)
- **Wallet Addresses**: Предварительно отобранные алгоритмические кошельки на основе поведенческих эвристик

### Слой получения данных (Data Ingestion Layer)
- **RPC Client**: Интеграция с Helius API для получения данных блокчейна
- **Signature Handler**: Сбор сигнатур транзакций с поддержкой Associated Token Accounts (ATA)
- **Transaction Fetching**: Получение полных данных транзакций с обработкой ошибок и лимитами скорости

### Основной пайплайн обработки (Core Processing Pipeline)
- **Transaction Processor**: Главный цикл обработки с захватом ошибок
- **Universal Parser**: Центральный оркестратор обработки с поддержкой CPI, использующий decoder как библиотеку инструментов
- **Decoder Architecture**: 4-этапный пайплайн декодирования (библиотека модулей)

### Поддерживаемые протоколы
- **Raydium**: AMM свапы и пулы ликвидности
- **Jupiter**: Агрегатор свапов и мультихоп маршруты
- **Orca**: Концентрированная ликвидность и управление позициями
- **SPL Token**: Трансферы и управление аккаунтами
- **Metaplex**: NFT операции и метаданные

### Слой хранения данных (Data Storage Layer)
- **SQLite Database**: SSOT архитектура с портативностью
- **transactions**: Таблица с сырыми и обогащенными данными
- **ml_ready_events**: Денормализованное представление для ML
- **feature_store_daily**: Агрегированные метрики по дням

### Аналитика и ML пайплайн
- **Pattern Analysis**: Обнаружение поведенческих паттернов
- **Jupyter Notebooks**: Интерактивный анализ и тестирование гипотез
- **Generated Reports**: Структурированные отчеты по найденным паттернам

## Технические особенности

### Decoder Pipeline (4 этапа)
1. **Нормализация**: Стандартизация формата транзакций
2. **Парсинг**: Распознавание инструкций по протоколам
3. **Резолвинг**: Преобразование в бизнес-события
4. **Обогащение**: Добавление ML-признаков

### CPI Support Layer
- **CPI Processor**: Анализ вложенных инструкций
- **Balance Analyzer**: Расчет истинного эффекта на основе балансов
- **Multi-hop Detector**: Обнаружение сложных маршрутов

### Обогатители (Enrichers)
**Реализованные:**
- **NetTokenChangeEnricher**: Анализ изменений токенов
- **ComputeUnitEnricher**: Метрики использования ресурсов
- **SequenceEnricher**: Анализ последовательности инструкций

**Планируемые к реализации:**
- **TimingEnricher**: Анализ временных паттернов
- **ContextEnricher**: Контекстное обогащение с исторической перекрестной ссылкой

### Типы событий
- **SWAP**: Обмены токенов с анализом ценового воздействия
- **TRANSFER**: Движения токенов с отслеживанием потоков
- **LIQUIDITY**: Операции с пулами и изменения позиций
- **NFT**: Операции с метаданными и изменения владения
- **UNKNOWN**: Нераспознанные паттерны с сохранением метаданных

## Заключение

Данная архитектура обеспечивает надежную, масштабируемую и расширяемую систему для анализа поведенческих паттернов алгоритмических кошельков на блокчейне Solana. 

Основные преимущества:
- **Отказоустойчивость**: Система продолжает работать даже при ошибках
- **Расширяемость**: Легко добавлять новые протоколы и типы анализа
- **ML-готовность**: Данные подготовлены для машинного обучения
- **Прозрачность**: Полное логирование и отслеживание операций
- **Портативность**: SQLite обеспечивает простоту развертывания 