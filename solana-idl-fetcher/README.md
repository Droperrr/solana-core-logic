# solana-idl-fetcher

Node.js утилита для автоматической загрузки и обновления IDL файлов Solana программ.

## 🎯 Назначение

Предоставляет инструменты для:
- **Автоматической загрузки IDL** файлов из Solana программ
- **Обновления схем** протоколов до актуальных версий
- **Валидации структуры** полученных IDL
- **Интеграции с CI/CD** для автоматического обновления

## 📦 Структура проекта

```
solana-idl-fetcher/
├── package.json              # Node.js зависимости и скрипты
├── package-lock.json         # Locked версии зависимостей
├── fetch_jupiter_idl.js      # Скрипт загрузки Jupiter IDL
├── jupiter_idl_example.json  # Пример полученного IDL
└── README.md                 # Данная документация
```

## 🚀 Установка и настройка

### Требования
- Node.js 16.x или выше
- npm 8.x или выше
- Доступ к Solana RPC endpoint

### Установка зависимостей
```bash
cd solana-idl-fetcher
npm install
```

### Основные зависимости
```json
{
  "@project-serum/anchor": "^0.25.0",
  "@solana/web3.js": "^1.78.0",
  "axios": "^1.4.0"
}
```

## 🔧 Использование

### Загрузка Jupiter IDL
```bash
# Выполнение скрипта загрузки
node fetch_jupiter_idl.js

# С кастомным RPC endpoint
RPC_URL=https://api.helius.xyz/v0/rpc node fetch_jupiter_idl.js

# С сохранением в конкретный файл
OUTPUT_FILE=jupiter_v7.json node fetch_jupiter_idl.js
```

### Пример скрипта загрузки
```javascript
// fetch_jupiter_idl.js
const { Program, Provider, web3 } = require('@project-serum/anchor');
const fs = require('fs');

async function fetchJupiterIDL() {
    const connection = new web3.Connection(
        process.env.RPC_URL || 'https://api.mainnet-beta.solana.com'
    );
    
    const programId = new web3.PublicKey('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4');
    
    try {
        // Получение IDL из on-chain данных
        const idl = await Program.fetchIdl(programId, connection);
        
        if (idl) {
            const outputFile = process.env.OUTPUT_FILE || 'jupiter_idl_example.json';
            fs.writeFileSync(outputFile, JSON.stringify(idl, null, 2));
            console.log(`IDL saved to ${outputFile}`);
        } else {
            console.log('IDL not found on-chain');
        }
    } catch (error) {
        console.error('Error fetching IDL:', error);
    }
}

fetchJupiterIDL();
```

## 🔄 Автоматизация обновлений

### GitHub Actions
```yaml
# .github/workflows/update-idl.yml
name: Update IDL Files
on:
  schedule:
    - cron: '0 2 * * *'  # Ежедневно в 2:00 UTC
  workflow_dispatch:

jobs:
  update-idl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18'
          
      - name: Install dependencies
        run: |
          cd solana-idl-fetcher
          npm install
          
      - name: Fetch latest IDL files
        run: |
          cd solana-idl-fetcher
          node fetch_jupiter_idl.js
          node fetch_raydium_idl.js
          node fetch_orca_idl.js
          
      - name: Copy to IDL directory
        run: |
          cp solana-idl-fetcher/*.json idl/
          
      - name: Create Pull Request
        uses: peter-evans/create-pull-request@v5
        with:
          title: 'chore: Update IDL files'
          body: 'Automated update of IDL files from on-chain data'
          branch: update-idl-files
```

### Cron job
```bash
# Добавить в crontab для ежедневного обновления
0 2 * * * cd /path/to/bant/solana-idl-fetcher && node fetch_jupiter_idl.js
```

## 📋 Поддерживаемые протоколы

### Текущие фетчеры
- **`fetch_jupiter_idl.js`** - Jupiter V6 агрегатор
- **`fetch_raydium_idl.js`** - Raydium AMM (планируется)
- **`fetch_orca_idl.js`** - Orca V2 (планируется)

### Добавление нового протокола
```javascript
// fetch_protocol_idl.js
const { Program, web3 } = require('@project-serum/anchor');
const fs = require('fs');

async function fetchProtocolIDL() {
    const connection = new web3.Connection(process.env.RPC_URL);
    const programId = new web3.PublicKey('YOUR_PROGRAM_ID_HERE');
    
    try {
        const idl = await Program.fetchIdl(programId, connection);
        
        if (idl) {
            const filename = 'protocol_name.json';
            fs.writeFileSync(filename, JSON.stringify(idl, null, 2));
            console.log(`${filename} updated successfully`);
            
            // Валидация структуры
            validateIDLStructure(idl);
            
        } else {
            console.log('IDL not available on-chain for this program');
        }
    } catch (error) {
        console.error('Failed to fetch IDL:', error.message);
    }
}

function validateIDLStructure(idl) {
    const requiredFields = ['version', 'name', 'instructions'];
    
    for (const field of requiredFields) {
        if (!idl[field]) {
            throw new Error(`Missing required field: ${field}`);
        }
    }
    
    console.log('IDL structure validation passed');
}

fetchProtocolIDL();
```

## 🔍 Валидация и проверки

### Структурная валидация
```javascript
function validateIDL(idl) {
    const checks = {
        hasVersion: !!idl.version,
        hasName: !!idl.name,
        hasInstructions: Array.isArray(idl.instructions),
        instructionCount: idl.instructions?.length || 0,
        hasAccounts: Array.isArray(idl.accounts),
        hasTypes: Array.isArray(idl.types)
    };
    
    console.log('IDL Validation Results:', checks);
    
    // Проверка инструкций
    if (checks.hasInstructions) {
        idl.instructions.forEach((instruction, index) => {
            if (!instruction.name) {
                console.warn(`Instruction ${index} missing name`);
            }
            if (!Array.isArray(instruction.accounts)) {
                console.warn(`Instruction ${instruction.name} missing accounts`);
            }
        });
    }
    
    return checks;
}
```

### Сравнение версий
```javascript
function compareIDLVersions(oldIDL, newIDL) {
    const comparison = {
        versionChanged: oldIDL.version !== newIDL.version,
        instructionsAdded: [],
        instructionsRemoved: [],
        instructionsModified: []
    };
    
    const oldInstructions = new Set(oldIDL.instructions.map(i => i.name));
    const newInstructions = new Set(newIDL.instructions.map(i => i.name));
    
    // Найти добавленные инструкции
    for (const instruction of newInstructions) {
        if (!oldInstructions.has(instruction)) {
            comparison.instructionsAdded.push(instruction);
        }
    }
    
    // Найти удаленные инструкции
    for (const instruction of oldInstructions) {
        if (!newInstructions.has(instruction)) {
            comparison.instructionsRemoved.push(instruction);
        }
    }
    
    return comparison;
}
```

## ⚙️ Конфигурация

### Переменные окружения
```bash
# .env файл
RPC_URL=https://api.helius.xyz/v0/rpc
OUTPUT_DIR=../idl/
LOG_LEVEL=info
VALIDATE_IDL=true
BACKUP_OLD_IDL=true
```

### Настройки в package.json
```json
{
  "scripts": {
    "fetch:jupiter": "node fetch_jupiter_idl.js",
    "fetch:raydium": "node fetch_raydium_idl.js", 
    "fetch:all": "npm run fetch:jupiter && npm run fetch:raydium",
    "validate": "node validate_idl.js",
    "test": "npm run fetch:all && npm run validate"
  },
  "config": {
    "defaultRPC": "https://api.mainnet-beta.solana.com",
    "outputDirectory": "../idl/",
    "backupDirectory": "./backups/"
  }
}
```

## 🚨 Обработка ошибок

### Типичные проблемы
1. **Program not found** - Программа не найдена по указанному адресу
2. **IDL not available** - IDL не доступен on-chain (не Anchor программа)
3. **RPC timeout** - Таймаут подключения к RPC
4. **Invalid IDL structure** - Некорректная структура полученного IDL

### Retry механизм
```javascript
async function fetchWithRetry(fetchFunction, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await fetchFunction();
        } catch (error) {
            console.log(`Attempt ${attempt} failed:`, error.message);
            
            if (attempt === maxRetries) {
                throw error;
            }
            
            // Exponential backoff
            await new Promise(resolve => 
                setTimeout(resolve, Math.pow(2, attempt) * 1000)
            );
        }
    }
}
```

## 📊 Мониторинг и логирование

### Статистика загрузки
```javascript
class IDLFetchStats {
    constructor() {
        this.stats = {
            totalFetches: 0,
            successfulFetches: 0,
            failedFetches: 0,
            lastUpdate: null,
            programsCovered: new Set()
        };
    }
    
    recordSuccess(programId) {
        this.stats.totalFetches++;
        this.stats.successfulFetches++;
        this.stats.lastUpdate = new Date();
        this.stats.programsCovered.add(programId);
    }
    
    recordFailure() {
        this.stats.totalFetches++;
        this.stats.failedFetches++;
    }
    
    getReport() {
        return {
            ...this.stats,
            successRate: this.stats.successfulFetches / this.stats.totalFetches,
            programsCovered: Array.from(this.stats.programsCovered)
        };
    }
}
```

## 🧪 Тестирование

### Unit тесты
```bash
# Установка test зависимостей
npm install --save-dev jest

# Запуск тестов
npm test
```

### Пример теста
```javascript
// test/fetch_idl.test.js
const { fetchJupiterIDL } = require('../fetch_jupiter_idl');

describe('IDL Fetcher', () => {
    test('should fetch valid Jupiter IDL', async () => {
        const idl = await fetchJupiterIDL();
        
        expect(idl).toBeDefined();
        expect(idl.name).toBe('jupiter');
        expect(Array.isArray(idl.instructions)).toBe(true);
        expect(idl.instructions.length).toBeGreaterThan(0);
    });
    
    test('should validate IDL structure', () => {
        const mockIDL = {
            version: '0.1.0',
            name: 'test',
            instructions: []
        };
        
        expect(() => validateIDL(mockIDL)).not.toThrow();
    });
});
```

## 🔧 Интеграция с основным проектом

### Автоматическое копирование
```bash
#!/bin/bash
# update_idl.sh

cd solana-idl-fetcher

# Загрузка всех IDL
npm run fetch:all

# Валидация
npm run validate

# Копирование в основную директорию
cp *.json ../idl/

# Создание backup
mkdir -p backups/$(date +%Y%m%d)
cp ../idl/*.json backups/$(date +%Y%m%d)/

echo "IDL files updated successfully"
```

### Уведомления об обновлениях
```javascript
// В основном скрипте
const fs = require('fs');
const path = require('path');

function notifyIDLUpdate(programName, changes) {
    const notification = {
        timestamp: new Date().toISOString(),
        program: programName,
        changes: changes,
        action: 'idl_updated'
    };
    
    // Сохранить в лог
    fs.appendFileSync(
        path.join('..', 'logs', 'idl_updates.log'),
        JSON.stringify(notification) + '\n'
    );
    
    // Отправить webhook (опционально)
    if (process.env.WEBHOOK_URL) {
        sendWebhook(notification);
    }
}
```

Утилита обеспечивает актуальность IDL схем для корректного функционирования decoder модуля и должна запускаться регулярно для отслеживания изменений в протоколах. 