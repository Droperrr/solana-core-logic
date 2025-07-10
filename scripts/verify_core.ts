// scripts/verify_core.ts
import { StateDiffAnalyzer } from '../src/src/StateDiffAnalyzer.js';
import { EventAggregator } from '../src/src/EventAggregator.js';
import { JupiterEnricher } from '../src/src/plugins/JupiterEnricher.js';
import { RawTransaction, SwapEvent } from '../src/src/types.js';
import * as fs from 'fs';
import * as path from 'path';

async function verifyCore() {
    console.log('--- 🔬 Core Verification Script ---');
    
    // 1. Инициализация компонентов
    const analyzer = new StateDiffAnalyzer();
    const aggregator = new EventAggregator({}, [new JupiterEnricher()]);
    console.log('✅ Components initialized.');

    // 2. Загрузка эталонной транзакции из фикстуры
    const fixturePath = path.resolve(
        process.cwd(),
        'tests/regression/fixtures/2WjgSSiEnMNTynHFH5voUepgeB7RhbXDCK7ZZPwjKAMmBsmJiiJn5kcvT2aKsQuiJrHSzhWmNtJEDZYie9nE7RAz/raw.json'
    );
    let rawTx: RawTransaction;
    try {
        const rawJson = fs.readFileSync(fixturePath, 'utf-8');
        const parsedJson = JSON.parse(rawJson);
        // Адаптируем фикстуру к нашему типу RawTransaction
        rawTx = parsedJson.result ? parsedJson.result : parsedJson;
        rawTx.signature = rawTx.transaction.signatures[0];
        console.log(`✅ Loaded transaction fixture: ${rawTx.signature}`);
    } catch (e) {
        console.error('❌ FATAL: Could not load fixture file.', e);
        process.exit(1);
    }

    // 3. Запуск пайплайна
    try {
        const atomicEvents = analyzer.analyze(rawTx);
        console.log(`📊 StateDiffAnalyzer created ${atomicEvents.length} atomic events.`);

        const semanticEvent = await aggregator.aggregate(atomicEvents, rawTx);
        console.log(`🎯 EventAggregator produced a semantic event of type: ${semanticEvent.type}`);

        // 4. Финальная проверка
        if (semanticEvent.type === 'SWAP') {
            const swap = semanticEvent as SwapEvent;
            console.log(`  Swapper: ${swap.swapper}`);
            console.log(`  Token In: ${swap.tokenIn.amount} of ${swap.tokenIn.mint}`);
            console.log(`  Token Out: ${swap.tokenOut.amount} of ${swap.tokenOut.mint}`);
            console.log('✅ SUCCESS: The core pipeline correctly identified a SWAP event.');
        } else {
            console.error(`❌ FAILURE: Expected a SWAP event, but got ${semanticEvent.type}.`);
        }
        // console.log(JSON.stringify(semanticEvent, null, 2)); // Закомментировано для чистоты вывода

    } catch (e) {
        console.error('❌ FATAL: An error occurred during processing.', e);
        process.exit(1);
    }
}

verifyCore(); 