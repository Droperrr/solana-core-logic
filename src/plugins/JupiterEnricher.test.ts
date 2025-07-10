import { JupiterEnricher } from '../src/plugins/JupiterEnricher';
import { SwapEvent, RawTransaction } from '../src/types';
import * as fs from 'fs';
import * as path from 'path';

describe('JupiterEnricher', () => {
  const enricher = new JupiterEnricher();
  const fixturePath = path.resolve(
    process.cwd(),
    'tests/regression/fixtures/2WjgSSiEnMNTynHFH5voUepgeB7RhbXDCK7ZZPwjKAMmBsmJiiJn5kcvT2aKsQuiJrHSzhWmNtJEDZYie9nE7RAz/raw.json'
  );
  const raw: RawTransaction = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));

  function makeSwapEvent(): SwapEvent {
    return {
      type: 'SWAP',
      signature: raw.signature,
      timestamp: raw.blockTime,
      swapper: raw.transaction.message.accountKeys[0],
      tokenIn: {
        mint: 'AL2HhMQLkJqeeK5w4akoogk6Qw2Qw2Qw2Qw2Qw2Qw2Qw2',
        amount: '1000000',
        decimals: 6,
      },
      tokenOut: {
        mint: 'So11111111111111111111111111111111111111112',
        amount: '500000',
        decimals: 9,
      },
      atomicEvents: [],
    };
  }

  it('should enrich SWAP event with Jupiter data (happy path)', async () => {
    const event = makeSwapEvent();
    const enriched = await enricher.enrich(event, raw);
    expect(enriched.metadata?.jupiter_v6?.protocol_name).toBe('Jupiter');
    expect(enriched.metadata?.jupiter_v6?.route).toBeDefined();
  });

  it('should not enrich event if instruction is not Jupiter', async () => {
    const event: SwapEvent = { ...makeSwapEvent(), type: 'SWAP' };
    // Мок-транзакция без Jupiter-инструкций
    const tx: RawTransaction = { ...raw, transaction: { ...raw.transaction, message: { ...raw.transaction.message, instructions: [] } } };
    const enriched = await enricher.enrich(event, tx);
    expect(enriched.metadata?.jupiter_v6?.protocol_name).toBeUndefined();
  });

  it('should not enrich event if no matching instruction', async () => {
    const event: SwapEvent = { ...makeSwapEvent(), type: 'SWAP' };
    // Мок-транзакция без инструкций вообще
    const tx: RawTransaction = { ...raw, transaction: { ...raw.transaction, message: { ...raw.transaction.message, instructions: [] } } };
    const enriched = await enricher.enrich(event, tx);
    expect(enriched.metadata?.jupiter_v6?.protocol_name).toBeUndefined();
  });
}); 