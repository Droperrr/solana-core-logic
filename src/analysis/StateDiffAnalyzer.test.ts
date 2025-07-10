import { StateDiffAnalyzer } from '../src/StateDiffAnalyzer';
import { RawTransaction } from '../src/types';
import * as fs from 'fs';
import * as path from 'path';

describe('StateDiffAnalyzer', () => {
  const fixturePath = path.resolve(
    process.cwd(),
    'tests/regression/fixtures/2WjgSSiEnMNTynHFH5voUepgeB7RhbXDCK7ZZPwjKAMmBsmJiiJn5kcvT2aKsQuiJrHSzhWmNtJEDZYie9nE7RAz/raw.json'
  );
  const raw: RawTransaction = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));
  const analyzer = new StateDiffAnalyzer();

  it('should correctly identify DEBIT and CREDIT for simple SOL transfer', () => {
    // Используем реальные pre/postBalances для SOL
    const tx = JSON.parse(JSON.stringify(raw));
    // Оставляем только изменения по SOL (обнуляем pre/postTokenBalances)
    tx.meta.preTokenBalances = [];
    tx.meta.postTokenBalances = [];
    const events = analyzer.analyze(tx);
    expect(events.some(e => e.type === 'DEBIT_SOL')).toBe(true);
    expect(events.some(e => e.type === 'CREDIT_SOL')).toBe(true);
  });

  it('should correctly identify DEBIT and CREDIT for simple SPL token transfer', () => {
    // Используем реальные pre/postTokenBalances для SPL
    const tx = JSON.parse(JSON.stringify(raw));
    // Оставляем только изменения по SPL (обнуляем pre/postBalances)
    tx.meta.preBalances = tx.meta.preBalances.map(() => 0);
    tx.meta.postBalances = tx.meta.postBalances.map(() => 0);
    const events = analyzer.analyze(tx);
    expect(events.some(e => e.type === 'DEBIT_SPL')).toBe(true);
    expect(events.some(e => e.type === 'CREDIT_SPL')).toBe(true);
  });

  it('should handle SPL transfer with temporary ATA (no atomic events for temp account)', () => {
    // В этом фикстурном tx есть создание и закрытие временного ATA
    // Проверяем, что не генерируются атомарные события для временного счета (owner != user)
    const tx = JSON.parse(JSON.stringify(raw));
    const events = analyzer.analyze(tx);
    // Временный ATA: ищем события с owner отличным от основных участников
    const tempAtaEvents = events.filter(e =>
      'owner' in e &&
      (e as any).owner === '6qNayJgRDgEVEg4TZU2F1Sf87LJ9VabUrzhMP8nJ7veV'
    );
    expect(tempAtaEvents.length).toBe(0);
  });

  it('should return empty array for transaction with no balance changes', () => {
    // Обнуляем все балансы
    const tx = JSON.parse(JSON.stringify(raw));
    tx.meta.preBalances = tx.meta.preBalances.map(() => 1000);
    tx.meta.postBalances = tx.meta.preBalances.map(() => 1000);
    tx.meta.preTokenBalances = [];
    tx.meta.postTokenBalances = [];
    const events = analyzer.analyze(tx);
    expect(events).toEqual([]);
  });

  it('should correctly parse balance changes even if transaction failed', () => {
    const tx = JSON.parse(JSON.stringify(raw));
    tx.meta.err = { InstructionError: [0, { Custom: 1 }] };
    const events = analyzer.analyze(tx);
    // Проверяем, что события есть и они валидны
    expect(Array.isArray(events)).toBe(true);
    expect(events.length).toBeGreaterThan(0);
    // Проверяем, что есть хотя бы одно событие DEBIT или CREDIT
    expect(events.some(e => e.type.includes('DEBIT') || e.type.includes('CREDIT'))).toBe(true);
  });
}); 