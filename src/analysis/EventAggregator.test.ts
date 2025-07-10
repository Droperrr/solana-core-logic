import { EventAggregator } from '../src/EventAggregator';
import { AtomicEvent, RawTransaction } from '../src/types';

describe('EventAggregator', () => {
  const aggregator = new EventAggregator();
  const baseTx: RawTransaction = {
    signature: 'testsig',
    meta: {
      err: null,
      fee: 5000,
      preBalances: [],
      postBalances: [],
      preTokenBalances: [],
      postTokenBalances: [],
      logMessages: [
        'Program log: Instruction: Transfer',
        'Program log: Success'
      ],
      innerInstructions: [],
    },
    transaction: {
      message: {
        accountKeys: [],
        recentBlockhash: 'dummyBlockhash',
        instructions: [],
      },
      signatures: ['dummySignature'],
    },
    blockTime: 1234567890,
    slot: 1,
  };

  // Для SWAP-сценария innerInstructions отражают структуру swap
  const swapTx: RawTransaction = {
    ...baseTx,
    meta: {
      ...baseTx.meta,
      innerInstructions: [
        {
          index: 0,
          instructions: [
            { programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', data: 'swap1' },
            { programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', data: 'swap2' }
          ]
        }
      ]
    }
  };

  it('should aggregate DEBIT and CREDIT into TRANSFER', async () => {
    const atomicEvents: AtomicEvent[] = [
      {
        type: 'DEBIT_SPL',
        account: 'A',
        amount: '100',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT',
        tokenAccount: 'A_TA',
        owner: 'A',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
      {
        type: 'CREDIT_SPL',
        account: 'B',
        amount: '100',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT',
        tokenAccount: 'B_TA',
        owner: 'B',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
    ];
    const event = await aggregator.aggregate(atomicEvents, baseTx);
    expect(event.type).toBe('TRANSFER');
    expect(event.atomicEvents.length).toBe(2);
  });

  it('should aggregate two pairs of DEBIT/CREDIT into SWAP', async () => {
    const atomicEvents: AtomicEvent[] = [
      {
        type: 'DEBIT_SPL',
        account: 'A',
        amount: '100',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT1',
        tokenAccount: 'A_TA1',
        owner: 'A',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
      {
        type: 'CREDIT_SPL',
        account: 'A',
        amount: '200',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT2',
        tokenAccount: 'A_TA2',
        owner: 'A',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
      {
        type: 'DEBIT_SPL',
        account: 'B',
        amount: '200',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT2',
        tokenAccount: 'B_TA2',
        owner: 'B',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
      {
        type: 'CREDIT_SPL',
        account: 'B',
        amount: '100',
        signature: 'testsig',
        timestamp: 1234567890,
        mint: 'MINT1',
        tokenAccount: 'B_TA1',
        owner: 'B',
        decimals: 6,
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
      },
    ];
    const event = await aggregator.aggregate(atomicEvents, swapTx);
    expect(event.type).toBe('SWAP');
    expect(event.atomicEvents.length).toBeGreaterThanOrEqual(2);
  });

  it('should aggregate empty atomic events into TRANSACTION_FAILED if tx.meta.err', async () => {
    const atomicEvents: AtomicEvent[] = [];
    const tx = { ...baseTx, meta: { ...baseTx.meta, err: { InstructionError: [0, { Custom: 1 }] } } };
    const event = await aggregator.aggregate(atomicEvents, tx);
    expect(event.type).toBe('TRANSACTION_FAILED');
  });
}); 