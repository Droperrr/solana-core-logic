/**
 * Comprehensive tests for EventAggregator
 */

import { EventAggregator } from '../EventAggregator';
import {
  AtomicEvent,
  RawTransaction,
  SwapEvent,
  TransferEvent,
  FailedTransactionEvent,
  ComplexTransactionEvent,
  UnknownTransactionEvent,
  SplTokenTransferEvent,
  SolTransferEvent,
} from '../types';

describe('EventAggregator', () => {
  let aggregator: EventAggregator;

  beforeEach(() => {
    aggregator = new EventAggregator();
  });

  describe('Failed Transaction Handling', () => {
    it('should create FailedTransactionEvent for failed transactions', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'fee_payer_account',
          amount: '5000',
          signature: 'fee_tx_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'failed_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: {
            InstructionError: [0, 'Custom error'],
          },
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['failed_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('TRANSACTION_FAILED');
      const failedEvent = result as FailedTransactionEvent;
      expect(failedEvent.error).toEqual({
        InstructionError: [0, 'Custom error'],
      });
      expect(failedEvent.feePaid).toBe('5000');
      expect(failedEvent.feePayer).toBe('fee_payer_account');
      expect(failedEvent.atomicEvents).toEqual(atomicEvents);
    });
  });

  describe('Swap Event Detection', () => {
    it('should detect simple token swap', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SPL',
          account: 'swapper_account',
          amount: '1000000',
          signature: 'swap_tx_sig',
          timestamp: 1640995200,
          mint: 'token_a_mint',
          tokenAccount: 'token_a_account',
          owner: 'swapper_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
        {
          type: 'CREDIT_SPL',
          account: 'swapper_account',
          amount: '2000000',
          signature: 'swap_tx_sig',
          timestamp: 1640995200,
          mint: 'token_b_mint',
          tokenAccount: 'token_b_account',
          owner: 'swapper_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'swap_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['swap_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('SWAP');
      const swapEvent = result as SwapEvent;
      expect(swapEvent.swapper).toBe('swapper_account');
      expect(swapEvent.tokenIn.mint).toBe('token_a_mint');
      expect(swapEvent.tokenIn.amount).toBe('1000000');
      expect(swapEvent.tokenOut.mint).toBe('token_b_mint');
      expect(swapEvent.tokenOut.amount).toBe('2000000');
      expect(swapEvent.rate).toBe('2'); // 2000000 / 1000000
      expect(swapEvent.atomicEvents).toHaveLength(2);
    });

    it('should detect SOL to SPL token swap', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'swapper_account',
          amount: '1000000000', // 1 SOL
          signature: 'sol_swap_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
        {
          type: 'CREDIT_SPL',
          account: 'swapper_account',
          amount: '100000000', // 100 tokens
          signature: 'sol_swap_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'usdc_account',
          owner: 'swapper_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'sol_swap_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [2000000000, 0],
          postBalances: [999995000, 0], // Lost 1 SOL + fee
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['swapper_account', 'program_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['sol_swap_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('SWAP');
      const swapEvent = result as SwapEvent;
      expect(swapEvent.swapper).toBe('swapper_account');
      expect(swapEvent.tokenIn.mint).toBe('SOL');
      expect(swapEvent.tokenIn.amount).toBe('1000000000');
      expect(swapEvent.tokenOut.mint).toBe('usdc_mint');
      expect(swapEvent.tokenOut.amount).toBe('100000000');
    });
  });

  describe('Transfer Event Detection', () => {
    it('should detect simple SPL token transfer', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SPL',
          account: 'sender_account',
          amount: '1000000',
          signature: 'transfer_tx_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'sender_usdc_account',
          owner: 'sender_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
        {
          type: 'CREDIT_SPL',
          account: 'receiver_account',
          amount: '1000000',
          signature: 'transfer_tx_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'receiver_usdc_account',
          owner: 'receiver_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'transfer_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['transfer_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('TRANSFER');
      const transferEvent = result as TransferEvent;
      expect(transferEvent.sender).toBe('sender_account');
      expect(transferEvent.receiver).toBe('receiver_account');
      expect(transferEvent.token.mint).toBe('usdc_mint');
      expect(transferEvent.token.amount).toBe('1000000');
      expect(transferEvent.token.decimals).toBe(6);
      expect(transferEvent.transferType).toBe('DIRECT');
    });

    it('should detect SOL transfer', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'sender_account',
          amount: '1000000000', // 1 SOL
          signature: 'sol_transfer_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
        {
          type: 'CREDIT_SOL',
          account: 'receiver_account',
          amount: '1000000000', // 1 SOL
          signature: 'sol_transfer_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'sol_transfer_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [2000000000, 0],
          postBalances: [999995000, 1000000000], // Lost 1 SOL + fee, gained 1 SOL
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['sender_account', 'receiver_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['sol_transfer_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('TRANSFER');
      const transferEvent = result as TransferEvent;
      expect(transferEvent.sender).toBe('sender_account');
      expect(transferEvent.receiver).toBe('receiver_account');
      expect(transferEvent.token.mint).toBe('SOL');
      expect(transferEvent.token.amount).toBe('1000000000');
      expect(transferEvent.token.decimals).toBe(9);
    });
  });

  describe('Complex Transaction Handling', () => {
    it('should create ComplexTransactionEvent for multi-operation transactions', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'user_account',
          amount: '1000000000',
          signature: 'complex_tx_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
        {
          type: 'CREDIT_SOL',
          account: 'receiver_account',
          amount: '1000000000',
          signature: 'complex_tx_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
        {
          type: 'DEBIT_SPL',
          account: 'user_account',
          amount: '500000',
          signature: 'complex_tx_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'user_usdc_account',
          owner: 'user_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'complex_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [2000000000, 0],
          postBalances: [999995000, 1000000000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['user_account', 'receiver_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['complex_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('COMPLEX_TRANSACTION');
      expect(result.atomicEvents).toHaveLength(3);
    });
  });

  describe('Unknown Transaction Handling', () => {
    it('should create UnknownTransactionEvent for unrecognizable patterns', async () => {
      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'unknown_account',
          amount: '1000000',
          signature: 'unknown_tx_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'unknown_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999000000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['unknown_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['unknown_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('UNKNOWN_TRANSACTION');
      expect(result.atomicEvents).toEqual(atomicEvents);
    });

    it('should create UnknownTransactionEvent when no events after filtering', async () => {
      const atomicEvents: AtomicEvent[] = [];

      const mockTransaction: RawTransaction = {
        signature: 'empty_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['empty_tx_sig'],
        },
      };

      const result = await aggregator.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('UNKNOWN_TRANSACTION');
      expect(result.atomicEvents).toEqual(atomicEvents);
    });
  });

  describe('Configuration Options', () => {
    it('should include fee events when configured', async () => {
      const aggregatorWithFees = new EventAggregator({ includeFeeEvents: true });

      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SOL',
          account: 'fee_payer',
          amount: '5000',
          signature: 'fee_tx_sig',
          timestamp: 1640995200,
          currency: 'SOL',
        } as SolTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'fee_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['fee_tx_sig'],
        },
      };

      const result = await aggregatorWithFees.aggregate(atomicEvents, mockTransaction);

      // Should not filter out the fee event, so it should be processed
      expect(result.atomicEvents).toEqual(atomicEvents);
    });

    it('should respect amount tolerance for transfers', async () => {
      const aggregatorWithTolerance = new EventAggregator({ amountTolerance: 0.01 }); // 1% tolerance

      const atomicEvents: AtomicEvent[] = [
        {
          type: 'DEBIT_SPL',
          account: 'sender_account',
          amount: '1000000',
          signature: 'transfer_tx_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'sender_usdc_account',
          owner: 'sender_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
        {
          type: 'CREDIT_SPL',
          account: 'receiver_account',
          amount: '999000', // 0.1% difference (within 1% tolerance)
          signature: 'transfer_tx_sig',
          timestamp: 1640995200,
          mint: 'usdc_mint',
          tokenAccount: 'receiver_usdc_account',
          owner: 'receiver_account',
          decimals: 6,
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        } as SplTokenTransferEvent,
      ];

      const mockTransaction: RawTransaction = {
        signature: 'transfer_tx_sig',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000],
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['transfer_tx_sig'],
        },
      };

      const result = await aggregatorWithTolerance.aggregate(atomicEvents, mockTransaction);

      expect(result.type).toBe('TRANSFER');
      const transferEvent = result as TransferEvent;
      expect(transferEvent.sender).toBe('sender_account');
      expect(transferEvent.receiver).toBe('receiver_account');
      expect(transferEvent.token.mint).toBe('usdc_mint');
      expect(transferEvent.token.amount).toBe('1000000');
    });
  });
});
