/**
 * Comprehensive tests for StateDiffAnalyzer
 */

import { StateDiffAnalyzer } from '../StateDiffAnalyzer';
import { RawTransaction, SplTokenTransferEvent, TokenBalance } from '../types';

describe('StateDiffAnalyzer', () => {
  let analyzer: StateDiffAnalyzer;

  beforeEach(() => {
    analyzer = new StateDiffAnalyzer();
  });

  describe('SOL Transfer Analysis', () => {
    it('should detect simple SOL transfer from one account to another', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_1',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 500000000], // 1 SOL, 0.5 SOL
          postBalances: [800000000, 695000000], // 0.8 SOL, 0.695 SOL (received 0.2 SOL, paid 0.005 SOL fee)
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['sender_account_1', 'receiver_account_1'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_1'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      expect(events).toHaveLength(2);

      // Check debit event
      const debitEvent = events.find(e => e.type === 'DEBIT_SOL');
      expect(debitEvent).toBeDefined();
      expect(debitEvent?.account).toBe('sender_account_1');
      expect(debitEvent?.amount).toBe('200000000'); // 0.2 SOL in lamports
      expect(debitEvent?.signature).toBe('test_signature_1');
      expect(debitEvent?.timestamp).toBe(1640995200);

      // Check credit event
      const creditEvent = events.find(e => e.type === 'CREDIT_SOL');
      expect(creditEvent).toBeDefined();
      expect(creditEvent?.account).toBe('receiver_account_1');
      expect(creditEvent?.amount).toBe('195000000'); // 0.195 SOL in lamports
      expect(creditEvent?.signature).toBe('test_signature_1');
    });

    it('should filter out fee-related changes by default', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_fee',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000], // 1 SOL
          postBalances: [999995000], // 0.999995 SOL (only fee deducted)
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
          signatures: ['test_signature_fee'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      // Should not generate events for fee-only transactions
      expect(events).toHaveLength(0);
    });

    it('should include fee-related changes when configured', () => {
      const analyzerWithFees = new StateDiffAnalyzer({
        includeFeeChanges: true,
      });

      const mockTransaction: RawTransaction = {
        signature: 'test_signature_fee_included',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000], // 1 SOL
          postBalances: [999995000], // 0.999995 SOL (only fee deducted)
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
          signatures: ['test_signature_fee_included'],
        },
      };

      const events = analyzerWithFees.analyze(mockTransaction);

      expect(events).toHaveLength(1);
      expect(events[0].type).toBe('DEBIT_SOL');
      expect(events[0].amount).toBe('5000');
    });
  });

  describe('SPL Token Transfer Analysis', () => {
    it('should detect simple SPL token transfer between existing accounts', () => {
      const mockTokenBalance1: TokenBalance = {
        accountIndex: 1,
        mint: 'token_mint_1',
        owner: 'token_owner_1',
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        uiTokenAmount: {
          amount: '1000000',
          decimals: 6,
          uiAmount: 1.0,
          uiAmountString: '1',
        },
      };

      const mockTokenBalance2: TokenBalance = {
        accountIndex: 2,
        mint: 'token_mint_1',
        owner: 'token_owner_2',
        programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        uiTokenAmount: {
          amount: '500000',
          decimals: 6,
          uiAmount: 0.5,
          uiAmountString: '0.5',
        },
      };

      const mockTransaction: RawTransaction = {
        signature: 'test_signature_spl',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 0, 0], // SOL balances
          postBalances: [995000000, 0, 0], // SOL balances after fee
          preTokenBalances: [mockTokenBalance1, mockTokenBalance2],
          postTokenBalances: [
            {
              ...mockTokenBalance1,
              uiTokenAmount: {
                ...mockTokenBalance1.uiTokenAmount,
                amount: '750000', // Sent 0.25 tokens
                uiAmount: 0.75,
                uiAmountString: '0.75',
              },
            },
            {
              ...mockTokenBalance2,
              uiTokenAmount: {
                ...mockTokenBalance2.uiTokenAmount,
                amount: '750000', // Received 0.25 tokens
                uiAmount: 0.75,
                uiAmountString: '0.75',
              },
            },
          ],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['fee_payer', 'token_account_1', 'token_account_2'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_spl'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      // Debug: Print all events to understand what's happening
      console.log('All events:', JSON.stringify(events, null, 2));
      console.log('Pre token balances:', JSON.stringify(mockTransaction.meta.preTokenBalances, null, 2));
      console.log('Post token balances:', JSON.stringify(mockTransaction.meta.postTokenBalances, null, 2));

      // Should have 2 SPL token events (debit and credit)
      const splEvents = events.filter(e => e.type.includes('SPL'));
      expect(splEvents).toHaveLength(2);

      // Check debit event
      const debitEvent = splEvents.find(
        e => e.type === 'DEBIT_SPL'
      ) as SplTokenTransferEvent;
      expect(debitEvent).toBeDefined();
      expect(debitEvent?.account).toBe('token_owner_1');
      expect(debitEvent?.amount).toBe('250000');
      expect(debitEvent?.mint).toBe('token_mint_1');
      expect(debitEvent?.tokenAccount).toBe('token_account_1');

      // Check credit event
      const creditEvent = splEvents.find(
        e => e.type === 'CREDIT_SPL'
      ) as SplTokenTransferEvent;
      expect(creditEvent).toBeDefined();
      expect(creditEvent?.account).toBe('token_owner_2');
      expect(creditEvent?.amount).toBe('250000');
      expect(creditEvent?.mint).toBe('token_mint_1');
      expect(creditEvent?.tokenAccount).toBe('token_account_2');
    });

    it('should handle token account creation scenario', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_ata_creation',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 0, 0], // Fee payer, sender token account, new ATA
          postBalances: [995000000, 0, 2039280], // Fee payer after fee, sender, new ATA with rent
          preTokenBalances: [
            {
              accountIndex: 1,
              mint: 'token_mint_1',
              owner: 'sender_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '1000000',
                decimals: 6,
                uiAmount: 1.0,
                uiAmountString: '1',
              },
            },
          ],
          postTokenBalances: [
            {
              accountIndex: 1,
              mint: 'token_mint_1',
              owner: 'sender_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '500000',
                decimals: 6,
                uiAmount: 0.5,
                uiAmountString: '0.5',
              },
            },
            {
              accountIndex: 2,
              mint: 'token_mint_1',
              owner: 'receiver_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '500000',
                decimals: 6,
                uiAmount: 0.5,
                uiAmountString: '0.5',
              },
            },
          ],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: [
              'fee_payer',
              'sender_token_account',
              'new_ata_account',
            ],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_ata_creation'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      const splEvents = events.filter(e => e.type.includes('SPL'));
      expect(splEvents).toHaveLength(2);

      // Check debit from existing account
      const debitEvent = splEvents.find(
        e => e.type === 'DEBIT_SPL'
      ) as SplTokenTransferEvent;
      expect(debitEvent).toBeDefined();
      expect(debitEvent?.account).toBe('sender_owner');
      expect(debitEvent?.amount).toBe('500000');

      // Check credit to new account
      const creditEvent = splEvents.find(
        e => e.type === 'CREDIT_SPL'
      ) as SplTokenTransferEvent;
      expect(creditEvent).toBeDefined();
      expect(creditEvent?.account).toBe('receiver_owner');
      expect(creditEvent?.amount).toBe('500000');
    });

    it('should handle token account closure scenario', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_account_closure',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 2039280, 0], // Fee payer, token account to be closed, receiver
          postBalances: [997000000, 0, 2039280], // Fee payer after fee, closed account, receiver got rent
          preTokenBalances: [
            {
              accountIndex: 1,
              mint: 'token_mint_1',
              owner: 'sender_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '1000000',
                decimals: 6,
                uiAmount: 1.0,
                uiAmountString: '1',
              },
            },
            {
              accountIndex: 2,
              mint: 'token_mint_1',
              owner: 'receiver_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '500000',
                decimals: 6,
                uiAmount: 0.5,
                uiAmountString: '0.5',
              },
            },
          ],
          postTokenBalances: [
            // Token account 1 is closed (not in postTokenBalances)
            {
              accountIndex: 2,
              mint: 'token_mint_1',
              owner: 'receiver_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '1500000', // Received all tokens
                decimals: 6,
                uiAmount: 1.5,
                uiAmountString: '1.5',
              },
            },
          ],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: [
              'fee_payer',
              'sender_token_account',
              'receiver_token_account',
            ],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_account_closure'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      const splEvents = events.filter(e => e.type.includes('SPL'));
      expect(splEvents).toHaveLength(2);

      // Check debit from closed account
      const debitEvent = splEvents.find(
        e => e.type === 'DEBIT_SPL'
      ) as SplTokenTransferEvent;
      expect(debitEvent).toBeDefined();
      expect(debitEvent?.account).toBe('sender_owner');
      expect(debitEvent?.amount).toBe('1000000');

      // Check credit to receiver
      const creditEvent = splEvents.find(
        e => e.type === 'CREDIT_SPL'
      ) as SplTokenTransferEvent;
      expect(creditEvent).toBeDefined();
      expect(creditEvent?.account).toBe('receiver_owner');
      expect(creditEvent?.amount).toBe('1000000');
    });
  });

  describe('Configuration Options', () => {
    it('should respect minimum balance change threshold', () => {
      const analyzerWithThreshold = new StateDiffAnalyzer({
        minBalanceChange: '1000000', // 1 million lamports threshold
      });

      const mockTransaction: RawTransaction = {
        signature: 'test_signature_threshold',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 500000000],
          postBalances: [999500000, 500500000], // Small transfer of 0.5 million lamports
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['sender', 'receiver'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_threshold'],
        },
      };

      const events = analyzerWithThreshold.analyze(mockTransaction);

      // Should not generate events because change is below threshold
      expect(events).toHaveLength(0);
    });

    it('should handle large token amounts using BigInt', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_large_amounts',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 5000,
          preBalances: [1000000000, 0, 0],
          postBalances: [995000000, 0, 0],
          preTokenBalances: [
            {
              accountIndex: 1,
              mint: 'token_mint_1',
              owner: 'sender_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '18446744073709551615', // Max uint64 - 1
                decimals: 18,
                uiAmount: null,
                uiAmountString: '18446744073709551615',
              },
            },
          ],
          postTokenBalances: [
            {
              accountIndex: 1,
              mint: 'token_mint_1',
              owner: 'sender_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '9223372036854775807', // Half of the original
                decimals: 18,
                uiAmount: null,
                uiAmountString: '9223372036854775807',
              },
            },
            {
              accountIndex: 2,
              mint: 'token_mint_1',
              owner: 'receiver_owner',
              programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
              uiTokenAmount: {
                amount: '9223372036854775808', // Other half + 1
                decimals: 18,
                uiAmount: null,
                uiAmountString: '9223372036854775808',
              },
            },
          ],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: [
              'fee_payer',
              'sender_token_account',
              'receiver_token_account',
            ],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_large_amounts'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      const splEvents = events.filter(e => e.type.includes('SPL'));
      expect(splEvents).toHaveLength(2);

      // Verify that large amounts are handled correctly
      const debitEvent = splEvents.find(
        e => e.type === 'DEBIT_SPL'
      ) as SplTokenTransferEvent;
      expect(debitEvent?.amount).toBe('9223372036854775808');

      const creditEvent = splEvents.find(
        e => e.type === 'CREDIT_SPL'
      ) as SplTokenTransferEvent;
      expect(creditEvent?.amount).toBe('9223372036854775808');
    });
  });

  describe('Edge Cases', () => {
    it('should handle transactions with no balance changes', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_no_changes',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: null,
          fee: 0,
          preBalances: [1000000000],
          postBalances: [1000000000], // No change
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['unchanged_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_no_changes'],
        },
      };

      const events = analyzer.analyze(mockTransaction);
      expect(events).toHaveLength(0);
    });

    it('should handle failed transactions', () => {
      const mockTransaction: RawTransaction = {
        signature: 'test_signature_failed',
        blockTime: 1640995200,
        slot: 123456,
        meta: {
          err: { InstructionError: [0, 'Custom error'] },
          fee: 5000,
          preBalances: [1000000000],
          postBalances: [999995000], // Only fee deducted
          preTokenBalances: [],
          postTokenBalances: [],
          logMessages: [],
          innerInstructions: [],
        },
        transaction: {
          message: {
            accountKeys: ['failed_tx_account'],
            recentBlockhash: 'recent_blockhash',
            instructions: [],
          },
          signatures: ['test_signature_failed'],
        },
      };

      const events = analyzer.analyze(mockTransaction);

      // Should still analyze state changes even for failed transactions
      // (in this case, only fee deduction, which is filtered out by default)
      expect(events).toHaveLength(0);
    });
  });
});
