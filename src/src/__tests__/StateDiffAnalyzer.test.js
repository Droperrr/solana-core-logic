"use strict";
/**
 * Comprehensive tests for StateDiffAnalyzer
 */
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
Object.defineProperty(exports, "__esModule", { value: true });
var StateDiffAnalyzer_1 = require("../StateDiffAnalyzer");
describe('StateDiffAnalyzer', function () {
    var analyzer;
    beforeEach(function () {
        analyzer = new StateDiffAnalyzer_1.StateDiffAnalyzer();
    });
    describe('SOL Transfer Analysis', function () {
        it('should detect simple SOL transfer from one account to another', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            expect(events).toHaveLength(2);
            // Check debit event
            var debitEvent = events.find(function (e) { return e.type === 'DEBIT_SOL'; });
            expect(debitEvent).toBeDefined();
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.account).toBe('sender_account_1');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.amount).toBe('200000000'); // 0.2 SOL in lamports
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.signature).toBe('test_signature_1');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.timestamp).toBe(1640995200);
            // Check credit event
            var creditEvent = events.find(function (e) { return e.type === 'CREDIT_SOL'; });
            expect(creditEvent).toBeDefined();
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.account).toBe('receiver_account_1');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.amount).toBe('195000000'); // 0.195 SOL in lamports
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.signature).toBe('test_signature_1');
        });
        it('should filter out fee-related changes by default', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            // Should not generate events for fee-only transactions
            expect(events).toHaveLength(0);
        });
        it('should include fee-related changes when configured', function () {
            var analyzerWithFees = new StateDiffAnalyzer_1.StateDiffAnalyzer({
                includeFeeChanges: true,
            });
            var mockTransaction = {
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
            var events = analyzerWithFees.analyze(mockTransaction);
            expect(events).toHaveLength(1);
            expect(events[0].type).toBe('DEBIT_SOL');
            expect(events[0].amount).toBe('5000');
        });
    });
    describe('SPL Token Transfer Analysis', function () {
        it('should detect simple SPL token transfer between existing accounts', function () {
            var mockTokenBalance1 = {
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
            var mockTokenBalance2 = {
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
            var mockTransaction = {
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
                        __assign(__assign({}, mockTokenBalance1), { uiTokenAmount: __assign(__assign({}, mockTokenBalance1.uiTokenAmount), { amount: '750000', uiAmount: 0.75, uiAmountString: '0.75' }) }),
                        __assign(__assign({}, mockTokenBalance2), { uiTokenAmount: __assign(__assign({}, mockTokenBalance2.uiTokenAmount), { amount: '750000', uiAmount: 0.75, uiAmountString: '0.75' }) }),
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
            var events = analyzer.analyze(mockTransaction);
            // Should have 2 SPL token events (debit and credit)
            var splEvents = events.filter(function (e) { return e.type.includes('SPL'); });
            expect(splEvents).toHaveLength(2);
            // Check debit event
            var debitEvent = splEvents.find(function (e) { return e.type === 'DEBIT_SPL'; });
            expect(debitEvent).toBeDefined();
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.account).toBe('token_owner_1');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.amount).toBe('250000');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.mint).toBe('token_mint_1');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.tokenAccount).toBe('token_account_1');
            // Check credit event
            var creditEvent = splEvents.find(function (e) { return e.type === 'CREDIT_SPL'; });
            expect(creditEvent).toBeDefined();
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.account).toBe('token_owner_2');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.amount).toBe('250000');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.mint).toBe('token_mint_1');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.tokenAccount).toBe('token_account_2');
        });
        it('should handle token account creation scenario', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            var splEvents = events.filter(function (e) { return e.type.includes('SPL'); });
            expect(splEvents).toHaveLength(2);
            // Check debit from existing account
            var debitEvent = splEvents.find(function (e) { return e.type === 'DEBIT_SPL'; });
            expect(debitEvent).toBeDefined();
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.account).toBe('sender_owner');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.amount).toBe('500000');
            // Check credit to new account
            var creditEvent = splEvents.find(function (e) { return e.type === 'CREDIT_SPL'; });
            expect(creditEvent).toBeDefined();
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.account).toBe('receiver_owner');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.amount).toBe('500000');
        });
        it('should handle token account closure scenario', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            var splEvents = events.filter(function (e) { return e.type.includes('SPL'); });
            expect(splEvents).toHaveLength(2);
            // Check debit from closed account
            var debitEvent = splEvents.find(function (e) { return e.type === 'DEBIT_SPL'; });
            expect(debitEvent).toBeDefined();
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.account).toBe('sender_owner');
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.amount).toBe('1000000');
            // Check credit to receiver
            var creditEvent = splEvents.find(function (e) { return e.type === 'CREDIT_SPL'; });
            expect(creditEvent).toBeDefined();
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.account).toBe('receiver_owner');
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.amount).toBe('1000000');
        });
    });
    describe('Configuration Options', function () {
        it('should respect minimum balance change threshold', function () {
            var analyzerWithThreshold = new StateDiffAnalyzer_1.StateDiffAnalyzer({
                minBalanceChange: '1000000', // 1 million lamports threshold
            });
            var mockTransaction = {
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
            var events = analyzerWithThreshold.analyze(mockTransaction);
            // Should not generate events because change is below threshold
            expect(events).toHaveLength(0);
        });
        it('should handle large token amounts using BigInt', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            var splEvents = events.filter(function (e) { return e.type.includes('SPL'); });
            expect(splEvents).toHaveLength(2);
            // Verify that large amounts are handled correctly
            var debitEvent = splEvents.find(function (e) { return e.type === 'DEBIT_SPL'; });
            expect(debitEvent === null || debitEvent === void 0 ? void 0 : debitEvent.amount).toBe('9223372036854775808');
            var creditEvent = splEvents.find(function (e) { return e.type === 'CREDIT_SPL'; });
            expect(creditEvent === null || creditEvent === void 0 ? void 0 : creditEvent.amount).toBe('9223372036854775808');
        });
    });
    describe('Edge Cases', function () {
        it('should handle transactions with no balance changes', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            expect(events).toHaveLength(0);
        });
        it('should handle failed transactions', function () {
            var mockTransaction = {
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
            var events = analyzer.analyze(mockTransaction);
            // Should still analyze state changes even for failed transactions
            // (in this case, only fee deduction, which is filtered out by default)
            expect(events).toHaveLength(0);
        });
    });
});
