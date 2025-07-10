"use strict";
/**
 * Comprehensive tests for EventAggregator
 */
Object.defineProperty(exports, "__esModule", { value: true });
var EventAggregator_1 = require("../EventAggregator");
describe('EventAggregator', function () {
    var aggregator;
    beforeEach(function () {
        aggregator = new EventAggregator_1.EventAggregator();
    });
    describe('Failed Transaction Handling', function () {
        it('should create FailedTransactionEvent for failed transactions', function () {
            var atomicEvents = [
                {
                    type: 'DEBIT_SOL',
                    account: 'fee_payer_account',
                    amount: '5000',
                    signature: 'failed_tx_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
            ];
            var mockTransaction = {
                signature: 'failed_tx_sig',
                blockTime: 1640995200,
                slot: 123456,
                meta: {
                    err: { InstructionError: [0, 'Custom error'] },
                    fee: 5000,
                    preBalances: [1000000000],
                    postBalances: [999995000],
                    preTokenBalances: [],
                    postTokenBalances: [],
                    logMessages: ['Program log: Error occurred'],
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
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('TRANSACTION_FAILED');
            var failedEvent = result;
            expect(failedEvent.error).toEqual({
                InstructionError: [0, 'Custom error'],
            });
            expect(failedEvent.feePaid).toBe('5000');
            expect(failedEvent.feePayer).toBe('fee_payer_account');
            expect(failedEvent.atomicEvents).toEqual(atomicEvents);
        });
    });
    describe('Swap Event Detection', function () {
        it('should detect simple token swap', function () {
            var atomicEvents = [
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
                },
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
                },
            ];
            var mockTransaction = {
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
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('SWAP');
            var swapEvent = result;
            expect(swapEvent.swapper).toBe('swapper_account');
            expect(swapEvent.tokenIn.mint).toBe('token_a_mint');
            expect(swapEvent.tokenIn.amount).toBe('1000000');
            expect(swapEvent.tokenOut.mint).toBe('token_b_mint');
            expect(swapEvent.tokenOut.amount).toBe('2000000');
            expect(swapEvent.rate).toBe('2'); // 2000000 / 1000000
            expect(swapEvent.atomicEvents).toHaveLength(2);
        });
        it('should detect SOL to SPL token swap', function () {
            var atomicEvents = [
                {
                    type: 'DEBIT_SOL',
                    account: 'swapper_account',
                    amount: '1000000000', // 1 SOL
                    signature: 'sol_swap_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
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
                },
            ];
            var mockTransaction = {
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
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('SWAP');
            var swapEvent = result;
            expect(swapEvent.swapper).toBe('swapper_account');
            expect(swapEvent.tokenIn.mint).toBe('SOL');
            expect(swapEvent.tokenIn.amount).toBe('1000000000');
            expect(swapEvent.tokenOut.mint).toBe('usdc_mint');
            expect(swapEvent.tokenOut.amount).toBe('100000000');
        });
    });
    describe('Transfer Event Detection', function () {
        it('should detect simple SPL token transfer', function () {
            var atomicEvents = [
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
                },
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
                },
            ];
            var mockTransaction = {
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
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('TRANSFER');
            var transferEvent = result;
            expect(transferEvent.sender).toBe('sender_account');
            expect(transferEvent.receiver).toBe('receiver_account');
            expect(transferEvent.token.mint).toBe('usdc_mint');
            expect(transferEvent.token.amount).toBe('1000000');
            expect(transferEvent.token.decimals).toBe(6);
            expect(transferEvent.transferType).toBe('DIRECT');
        });
        it('should detect SOL transfer', function () {
            var atomicEvents = [
                {
                    type: 'DEBIT_SOL',
                    account: 'sender_account',
                    amount: '500000000', // 0.5 SOL
                    signature: 'sol_transfer_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
                {
                    type: 'CREDIT_SOL',
                    account: 'receiver_account',
                    amount: '500000000', // 0.5 SOL
                    signature: 'sol_transfer_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
            ];
            var mockTransaction = {
                signature: 'sol_transfer_sig',
                blockTime: 1640995200,
                slot: 123456,
                meta: {
                    err: null,
                    fee: 5000,
                    preBalances: [1000000000, 0],
                    postBalances: [499995000, 500000000], // Sender lost 0.5 SOL + fee, receiver got 0.5 SOL
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
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('TRANSFER');
            var transferEvent = result;
            expect(transferEvent.sender).toBe('sender_account');
            expect(transferEvent.receiver).toBe('receiver_account');
            expect(transferEvent.token.mint).toBe('SOL');
            expect(transferEvent.token.amount).toBe('500000000');
            expect(transferEvent.token.decimals).toBe(9);
        });
    });
    describe('Complex Transaction Handling', function () {
        it('should create ComplexTransactionEvent for multi-operation transactions', function () {
            var aggregatorWithComplexEvents = new EventAggregator_1.EventAggregator({
                generateComplexEvents: true,
            });
            var atomicEvents = [
                {
                    type: 'DEBIT_SPL',
                    account: 'account_1',
                    amount: '1000000',
                    signature: 'complex_tx_sig',
                    timestamp: 1640995200,
                    mint: 'token_a_mint',
                    tokenAccount: 'token_a_account_1',
                    owner: 'account_1',
                    decimals: 6,
                    programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
                {
                    type: 'CREDIT_SPL',
                    account: 'account_2',
                    amount: '2000000',
                    signature: 'complex_tx_sig',
                    timestamp: 1640995200,
                    mint: 'token_b_mint',
                    tokenAccount: 'token_b_account_2',
                    owner: 'account_2',
                    decimals: 6,
                    programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
                {
                    type: 'DEBIT_SOL',
                    account: 'account_3',
                    amount: '100000000',
                    signature: 'complex_tx_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
            ];
            var mockTransaction = {
                signature: 'complex_tx_sig',
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
                    signatures: ['complex_tx_sig'],
                },
            };
            var result = aggregatorWithComplexEvents.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('COMPLEX_TRANSACTION');
            expect(result.atomicEvents).toHaveLength(3);
        });
    });
    describe('Unknown Transaction Handling', function () {
        it('should create UnknownTransactionEvent for unrecognizable patterns', function () {
            var atomicEvents = [
                {
                    type: 'DEBIT_SPL',
                    account: 'account_1',
                    amount: '1000000',
                    signature: 'unknown_tx_sig',
                    timestamp: 1640995200,
                    mint: 'token_a_mint',
                    tokenAccount: 'token_a_account',
                    owner: 'account_1',
                    decimals: 6,
                    programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
            ];
            var mockTransaction = {
                signature: 'unknown_tx_sig',
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
                    signatures: ['unknown_tx_sig'],
                },
            };
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('UNKNOWN_TRANSACTION');
            expect(result.atomicEvents).toEqual(atomicEvents);
        });
        it('should create UnknownTransactionEvent when no events after filtering', function () {
            var atomicEvents = [
                {
                    type: 'DEBIT_SOL',
                    account: 'fee_payer',
                    amount: '5000', // Exactly the fee amount
                    signature: 'fee_only_tx_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
            ];
            var mockTransaction = {
                signature: 'fee_only_tx_sig',
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
                    signatures: ['fee_only_tx_sig'],
                },
            };
            var result = aggregator.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('UNKNOWN_TRANSACTION');
            expect(result.atomicEvents).toEqual(atomicEvents);
        });
    });
    describe('Configuration Options', function () {
        it('should include fee events when configured', function () {
            var aggregatorWithFees = new EventAggregator_1.EventAggregator({
                includeFeeEvents: true,
            });
            var atomicEvents = [
                {
                    type: 'DEBIT_SOL',
                    account: 'fee_payer',
                    amount: '5000',
                    signature: 'fee_tx_sig',
                    timestamp: 1640995200,
                    currency: 'SOL',
                },
            ];
            var mockTransaction = {
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
            var result = aggregatorWithFees.aggregate(atomicEvents, mockTransaction);
            // Should not filter out the fee event, so it should be processed
            expect(result.atomicEvents).toEqual(atomicEvents);
        });
        it('should respect amount tolerance for transfers', function () {
            var aggregatorWithTolerance = new EventAggregator_1.EventAggregator({
                amountTolerance: 0.01, // 1% tolerance
            });
            var atomicEvents = [
                {
                    type: 'DEBIT_SPL',
                    account: 'sender_account',
                    amount: '1000000',
                    signature: 'tolerance_tx_sig',
                    timestamp: 1640995200,
                    mint: 'usdc_mint',
                    tokenAccount: 'sender_usdc_account',
                    owner: 'sender_account',
                    decimals: 6,
                    programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
                {
                    type: 'CREDIT_SPL',
                    account: 'receiver_account',
                    amount: '1005000', // 0.5% difference, within 1% tolerance
                    signature: 'tolerance_tx_sig',
                    timestamp: 1640995200,
                    mint: 'usdc_mint',
                    tokenAccount: 'receiver_usdc_account',
                    owner: 'receiver_account',
                    decimals: 6,
                    programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
            ];
            var mockTransaction = {
                signature: 'tolerance_tx_sig',
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
                    signatures: ['tolerance_tx_sig'],
                },
            };
            var result = aggregatorWithTolerance.aggregate(atomicEvents, mockTransaction);
            expect(result.type).toBe('TRANSFER');
            var transferEvent = result;
            expect(transferEvent.sender).toBe('sender_account');
            expect(transferEvent.receiver).toBe('receiver_account');
        });
    });
});
