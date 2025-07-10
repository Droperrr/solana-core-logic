"use strict";
/**
 * StateDiffAnalyzer - Core component for analyzing Solana transaction state changes
 *
 * This class implements the "State-First" philosophy by analyzing only the differences
 * between pre and post transaction states, without parsing instructions.
 */
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.StateDiffAnalyzer = void 0;
var StateDiffAnalyzer = /** @class */ (function () {
    function StateDiffAnalyzer(config) {
        if (config === void 0) { config = {}; }
        var _a, _b, _c;
        this.config = {
            minBalanceChange: (_a = config.minBalanceChange) !== null && _a !== void 0 ? _a : '0',
            includeFeeChanges: (_b = config.includeFeeChanges) !== null && _b !== void 0 ? _b : false,
            traceTemporaryAccounts: (_c = config.traceTemporaryAccounts) !== null && _c !== void 0 ? _c : true,
        };
    }
    /**
     * Analyzes a raw transaction and returns atomic events based on state differences
     * @param rawTx - Raw transaction data from getTransaction RPC
     * @returns Array of atomic events representing balance changes
     */
    StateDiffAnalyzer.prototype.analyze = function (rawTx) {
        var events = [];
        // Analyze SOL balance changes
        var solEvents = this.analyzeSolBalanceChanges(rawTx);
        events.push.apply(events, solEvents);
        // Analyze SPL token balance changes
        var splEvents = this.analyzeSplTokenBalanceChanges(rawTx);
        events.push.apply(events, splEvents);
        return events;
    };
    /**
     * Analyzes SOL balance changes between pre and post transaction states
     */
    StateDiffAnalyzer.prototype.analyzeSolBalanceChanges = function (rawTx) {
        var _a, _b;
        var events = [];
        var _c = rawTx.meta, preBalances = _c.preBalances, postBalances = _c.postBalances;
        var accountKeys = rawTx.transaction.message.accountKeys;
        // Calculate balance changes for each account
        var balanceChanges = [];
        for (var i = 0; i < accountKeys.length; i++) {
            var preBalance = (_a = preBalances[i]) !== null && _a !== void 0 ? _a : 0;
            var postBalance = (_b = postBalances[i]) !== null && _b !== void 0 ? _b : 0;
            var change = postBalance - preBalance;
            if (change !== 0) {
                balanceChanges.push({
                    accountIndex: i,
                    account: accountKeys[i],
                    preBalance: preBalance,
                    postBalance: postBalance,
                    change: change,
                });
            }
        }
        // Filter out fee-related changes if configured
        var filteredChanges = this.config.includeFeeChanges
            ? balanceChanges
            : this.filterFeeChanges(balanceChanges, rawTx.meta.fee);
        // Convert balance changes to atomic events
        for (var _i = 0, filteredChanges_1 = filteredChanges; _i < filteredChanges_1.length; _i++) {
            var balanceChange = filteredChanges_1[_i];
            var absChange = Math.abs(balanceChange.change);
            var minChange = parseInt(this.config.minBalanceChange, 10);
            if (absChange >= minChange) {
                var eventType = balanceChange.change > 0 ? 'CREDIT_SOL' : 'DEBIT_SOL';
                events.push({
                    type: eventType,
                    account: balanceChange.account,
                    amount: Math.abs(balanceChange.change).toString(),
                    signature: rawTx.signature,
                    timestamp: rawTx.blockTime,
                    currency: 'SOL',
                });
            }
        }
        return events;
    };
    /**
     * Analyzes SPL token balance changes between pre and post transaction states
     */
    StateDiffAnalyzer.prototype.analyzeSplTokenBalanceChanges = function (rawTx) {
        var events = [];
        var _a = rawTx.meta, preTokenBalances = _a.preTokenBalances, postTokenBalances = _a.postTokenBalances;
        var accountKeys = rawTx.transaction.message.accountKeys;
        // Create maps for efficient lookup
        var preBalanceMap = new Map();
        var postBalanceMap = new Map();
        for (var _i = 0, preTokenBalances_1 = preTokenBalances; _i < preTokenBalances_1.length; _i++) {
            var balance = preTokenBalances_1[_i];
            preBalanceMap.set(balance.accountIndex, balance);
        }
        for (var _b = 0, postTokenBalances_1 = postTokenBalances; _b < postTokenBalances_1.length; _b++) {
            var balance = postTokenBalances_1[_b];
            postBalanceMap.set(balance.accountIndex, balance);
        }
        // Find all accounts that had token balance changes
        var allAccountIndices = new Set(__spreadArray(__spreadArray([], preTokenBalances.map(function (b) { return b.accountIndex; }), true), postTokenBalances.map(function (b) { return b.accountIndex; }), true));
        var tokenBalanceChanges = [];
        for (var _c = 0, allAccountIndices_1 = allAccountIndices; _c < allAccountIndices_1.length; _c++) {
            var accountIndex = allAccountIndices_1[_c];
            var preBalance = preBalanceMap.get(accountIndex);
            var postBalance = postBalanceMap.get(accountIndex);
            // Handle different scenarios
            if (preBalance && postBalance) {
                // Account existed before and after - check for balance change
                if (preBalance.uiTokenAmount.amount !== postBalance.uiTokenAmount.amount) {
                    var change = this.calculateTokenAmountChange(preBalance.uiTokenAmount.amount, postBalance.uiTokenAmount.amount);
                    tokenBalanceChanges.push({
                        accountIndex: accountIndex,
                        tokenAccount: accountKeys[accountIndex],
                        owner: postBalance.owner,
                        mint: postBalance.mint,
                        decimals: postBalance.uiTokenAmount.decimals,
                        programId: postBalance.programId,
                        preAmount: preBalance.uiTokenAmount.amount,
                        postAmount: postBalance.uiTokenAmount.amount,
                        change: change,
                    });
                }
            }
            else if (!preBalance && postBalance) {
                // Account was created and received tokens
                if (postBalance.uiTokenAmount.amount !== '0') {
                    tokenBalanceChanges.push({
                        accountIndex: accountIndex,
                        tokenAccount: accountKeys[accountIndex],
                        owner: postBalance.owner,
                        mint: postBalance.mint,
                        decimals: postBalance.uiTokenAmount.decimals,
                        programId: postBalance.programId,
                        preAmount: '0',
                        postAmount: postBalance.uiTokenAmount.amount,
                        change: postBalance.uiTokenAmount.amount,
                    });
                }
            }
            else if (preBalance && !postBalance) {
                // Account was closed/destroyed
                if (preBalance.uiTokenAmount.amount !== '0') {
                    tokenBalanceChanges.push({
                        accountIndex: accountIndex,
                        tokenAccount: accountKeys[accountIndex],
                        owner: preBalance.owner,
                        mint: preBalance.mint,
                        decimals: preBalance.uiTokenAmount.decimals,
                        programId: preBalance.programId,
                        preAmount: preBalance.uiTokenAmount.amount,
                        postAmount: '0',
                        change: "-".concat(preBalance.uiTokenAmount.amount),
                    });
                }
            }
        }
        // Process temporary account transfers if enabled
        var processedChanges = this.config.traceTemporaryAccounts
            ? this.processTemporaryAccountTransfers(tokenBalanceChanges)
            : tokenBalanceChanges;
        // Convert token balance changes to atomic events
        for (var _d = 0, processedChanges_1 = processedChanges; _d < processedChanges_1.length; _d++) {
            var change = processedChanges_1[_d];
            var minChange = BigInt(this.config.minBalanceChange);
            var changeAmount = BigInt(change.change);
            var absChange = changeAmount < 0n ? -changeAmount : changeAmount;
            if (absChange >= minChange) {
                var isPositive = changeAmount > 0n;
                var eventType = isPositive ? 'CREDIT_SPL' : 'DEBIT_SPL';
                events.push({
                    type: eventType,
                    account: change.owner, // Use owner address, not token account
                    amount: absChange.toString(),
                    signature: rawTx.signature,
                    timestamp: rawTx.blockTime,
                    mint: change.mint,
                    tokenAccount: change.tokenAccount,
                    owner: change.owner,
                    decimals: change.decimals,
                    programId: change.programId,
                });
            }
        }
        return events;
    };
    /**
     * Filters out fee-related balance changes
     */
    StateDiffAnalyzer.prototype.filterFeeChanges = function (changes, fee) {
        // Simple heuristic: if there's exactly one debit that matches the fee amount,
        // it's likely the fee payment
        var feeDebits = changes.filter(function (c) { return c.change === -fee; });
        if (feeDebits.length === 1) {
            return changes.filter(function (c) { return c !== feeDebits[0]; });
        }
        return changes;
    };
    /**
     * Calculates the change between two token amounts (as strings)
     */
    StateDiffAnalyzer.prototype.calculateTokenAmountChange = function (preAmount, postAmount) {
        var pre = BigInt(preAmount);
        var post = BigInt(postAmount);
        var change = post - pre;
        return change.toString();
    };
    /**
     * Processes temporary account transfers to trace the final beneficiaries
     * This is a simplified implementation - in practice, this would need more
     * sophisticated logic to handle complex temporary account scenarios
     */
    StateDiffAnalyzer.prototype.processTemporaryAccountTransfers = function (changes) {
        // For now, return changes as-is
        // TODO: Implement logic to trace temporary ATA transfers
        // This would involve detecting patterns where tokens flow through temporary accounts
        // and consolidating them into direct transfers
        return changes;
    };
    return StateDiffAnalyzer;
}());
exports.StateDiffAnalyzer = StateDiffAnalyzer;
