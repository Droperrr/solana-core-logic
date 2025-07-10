"use strict";
/**
 * EventAggregator - Aggregates atomic events into semantic business events
 *
 * This class takes atomic events from StateDiffAnalyzer and interprets them
 * into meaningful business operations like SWAP, TRANSFER, etc.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EventAggregator = void 0;
var EventAggregator = /** @class */ (function () {
    function EventAggregator(config, enrichers) {
        if (config === void 0) { config = {}; }
        if (enrichers === void 0) { enrichers = []; }
        var _a, _b, _c, _d;
        this.config = {
            amountTolerance: (_a = config.amountTolerance) !== null && _a !== void 0 ? _a : 0.001, // 0.1% tolerance
            includeFeeEvents: (_b = config.includeFeeEvents) !== null && _b !== void 0 ? _b : false,
            maxSwapHops: (_c = config.maxSwapHops) !== null && _c !== void 0 ? _c : 3,
            generateComplexEvents: (_d = config.generateComplexEvents) !== null && _d !== void 0 ? _d : true,
        };
        this.enrichers = enrichers;
    }
    /**
     * Aggregates atomic events into a single semantic event
     * @param atomicEvents - Array of atomic events from StateDiffAnalyzer
     * @param tx - Raw transaction data
     * @returns Semantic event representing the business operation
     */
    EventAggregator.prototype.aggregate = function (atomicEvents, tx) {
        return __awaiter(this, void 0, void 0, function () {
            var event_1, _i, _a, enricher, filteredEvents, event_2, _b, _c, enricher, swapCandidate, event_3, _d, _e, enricher, transferCandidate, event_4, _f, _g, enricher, event_5, _h, _j, enricher, event, _k, _l, enricher;
            return __generator(this, function (_m) {
                switch (_m.label) {
                    case 0:
                        if (!tx.meta.err) return [3 /*break*/, 5];
                        event_1 = this.createFailedTransactionEvent(atomicEvents, tx);
                        _i = 0, _a = this.enrichers;
                        _m.label = 1;
                    case 1:
                        if (!(_i < _a.length)) return [3 /*break*/, 4];
                        enricher = _a[_i];
                        return [4 /*yield*/, enricher.enrich(event_1, tx)];
                    case 2:
                        event_1 = _m.sent();
                        _m.label = 3;
                    case 3:
                        _i++;
                        return [3 /*break*/, 1];
                    case 4: return [2 /*return*/, event_1];
                    case 5:
                        filteredEvents = this.config.includeFeeEvents
                            ? atomicEvents
                            : this.filterFeeEvents(atomicEvents, tx);
                        if (!(filteredEvents.length === 0)) return [3 /*break*/, 10];
                        event_2 = this.createUnknownTransactionEvent(atomicEvents, tx, 'No meaningful events after filtering');
                        _b = 0, _c = this.enrichers;
                        _m.label = 6;
                    case 6:
                        if (!(_b < _c.length)) return [3 /*break*/, 9];
                        enricher = _c[_b];
                        return [4 /*yield*/, enricher.enrich(event_2, tx)];
                    case 7:
                        event_2 = _m.sent();
                        _m.label = 8;
                    case 8:
                        _b++;
                        return [3 /*break*/, 6];
                    case 9: return [2 /*return*/, event_2];
                    case 10:
                        swapCandidate = this.identifySwapPattern(filteredEvents);
                        if (!swapCandidate) return [3 /*break*/, 15];
                        event_3 = this.createSwapEvent(swapCandidate, tx);
                        _d = 0, _e = this.enrichers;
                        _m.label = 11;
                    case 11:
                        if (!(_d < _e.length)) return [3 /*break*/, 14];
                        enricher = _e[_d];
                        return [4 /*yield*/, enricher.enrich(event_3, tx)];
                    case 12:
                        event_3 = _m.sent();
                        _m.label = 13;
                    case 13:
                        _d++;
                        return [3 /*break*/, 11];
                    case 14: return [2 /*return*/, event_3];
                    case 15:
                        transferCandidate = this.identifyTransferPattern(filteredEvents);
                        if (!transferCandidate) return [3 /*break*/, 20];
                        event_4 = this.createTransferEvent(transferCandidate, tx);
                        _f = 0, _g = this.enrichers;
                        _m.label = 16;
                    case 16:
                        if (!(_f < _g.length)) return [3 /*break*/, 19];
                        enricher = _g[_f];
                        return [4 /*yield*/, enricher.enrich(event_4, tx)];
                    case 17:
                        event_4 = _m.sent();
                        _m.label = 18;
                    case 18:
                        _f++;
                        return [3 /*break*/, 16];
                    case 19: return [2 /*return*/, event_4];
                    case 20:
                        if (!(filteredEvents.length > 1 && this.config.generateComplexEvents)) return [3 /*break*/, 25];
                        event_5 = this.createComplexTransactionEvent(filteredEvents, tx);
                        _h = 0, _j = this.enrichers;
                        _m.label = 21;
                    case 21:
                        if (!(_h < _j.length)) return [3 /*break*/, 24];
                        enricher = _j[_h];
                        return [4 /*yield*/, enricher.enrich(event_5, tx)];
                    case 22:
                        event_5 = _m.sent();
                        _m.label = 23;
                    case 23:
                        _h++;
                        return [3 /*break*/, 21];
                    case 24: return [2 /*return*/, event_5];
                    case 25:
                        event = this.createUnknownTransactionEvent(atomicEvents, tx, 'Could not identify transaction pattern');
                        _k = 0, _l = this.enrichers;
                        _m.label = 26;
                    case 26:
                        if (!(_k < _l.length)) return [3 /*break*/, 29];
                        enricher = _l[_k];
                        return [4 /*yield*/, enricher.enrich(event, tx)];
                    case 27:
                        event = _m.sent();
                        _m.label = 28;
                    case 28:
                        _k++;
                        return [3 /*break*/, 26];
                    case 29: return [2 /*return*/, event];
                }
            });
        });
    };
    /**
     * Identifies swap patterns in atomic events
     */
    EventAggregator.prototype.identifySwapPattern = function (events) {
        var movements = this.extractTokenMovements(events);
        // Group movements by account
        var accountMovements = new Map();
        for (var _i = 0, movements_1 = movements; _i < movements_1.length; _i++) {
            var movement = movements_1[_i];
            var existing = accountMovements.get(movement.account) || [];
            existing.push(movement);
            accountMovements.set(movement.account, existing);
        }
        // Look for accounts with both IN and OUT movements of different tokens
        for (var _a = 0, accountMovements_1 = accountMovements; _a < accountMovements_1.length; _a++) {
            var _b = accountMovements_1[_a], account = _b[0], accountMoves = _b[1];
            var inMovements = accountMoves.filter(function (m) { return m.direction === 'IN'; });
            var outMovements = accountMoves.filter(function (m) { return m.direction === 'OUT'; });
            // Check for different token swaps
            for (var _c = 0, inMovements_1 = inMovements; _c < inMovements_1.length; _c++) {
                var inMove = inMovements_1[_c];
                for (var _d = 0, outMovements_1 = outMovements; _d < outMovements_1.length; _d++) {
                    var outMove = outMovements_1[_d];
                    if (inMove.mint !== outMove.mint) {
                        // Found a potential swap
                        var confidence = this.calculateSwapConfidence(inMove, outMove, movements);
                        if (confidence > 0.7) {
                            return {
                                swapper: account,
                                tokenIn: {
                                    mint: outMove.mint,
                                    amount: outMove.amount,
                                    decimals: outMove.decimals,
                                },
                                tokenOut: {
                                    mint: inMove.mint,
                                    amount: inMove.amount,
                                    decimals: inMove.decimals,
                                },
                                atomicEvents: [inMove.atomicEvent, outMove.atomicEvent],
                                confidence: confidence,
                            };
                        }
                    }
                }
            }
        }
        return null;
    };
    /**
     * Identifies transfer patterns in atomic events
     */
    EventAggregator.prototype.identifyTransferPattern = function (events) {
        var movements = this.extractTokenMovements(events);
        // Group movements by token mint
        var tokenMovements = new Map();
        for (var _i = 0, movements_2 = movements; _i < movements_2.length; _i++) {
            var movement = movements_2[_i];
            var existing = tokenMovements.get(movement.mint) || [];
            existing.push(movement);
            tokenMovements.set(movement.mint, existing);
        }
        // Look for balanced token movements (same token, different accounts)
        for (var _a = 0, tokenMovements_1 = tokenMovements; _a < tokenMovements_1.length; _a++) {
            var _b = tokenMovements_1[_a], mint = _b[0], tokenMoves = _b[1];
            var inMovements = tokenMoves.filter(function (m) { return m.direction === 'IN'; });
            var outMovements = tokenMoves.filter(function (m) { return m.direction === 'OUT'; });
            // Check for direct transfers
            for (var _c = 0, inMovements_2 = inMovements; _c < inMovements_2.length; _c++) {
                var inMove = inMovements_2[_c];
                for (var _d = 0, outMovements_2 = outMovements; _d < outMovements_2.length; _d++) {
                    var outMove = outMovements_2[_d];
                    if (inMove.account !== outMove.account) {
                        // Check if amounts match (within tolerance)
                        if (this.amountsMatch(inMove.amount, outMove.amount)) {
                            var confidence = this.calculateTransferConfidence(inMove, outMove, movements);
                            if (confidence > 0.8) {
                                return {
                                    sender: outMove.account,
                                    receiver: inMove.account,
                                    token: {
                                        mint: mint,
                                        amount: inMove.amount,
                                        decimals: inMove.decimals,
                                    },
                                    atomicEvents: [inMove.atomicEvent, outMove.atomicEvent],
                                    confidence: confidence,
                                };
                            }
                        }
                    }
                }
            }
        }
        return null;
    };
    /**
     * Extracts token movements from atomic events
     */
    EventAggregator.prototype.extractTokenMovements = function (events) {
        var movements = [];
        for (var _i = 0, events_1 = events; _i < events_1.length; _i++) {
            var event_6 = events_1[_i];
            if (event_6.type === 'DEBIT_SPL' || event_6.type === 'CREDIT_SPL') {
                var splEvent = event_6;
                movements.push({
                    account: splEvent.account,
                    mint: splEvent.mint,
                    amount: splEvent.amount,
                    decimals: splEvent.decimals,
                    direction: event_6.type === 'DEBIT_SPL' ? 'OUT' : 'IN',
                    atomicEvent: event_6,
                });
            }
            else if (event_6.type === 'DEBIT_SOL' || event_6.type === 'CREDIT_SOL') {
                var solEvent = event_6;
                movements.push({
                    account: solEvent.account,
                    mint: 'SOL',
                    amount: solEvent.amount,
                    decimals: 9, // SOL has 9 decimals
                    direction: event_6.type === 'DEBIT_SOL' ? 'OUT' : 'IN',
                    atomicEvent: event_6,
                });
            }
        }
        return movements;
    };
    /**
     * Calculates confidence score for a swap candidate
     */
    EventAggregator.prototype.calculateSwapConfidence = function (inMove, outMove, allMovements) {
        var confidence = 0.5; // Base confidence
        // Boost confidence if amounts are reasonable
        var inAmount = BigInt(inMove.amount);
        var outAmount = BigInt(outMove.amount);
        if (inAmount > 0n && outAmount > 0n) {
            confidence += 0.2;
        }
        // Boost confidence if this is the only movement for this account
        var accountMovements = allMovements.filter(function (m) { return m.account === inMove.account; });
        if (accountMovements.length === 2) {
            confidence += 0.3;
        }
        return Math.min(confidence, 1.0);
    };
    /**
     * Calculates confidence score for a transfer candidate
     */
    EventAggregator.prototype.calculateTransferConfidence = function (inMove, outMove, allMovements) {
        var confidence = 0.6; // Base confidence
        // Boost confidence if amounts match exactly or within tolerance
        if (inMove.amount === outMove.amount) {
            confidence += 0.3;
        }
        else if (this.amountsMatch(inMove.amount, outMove.amount)) {
            confidence += 0.25; // Slightly less confidence for tolerance matches
        }
        // Boost confidence if this is a simple 1:1 transfer
        var sameTokenMovements = allMovements.filter(function (m) { return m.mint === inMove.mint; });
        if (sameTokenMovements.length === 2) {
            confidence += 0.2;
        }
        return Math.min(confidence, 1.0);
    };
    /**
     * Checks if two amounts match within tolerance
     */
    EventAggregator.prototype.amountsMatch = function (amount1, amount2) {
        var a1 = BigInt(amount1);
        var a2 = BigInt(amount2);
        if (a1 === a2)
            return true;
        // Calculate tolerance
        var larger = a1 > a2 ? a1 : a2;
        var smaller = a1 < a2 ? a1 : a2;
        var difference = larger - smaller;
        // Convert tolerance to BigInt calculation
        // tolerance = larger * (amountTolerance / 100)
        var toleranceAmount = (larger * BigInt(Math.floor(this.config.amountTolerance * 100000))) /
            100000n;
        return difference <= toleranceAmount;
    };
    /**
     * Filters out fee-related events
     */
    EventAggregator.prototype.filterFeeEvents = function (events, tx) {
        // Simple heuristic: filter out SOL debits that match the transaction fee
        var fee = BigInt(tx.meta.fee);
        return events.filter(function (event) {
            if (event.type === 'DEBIT_SOL') {
                var amount = BigInt(event.amount);
                return amount !== fee;
            }
            return true;
        });
    };
    /**
     * Creates a failed transaction event
     */
    EventAggregator.prototype.createFailedTransactionEvent = function (atomicEvents, tx) {
        // Find fee payer (usually the first account that paid fees)
        var feeEvent = atomicEvents.find(function (event) {
            return event.type === 'DEBIT_SOL' && event.amount === tx.meta.fee.toString();
        });
        return {
            type: 'TRANSACTION_FAILED',
            signature: tx.signature,
            timestamp: tx.blockTime,
            atomicEvents: atomicEvents,
            error: tx.meta.err,
            feePaid: tx.meta.fee.toString(),
            feePayer: (feeEvent === null || feeEvent === void 0 ? void 0 : feeEvent.account) || tx.transaction.message.accountKeys[0] || 'unknown',
        };
    };
    /**
     * Creates a swap event
     */
    EventAggregator.prototype.createSwapEvent = function (candidate, tx) {
        // Calculate swap rate
        var tokenInAmount = BigInt(candidate.tokenIn.amount);
        var tokenOutAmount = BigInt(candidate.tokenOut.amount);
        var rate;
        if (tokenInAmount > 0n) {
            // Simple rate calculation (tokenOut / tokenIn)
            rate = (Number(tokenOutAmount) / Number(tokenInAmount)).toString();
        }
        var swapEvent = {
            type: 'SWAP',
            signature: tx.signature,
            timestamp: tx.blockTime,
            atomicEvents: candidate.atomicEvents,
            swapper: candidate.swapper,
            tokenIn: candidate.tokenIn,
            tokenOut: candidate.tokenOut,
            metadata: {
                aggregator: { confidence: candidate.confidence },
            },
        };
        if (rate !== undefined) {
            swapEvent.rate = rate;
        }
        return swapEvent;
    };
    /**
     * Creates a transfer event
     */
    EventAggregator.prototype.createTransferEvent = function (candidate, tx) {
        return {
            type: 'TRANSFER',
            signature: tx.signature,
            timestamp: tx.blockTime,
            atomicEvents: candidate.atomicEvents,
            sender: candidate.sender,
            receiver: candidate.receiver,
            token: candidate.token,
            transferType: 'DIRECT', // Default to direct transfer
            metadata: {
                aggregator: { confidence: candidate.confidence },
            },
        };
    };
    /**
     * Creates a complex transaction event
     */
    EventAggregator.prototype.createComplexTransactionEvent = function (atomicEvents, tx) {
        // For now, create a simple complex transaction
        // In the future, this could recursively identify sub-patterns
        return {
            type: 'COMPLEX_TRANSACTION',
            signature: tx.signature,
            timestamp: tx.blockTime,
            atomicEvents: atomicEvents,
            subEvents: [], // TODO: Implement sub-event detection
            metadata: {
                aggregator: { atomicEventCount: atomicEvents.length },
            },
        };
    };
    /**
     * Creates an unknown transaction event
     */
    EventAggregator.prototype.createUnknownTransactionEvent = function (atomicEvents, tx, reason) {
        return {
            type: 'UNKNOWN_TRANSACTION',
            signature: tx.signature,
            timestamp: tx.blockTime,
            atomicEvents: atomicEvents,
            reason: reason,
            unmatchedEventsCount: atomicEvents.length,
            metadata: {
                system: {
                    fee: tx.meta.fee.toString(),
                    logMessages: tx.meta.logMessages,
                },
            },
        };
    };
    return EventAggregator;
}());
exports.EventAggregator = EventAggregator;
