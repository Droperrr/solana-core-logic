"use strict";
/**
 * Solana State-Diff Transaction Decoder
 *
 * A TypeScript library for analyzing Solana transaction state changes
 * using the "State-First" philosophy - focusing on balance differences
 * rather than instruction parsing.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EventAggregator = exports.StateDiffAnalyzer = void 0;
var StateDiffAnalyzer_1 = require("./StateDiffAnalyzer");
Object.defineProperty(exports, "StateDiffAnalyzer", { enumerable: true, get: function () { return StateDiffAnalyzer_1.StateDiffAnalyzer; } });
var EventAggregator_1 = require("./EventAggregator");
Object.defineProperty(exports, "EventAggregator", { enumerable: true, get: function () { return EventAggregator_1.EventAggregator; } });
__exportStar(require("./types"), exports);
