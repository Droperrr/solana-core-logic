"use strict";
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
exports.JupiterEnricher = void 0;
var borsh_1 = require("@coral-xyz/anchor/dist/cjs/coder/borsh");
var jupiter_v6_json_1 = require("./idl/jupiter_v6.json");
var bs58_1 = require("bs58");
var JUPITER_PROGRAM_ID = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4';
var JupiterEnricher = /** @class */ (function () {
    function JupiterEnricher() {
        var idl = jupiter_v6_json_1.default;
        this.coder = new borsh_1.BorshInstructionCoder(idl);
    }
    JupiterEnricher.prototype.enrich = function (event, tx) {
        return __awaiter(this, void 0, void 0, function () {
            var swapEvent, involvedAccounts, _i, _a, ae, instructions, matchedIx, i, ix, programId, dataBuf, decoded, accountKeys, found, _b, involvedAccounts_1, acc, route, slippage_bps, decoded;
            return __generator(this, function (_c) {
                if (event.type !== 'SWAP')
                    return [2 /*return*/, event];
                swapEvent = event;
                try {
                    involvedAccounts = new Set();
                    for (_i = 0, _a = swapEvent.atomicEvents; _i < _a.length; _i++) {
                        ae = _a[_i];
                        if ('tokenAccount' in ae) {
                            involvedAccounts.add(ae.tokenAccount);
                        }
                    }
                    instructions = tx.transaction.message.instructions;
                    matchedIx = null;
                    for (i = 0; i < instructions.length; i++) {
                        ix = instructions[i];
                        programId = tx.transaction.message.accountKeys[ix.programIdIndex];
                        if (programId !== JUPITER_PROGRAM_ID)
                            continue;
                        try {
                            dataBuf = Buffer.from(bs58_1.default.decode(ix.data));
                            decoded = this.coder.decode(dataBuf);
                            if (!decoded)
                                continue;
                            if (decoded.name === 'route' || decoded.name === 'sharedAccountsRoute') {
                                accountKeys = ix.accounts.map(function (idx) { return tx.transaction.message.accountKeys[idx]; });
                                found = false;
                                for (_b = 0, involvedAccounts_1 = involvedAccounts; _b < involvedAccounts_1.length; _b++) {
                                    acc = involvedAccounts_1[_b];
                                    if (accountKeys.includes(acc)) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (found) {
                                    matchedIx = { ix: ix, decoded: decoded, accountKeys: accountKeys };
                                    break;
                                }
                            }
                        }
                        catch (e) {
                            // ignore decode errors
                        }
                    }
                    if (!matchedIx) {
                        return [2 /*return*/, __assign(__assign({}, event), { metadata: __assign(__assign({}, event.metadata), { jupiter_v6: {
                                        error: 'No matching Jupiter V6 instruction found for this SWAP event.'
                                    } }) })];
                    }
                    route = null;
                    slippage_bps = null;
                    try {
                        decoded = matchedIx.decoded;
                        route = decoded.data.routePlan || null;
                        slippage_bps = decoded.data.slippageBps || null;
                    }
                    catch (e) {
                        return [2 /*return*/, __assign(__assign({}, event), { metadata: __assign(__assign({}, event.metadata), { jupiter_v6: {
                                        error: 'Failed to decode Jupiter instruction: ' + e.message
                                    } }) })];
                    }
                    return [2 /*return*/, __assign(__assign({}, event), { metadata: __assign(__assign({}, event.metadata), { jupiter_v6: {
                                    protocol_name: 'Jupiter',
                                    route: route,
                                    slippage_bps: slippage_bps,
                                    error: null
                                } }) })];
                }
                catch (e) {
                    return [2 /*return*/, __assign(__assign({}, event), { metadata: __assign(__assign({}, event.metadata), { jupiter_v6: {
                                    error: 'Unexpected error in JupiterEnricher: ' + e.message
                                } }) })];
                }
                return [2 /*return*/];
            });
        });
    };
    return JupiterEnricher;
}());
exports.JupiterEnricher = JupiterEnricher;
