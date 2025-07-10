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
var JupiterEnricher_1 = require("../plugins/JupiterEnricher");
var fs = require("fs");
var path = require("path");
describe('JupiterEnricher', function () {
    var rawTx;
    var swapEvent;
    beforeAll(function () {
        var _a, _b, _c, _d, _e, _f;
        // Загружаем raw_json транзакции Jupiter V6 (signature: 3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok)
        // TODO: путь к файлу скорректировать под реальное расположение
        var rawPath = path.resolve(__dirname, '../../../../../tests/fixtures/enrichment/jupiter/3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok.json');
        if (!fs.existsSync(rawPath)) {
            throw new Error('Тестовая транзакция Jupiter V6 не найдена. Добавьте raw_json по указанному пути!');
        }
        rawTx = JSON.parse(fs.readFileSync(rawPath, 'utf-8'));
        // Мокаем SwapEvent максимально приближённо к реальному сценарию
        swapEvent = {
            type: 'SWAP',
            signature: rawTx.signature,
            timestamp: rawTx.blockTime,
            atomicEvents: [], // Для теста enrichment не требуется детализация
            swapper: rawTx.transaction.message.accountKeys[0],
            tokenIn: {
                mint: ((_a = rawTx.meta.preTokenBalances[0]) === null || _a === void 0 ? void 0 : _a.mint) || '',
                amount: ((_b = rawTx.meta.preTokenBalances[0]) === null || _b === void 0 ? void 0 : _b.uiTokenAmount.amount) || '0',
                decimals: ((_c = rawTx.meta.preTokenBalances[0]) === null || _c === void 0 ? void 0 : _c.uiTokenAmount.decimals) || 0,
            },
            tokenOut: {
                mint: ((_d = rawTx.meta.postTokenBalances[1]) === null || _d === void 0 ? void 0 : _d.mint) || '',
                amount: ((_e = rawTx.meta.postTokenBalances[1]) === null || _e === void 0 ? void 0 : _e.uiTokenAmount.amount) || '0',
                decimals: ((_f = rawTx.meta.postTokenBalances[1]) === null || _f === void 0 ? void 0 : _f.uiTokenAmount.decimals) || 0,
            },
            metadata: {},
        };
    });
    it('should enrich SWAP event with Jupiter V6 metadata', function () { return __awaiter(void 0, void 0, void 0, function () {
        var enricher, enriched, meta;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    enricher = new JupiterEnricher_1.JupiterEnricher();
                    return [4 /*yield*/, enricher.enrich(swapEvent, rawTx)];
                case 1:
                    enriched = _a.sent();
                    expect(enriched.metadata).toHaveProperty('jupiter_v6');
                    meta = enriched.metadata.jupiter_v6;
                    expect(meta).toHaveProperty('protocol_name', 'Jupiter');
                    expect(meta).toHaveProperty('route');
                    expect(meta).toHaveProperty('slippage_bps');
                    expect(meta.error).toBeNull();
                    return [2 /*return*/];
            }
        });
    }); });
    it('should write error in metadata if no matching instruction', function () { return __awaiter(void 0, void 0, void 0, function () {
        var enricher, fakeEvent, fakeTx, enriched;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    enricher = new JupiterEnricher_1.JupiterEnricher();
                    fakeEvent = __assign(__assign({}, swapEvent), { signature: 'fake', metadata: {} });
                    fakeTx = __assign(__assign({}, rawTx), { transaction: __assign(__assign({}, rawTx.transaction), { message: __assign(__assign({}, rawTx.transaction.message), { instructions: [] }) }) });
                    return [4 /*yield*/, enricher.enrich(fakeEvent, fakeTx)];
                case 1:
                    enriched = _a.sent();
                    expect(enriched.metadata).toHaveProperty('jupiter_v6');
                    expect(enriched.metadata.jupiter_v6.error).toMatch(/No matching Jupiter V6 instruction/);
                    return [2 /*return*/];
            }
        });
    }); });
});
