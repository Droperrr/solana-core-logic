# Solana State-Diff Transaction Decoder

A TypeScript library for analyzing Solana transaction state changes using the "State-First" philosophy - focusing on balance differences rather than instruction parsing.

## Philosophy

This library implements the **State-First** approach to transaction analysis, which prioritizes analyzing the actual state changes (balance differences) over parsing complex instruction data. This approach provides:

- **Deterministic Results**: State changes are absolute and unambiguous
- **Reliability**: No dependency on instruction parsing which can be complex and error-prone
- **Completeness**: Captures all balance changes regardless of the underlying mechanism
- **Simplicity**: Clean, focused API that's easy to understand and use

## Features

- ✅ **SOL Balance Analysis**: Detects SOL transfers between accounts
- ✅ **SPL Token Analysis**: Handles SPL token transfers, including complex scenarios
- ✅ **Fee Filtering**: Configurable filtering of fee-related balance changes
- ✅ **Large Number Support**: Uses BigInt for handling large token amounts
- ✅ **ATA Support**: Handles Associated Token Account creation and closure
- ✅ **Semantic Event Aggregation**: Aggregates atomic events into business-meaningful operations
- ✅ **Pattern Recognition**: Automatically detects SWAPs, TRANSFERs, and complex transactions
- ✅ **Comprehensive Testing**: 97%+ code coverage with extensive test cases
- ✅ **TypeScript**: Full type safety and IntelliSense support

## Installation

```bash
npm install @your-org/solana-state-diff-decoder
```

## Quick Start

```typescript
import { StateDiffAnalyzer, EventAggregator } from '@your-org/solana-state-diff-decoder';

// Initialize components
const analyzer = new StateDiffAnalyzer();
const aggregator = new EventAggregator();

// Analyze a transaction
const rawTransaction = await connection.getTransaction(signature, {
  commitment: 'confirmed',
  maxSupportedTransactionVersion: 0,
});

// Get atomic events
const atomicEvents = analyzer.analyze(rawTransaction);

// Aggregate into semantic events
const semanticEvent = aggregator.aggregate(atomicEvents, rawTransaction);

// Process semantic event
console.log(`Transaction type: ${semanticEvent.type}`);
if (semanticEvent.type === 'SWAP') {
  const swap = semanticEvent as SwapEvent;
  console.log(`Swap: ${swap.tokenIn.amount} ${swap.tokenIn.mint} → ${swap.tokenOut.amount} ${swap.tokenOut.mint}`);
}
```

## API Reference

### StateDiffAnalyzer

The main class for analyzing transaction state changes.

#### Constructor

```typescript
constructor(config?: StateDiffAnalyzerConfig)
```

#### Configuration Options

```typescript
interface StateDiffAnalyzerConfig {
  /** Minimum balance change to consider (in lamports for SOL, token units for SPL) */
  minBalanceChange?: string;
  /** Whether to include fee-related balance changes */
  includeFeeChanges?: boolean;
  /** Whether to trace temporary account transfers */
  traceTemporaryAccounts?: boolean;
}
```

#### Methods

##### `analyze(rawTx: RawTransaction): AtomicEvent[]`

Analyzes a raw transaction and returns atomic events based on state differences.

**Parameters:**
- `rawTx`: Raw transaction data from `getTransaction` RPC method

**Returns:**
- Array of atomic events representing balance changes

### EventAggregator

The class for aggregating atomic events into semantic business events.

#### Constructor

```typescript
constructor(config?: EventAggregatorConfig)
```

#### Configuration Options

```typescript
interface EventAggregatorConfig {
  /** Tolerance for amount matching (as a percentage, e.g., 0.01 for 1%) */
  amountTolerance?: number;
  /** Whether to include fee events in aggregation */
  includeFeeEvents?: boolean;
  /** Maximum number of hops to consider for multi-step swaps */
  maxSwapHops?: number;
  /** Whether to generate complex transaction events for multi-operation transactions */
  generateComplexEvents?: boolean;
}
```

#### Methods

##### `aggregate(atomicEvents: AtomicEvent[], tx: RawTransaction): SemanticEvent`

Aggregates atomic events into a single semantic event.

**Parameters:**
- `atomicEvents`: Array of atomic events from StateDiffAnalyzer
- `tx`: Raw transaction data from `getTransaction` RPC method

**Returns:**
- Semantic event representing the business operation

### Event Types

#### Atomic Events

#### SolTransferEvent

```typescript
interface SolTransferEvent {
  type: 'DEBIT_SOL' | 'CREDIT_SOL';
  account: string;           // Account address
  amount: string;            // Amount in lamports
  signature: string;         // Transaction signature
  timestamp: number | null;  // Block time
  currency: 'SOL';
}
```

#### SplTokenTransferEvent

```typescript
interface SplTokenTransferEvent {
  type: 'DEBIT_SPL' | 'CREDIT_SPL';
  account: string;           // Owner account address
  amount: string;            // Amount in token units
  signature: string;         // Transaction signature
  timestamp: number | null;  // Block time
  mint: string;              // Token mint address
  tokenAccount: string;      // Token account address
  owner: string;             // Token owner address
  decimals: number;          // Token decimals
  programId: string;         // Token program ID
}
```

#### Semantic Events

#### SwapEvent

```typescript
interface SwapEvent {
  type: 'SWAP';
  signature: string;         // Transaction signature
  timestamp: number | null;  // Block time
  atomicEvents: AtomicEvent[]; // Source atomic events
  swapper: string;           // Account that initiated the swap
  tokenIn: TokenInfo;        // Token being sold/sent
  tokenOut: TokenInfo;       // Token being bought/received
  rate?: string;             // Swap rate (tokenOut/tokenIn)
  platform?: string;        // DEX platform (if detected)
}
```

#### TransferEvent

```typescript
interface TransferEvent {
  type: 'TRANSFER';
  signature: string;         // Transaction signature
  timestamp: number | null;  // Block time
  atomicEvents: AtomicEvent[]; // Source atomic events
  sender: string;            // Account sending the token
  receiver: string;          // Account receiving the token
  token: TokenInfo;          // Token being transferred
  transferType?: 'DIRECT' | 'PROGRAM' | 'MULTISIG';
}
```

#### FailedTransactionEvent

```typescript
interface FailedTransactionEvent {
  type: 'TRANSACTION_FAILED';
  signature: string;         // Transaction signature
  timestamp: number | null;  // Block time
  atomicEvents: AtomicEvent[]; // Source atomic events
  error: unknown;            // Error information
  feePaid: string;           // Fee paid despite failure
  feePayer: string;          // Account that paid the fee
}
```

## Examples

### Basic SOL Transfer

```typescript
const analyzer = new StateDiffAnalyzer();
const events = analyzer.analyze(rawTransaction);

// Find SOL transfers
const solTransfers = events.filter(e => e.type.includes('SOL'));
console.log(`Found ${solTransfers.length} SOL transfers`);
```

### SPL Token Analysis

```typescript
const analyzer = new StateDiffAnalyzer();
const events = analyzer.analyze(rawTransaction);

// Find SPL token transfers
const splTransfers = events.filter(e => e.type.includes('SPL'));
splTransfers.forEach(event => {
  if (event.type.includes('SPL')) {
    const splEvent = event as SplTokenTransferEvent;
    console.log(`${splEvent.type}: ${splEvent.amount} ${splEvent.mint}`);
  }
});
```

### Semantic Event Analysis

```typescript
const analyzer = new StateDiffAnalyzer();
const aggregator = new EventAggregator();

const atomicEvents = analyzer.analyze(rawTransaction);
const semanticEvent = aggregator.aggregate(atomicEvents, rawTransaction);

switch (semanticEvent.type) {
  case 'SWAP':
    const swap = semanticEvent as SwapEvent;
    console.log(`User ${swap.swapper} swapped ${swap.tokenIn.amount} ${swap.tokenIn.mint} for ${swap.tokenOut.amount} ${swap.tokenOut.mint}`);
    break;
  
  case 'TRANSFER':
    const transfer = semanticEvent as TransferEvent;
    console.log(`Transfer of ${transfer.token.amount} ${transfer.token.mint} from ${transfer.sender} to ${transfer.receiver}`);
    break;
  
  case 'TRANSACTION_FAILED':
    const failed = semanticEvent as FailedTransactionEvent;
    console.log(`Transaction failed: ${JSON.stringify(failed.error)}`);
    break;
}
```

### Custom Configuration

```typescript
const analyzer = new StateDiffAnalyzer({
  minBalanceChange: '1000000', // 1M lamports minimum
  includeFeeChanges: true,     // Include fee-related changes
  traceTemporaryAccounts: true // Trace through temporary accounts
});

const aggregator = new EventAggregator({
  amountTolerance: 0.005,      // 0.5% tolerance for amount matching
  includeFeeEvents: false,     // Exclude fee events from aggregation
  generateComplexEvents: true, // Generate complex transaction events
});
```

## Architecture

The library is built around the core principle of state-first analysis:

1. **Balance Comparison**: Compare `preBalances` vs `postBalances` for SOL
2. **Token Balance Comparison**: Compare `preTokenBalances` vs `postTokenBalances` for SPL tokens
3. **Change Detection**: Identify accounts with balance changes
4. **Event Generation**: Create atomic events for each meaningful change
5. **Filtering**: Apply configured filters (fees, minimums, etc.)

## Testing

Run the test suite:

```bash
npm test
```

Run with coverage:

```bash
npm run test:coverage
```

## Development

### Setup

```bash
npm install
```

### Build

```bash
npm run build
```

### Format Code

```bash
npm run format
```

### Watch Mode

```bash
npm run dev
```

## Supported Scenarios

- ✅ Simple SOL transfers
- ✅ Simple SPL token transfers
- ✅ Associated Token Account (ATA) creation
- ✅ Token account closure
- ✅ Multi-token transactions
- ✅ Failed transactions (fee-only changes)
- ✅ Large token amounts (BigInt support)
- ✅ Fee filtering
- 🔄 Complex temporary account flows (basic support)

## Roadmap

- [ ] Advanced temporary account tracing
- [ ] Support for Solana 2.0 transaction formats
- [ ] Performance optimizations for batch processing
- [ ] Plugin system for custom event types
- [ ] Integration with popular Solana frameworks

## Contributing

Contributions are welcome! Please read our contributing guidelines and ensure all tests pass before submitting a PR.

## License

MIT License - see LICENSE file for details. 