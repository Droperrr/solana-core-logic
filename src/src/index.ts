/**
 * Solana State-Diff Transaction Decoder
 *
 * A TypeScript library for analyzing Solana transaction state changes
 * using the "State-First" philosophy - focusing on balance differences
 * rather than instruction parsing.
 */

export { StateDiffAnalyzer } from './StateDiffAnalyzer';
export { EventAggregator } from './EventAggregator';
export * from './types';

// Re-export for convenience
export type {
  RawTransaction,
  TokenBalance,
  AtomicEvent,
  SolTransferEvent,
  SplTokenTransferEvent,
  AtomicEventType,
  StateDiffAnalyzerConfig,
  SemanticEvent,
  SwapEvent,
  TransferEvent,
  FailedTransactionEvent,
  ComplexTransactionEvent,
  UnknownTransactionEvent,
  SemanticEventType,
  EventAggregatorConfig,
  TokenInfo,
} from './types';
