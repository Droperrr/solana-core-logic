/**
 * Types for Solana transaction state-diff analysis
 */

/**
 * Represents a raw Solana transaction as returned by getTransaction RPC method
 */
export interface RawTransaction {
  /** Transaction signature */
  signature: string;
  /** Transaction metadata */
  meta: {
    /** Error information if transaction failed */
    err: unknown | null;
    /** Transaction fee in lamports */
    fee: number;
    /** Account balances before transaction execution */
    preBalances: number[];
    /** Account balances after transaction execution */
    postBalances: number[];
    /** Token balances before transaction execution */
    preTokenBalances: TokenBalance[];
    /** Token balances after transaction execution */
    postTokenBalances: TokenBalance[];
    /** Log messages from transaction execution */
    logMessages: string[];
    /** Inner instructions */
    innerInstructions: unknown[];
    /** Compute units consumed */
    computeUnitsConsumed?: number;
  };
  /** Transaction data */
  transaction: {
    /** Transaction message */
    message: {
      /** Array of account keys */
      accountKeys: string[];
      /** Recent blockhash */
      recentBlockhash: string;
      /** Instructions */
      instructions: unknown[];
    };
    /** Transaction signatures */
    signatures: string[];
  };
  /** Block time */
  blockTime: number | null;
  /** Slot number */
  slot: number;
}

/**
 * Represents a token balance at a specific point in time
 */
export interface TokenBalance {
  /** Index of the account in the accountKeys array */
  accountIndex: number;
  /** Token mint address */
  mint: string;
  /** Token account owner */
  owner: string;
  /** Token amount information */
  uiTokenAmount: {
    /** Raw token amount as string */
    amount: string;
    /** Token decimals */
    decimals: number;
    /** Human-readable amount */
    uiAmount: number | null;
    /** Human-readable amount as string */
    uiAmountString: string;
  };
  /** Program ID that owns this token account */
  programId: string;
}

/**
 * Base interface for all atomic events
 */
export interface BaseAtomicEvent {
  /** Event type */
  type: AtomicEventType;
  /** Account address that experienced the balance change */
  account: string;
  /** Amount of the balance change in raw units (lamports for SOL, token units for SPL) */
  amount: string;
  /** Transaction signature this event belongs to */
  signature: string;
  /** Timestamp of the transaction */
  timestamp: number | null;
}

/**
 * SOL transfer event (debit or credit)
 */
export interface SolTransferEvent extends BaseAtomicEvent {
  type: 'DEBIT_SOL' | 'CREDIT_SOL';
  /** Always 'SOL' for SOL transfers */
  currency: 'SOL';
}

/**
 * SPL token transfer event (debit or credit)
 */
export interface SplTokenTransferEvent extends BaseAtomicEvent {
  type: 'DEBIT_SPL' | 'CREDIT_SPL';
  /** Token mint address */
  mint: string;
  /** Token account address */
  tokenAccount: string;
  /** Token owner address */
  owner: string;
  /** Token decimals */
  decimals: number;
  /** Program ID that owns this token account */
  programId: string;
}

/**
 * Union type for all atomic events
 */
export type AtomicEvent = SolTransferEvent | SplTokenTransferEvent;

/**
 * Enum for atomic event types
 */
export type AtomicEventType =
  | 'DEBIT_SOL'
  | 'CREDIT_SOL'
  | 'DEBIT_SPL'
  | 'CREDIT_SPL';

/**
 * Configuration options for StateDiffAnalyzer
 */
export interface StateDiffAnalyzerConfig {
  /** Minimum balance change to consider (in lamports for SOL, token units for SPL) */
  minBalanceChange?: string;
  /** Whether to include fee-related balance changes */
  includeFeeChanges?: boolean;
  /** Whether to trace temporary account transfers */
  traceTemporaryAccounts?: boolean;
}

// ===== SEMANTIC EVENT TYPES =====

/**
 * Base interface for all semantic events
 */
export interface BaseSemanticEvent {
  /** Semantic event type */
  type: SemanticEventType;
  /** Transaction signature this event belongs to */
  signature: string;
  /** Timestamp of the transaction */
  timestamp: number | null;
  /** Array of atomic events that were aggregated to create this semantic event */
  atomicEvents: AtomicEvent[];
  /**
   * Additional metadata, enriched by plugins.
   * Each plugin writes to its own namespaced key (e.g., metadata["jupiter_v6"])
   */
  metadata?: Record<string, Record<string, any>>;
}

/**
 * Token information for semantic events
 */
export interface TokenInfo {
  /** Token mint address */
  mint: string;
  /** Amount in raw token units */
  amount: string;
  /** Token decimals */
  decimals: number;
  /** Token symbol (if known) */
  symbol?: string;
}

/**
 * Swap event - represents a token exchange
 */
export interface SwapEvent extends BaseSemanticEvent {
  type: 'SWAP';
  /** Account that initiated the swap */
  swapper: string;
  /** Token being sold/sent */
  tokenIn: TokenInfo;
  /** Token being bought/received */
  tokenOut: TokenInfo;
  /** Swap rate (tokenOut amount / tokenIn amount) */
  rate?: string;
  /** Platform/DEX where swap occurred */
  platform?: string;
}

/**
 * Transfer event - represents a token transfer
 */
export interface TransferEvent extends BaseSemanticEvent {
  type: 'TRANSFER';
  /** Account sending the token */
  sender: string;
  /** Account receiving the token */
  receiver: string;
  /** Token being transferred */
  token: TokenInfo;
  /** Transfer type (direct, via program, etc.) */
  transferType?: 'DIRECT' | 'PROGRAM' | 'MULTISIG';
}

/**
 * Failed transaction event - represents a transaction that failed
 */
export interface FailedTransactionEvent extends BaseSemanticEvent {
  type: 'TRANSACTION_FAILED';
  /** Error information */
  error: unknown;
  /** Fee paid despite failure */
  feePaid: string;
  /** Account that paid the fee */
  feePayer: string;
}

/**
 * Complex transaction event - represents a transaction with multiple operations
 */
export interface ComplexTransactionEvent extends BaseSemanticEvent {
  type: 'COMPLEX_TRANSACTION';
  /** Array of sub-events that make up this complex transaction */
  subEvents: SemanticEvent[];
  /** Primary operation type */
  primaryOperation?: SemanticEventType;
}

/**
 * Unknown transaction event - represents a transaction that couldn't be categorized
 */
export interface UnknownTransactionEvent extends BaseSemanticEvent {
  type: 'UNKNOWN_TRANSACTION';
  /** Reason why the transaction couldn't be categorized */
  reason: string;
  /** Number of atomic events that couldn't be matched */
  unmatchedEventsCount: number;
}

/**
 * Union type for all semantic events
 */
export type SemanticEvent =
  | SwapEvent
  | TransferEvent
  | FailedTransactionEvent
  | ComplexTransactionEvent
  | UnknownTransactionEvent;

/**
 * Enum for semantic event types
 */
export type SemanticEventType =
  | 'SWAP'
  | 'TRANSFER'
  | 'TRANSACTION_FAILED'
  | 'COMPLEX_TRANSACTION'
  | 'UNKNOWN_TRANSACTION';

/**
 * Configuration options for EventAggregator
 */
export interface EventAggregatorConfig {
  /** Tolerance for amount matching (as a percentage, e.g., 0.01 for 1%) */
  amountTolerance?: number;
  /** Whether to include fee events in aggregation */
  includeFeeEvents?: boolean;
  /** Maximum number of hops to consider for multi-step swaps */
  maxSwapHops?: number;
  /** Whether to generate complex transaction events for multi-operation transactions */
  generateComplexEvents?: boolean;
}
 