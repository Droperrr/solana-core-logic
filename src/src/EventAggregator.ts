/**
 * EventAggregator - Aggregates atomic events into semantic business events
 *
 * This class takes atomic events from StateDiffAnalyzer and interprets them
 * into meaningful business operations like SWAP, TRANSFER, etc.
 */

import {
  AtomicEvent,
  RawTransaction,
  SemanticEvent,
  SwapEvent,
  TransferEvent,
  FailedTransactionEvent,
  ComplexTransactionEvent,
  UnknownTransactionEvent,
  EventAggregatorConfig,
  SplTokenTransferEvent,
  SolTransferEvent,
  TokenInfo,
} from './types';
import { EnricherPlugin } from './plugins/EnricherPlugin';

/**
 * Internal type for tracking token movements
 */
interface TokenMovement {
  account: string;
  mint: string;
  amount: string;
  decimals: number;
  direction: 'IN' | 'OUT';
  atomicEvent: AtomicEvent;
}

/**
 * Internal type for potential swap matches
 */
interface SwapCandidate {
  swapper: string;
  tokenIn: TokenInfo;
  tokenOut: TokenInfo;
  atomicEvents: AtomicEvent[];
  confidence: number;
}

/**
 * Internal type for potential transfer matches
 */
interface TransferCandidate {
  sender: string;
  receiver: string;
  token: TokenInfo;
  atomicEvents: AtomicEvent[];
  confidence: number;
}

export class EventAggregator {
  private readonly config: Required<EventAggregatorConfig>;
  private readonly enrichers: EnricherPlugin[];

  constructor(config: EventAggregatorConfig = {}, enrichers: EnricherPlugin[] = []) {
    this.config = {
      amountTolerance: config.amountTolerance ?? 0.001, // 0.1% tolerance
      includeFeeEvents: config.includeFeeEvents ?? false,
      maxSwapHops: config.maxSwapHops ?? 3,
      generateComplexEvents: config.generateComplexEvents ?? true,
    };
    this.enrichers = enrichers;
  }

  /**
   * Aggregates atomic events into a single semantic event
   * @param atomicEvents - Array of atomic events from StateDiffAnalyzer
   * @param tx - Raw transaction data
   * @returns Semantic event representing the business operation
   */
  public async aggregate(
    atomicEvents: AtomicEvent[],
    tx: RawTransaction
  ): Promise<SemanticEvent> {
    // First, check if transaction failed
    if (tx.meta.err) {
      let event = this.createFailedTransactionEvent(atomicEvents, tx) as SemanticEvent;
      for (const enricher of this.enrichers) {
        event = await enricher.enrich(event, tx);
      }
      return event;
    }

    // Filter out fee events if configured
    const filteredEvents = this.config.includeFeeEvents
      ? atomicEvents
      : this.filterFeeEvents(atomicEvents, tx);

    // If no events after filtering, return unknown transaction
    if (filteredEvents.length === 0) {
      let event = this.createUnknownTransactionEvent(
        atomicEvents,
        tx,
        'No meaningful events after filtering'
      ) as SemanticEvent;
      for (const enricher of this.enrichers) {
        event = await enricher.enrich(event, tx);
      }
      return event;
    }

    // Try to identify the primary pattern
    const swapCandidate = this.identifySwapPattern(filteredEvents);
    if (swapCandidate) {
      let event = this.createSwapEvent(swapCandidate, tx) as SemanticEvent;
      for (const enricher of this.enrichers) {
        event = await enricher.enrich(event, tx);
      }
      return event;
    }

    const transferCandidate = this.identifyTransferPattern(filteredEvents);
    if (transferCandidate) {
      // Check if there are unmatched events after identifying the transfer pattern
      const usedEventSignatures = new Set(transferCandidate.atomicEvents.map(e => `${e.signature}-${e.type}-${e.account}`));
      const unmatchedEvents = filteredEvents.filter(e => 
        !usedEventSignatures.has(`${e.signature}-${e.type}-${e.account}`)
      );
      
      // If there are significant unmatched events, treat as complex transaction
      if (unmatchedEvents.length > 0 && this.config.generateComplexEvents) {
        let event = this.createComplexTransactionEvent(filteredEvents, tx) as SemanticEvent;
        for (const enricher of this.enrichers) {
          event = await enricher.enrich(event, tx);
        }
        return event;
      }
      
      // Otherwise, create a simple transfer event
      let event = this.createTransferEvent(transferCandidate, tx) as SemanticEvent;
      for (const enricher of this.enrichers) {
        event = await enricher.enrich(event, tx);
      }
      return event;
    }

    // If we have multiple unmatched events, create a complex transaction
    if (filteredEvents.length > 1 && this.config.generateComplexEvents) {
      let event = this.createComplexTransactionEvent(filteredEvents, tx) as SemanticEvent;
      for (const enricher of this.enrichers) {
        event = await enricher.enrich(event, tx);
      }
      return event;
    }

    // Fallback to unknown transaction
    let event = this.createUnknownTransactionEvent(
      atomicEvents,
      tx,
      'Could not identify transaction pattern'
    ) as SemanticEvent;
    for (const enricher of this.enrichers) {
      event = await enricher.enrich(event, tx);
    }
    return event;
  }

  /**
   * Identifies swap patterns in atomic events
   */
  private identifySwapPattern(events: AtomicEvent[]): SwapCandidate | null {
    const movements = this.extractTokenMovements(events);

    // Group movements by account
    const accountMovements = new Map<string, TokenMovement[]>();
    for (const movement of movements) {
      const existing = accountMovements.get(movement.account) || [];
      existing.push(movement);
      accountMovements.set(movement.account, existing);
    }

    // Look for accounts with both IN and OUT movements of different tokens
    for (const [account, accountMoves] of accountMovements) {
      const inMovements = accountMoves.filter(m => m.direction === 'IN');
      const outMovements = accountMoves.filter(m => m.direction === 'OUT');

      // Check for different token swaps
      for (const inMove of inMovements) {
        for (const outMove of outMovements) {
          if (inMove.mint !== outMove.mint) {
            // Found a potential swap
            const confidence = this.calculateSwapConfidence(
              inMove,
              outMove,
              movements
            );

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
                confidence,
              };
            }
          }
        }
      }
    }

    return null;
  }

  /**
   * Identifies transfer patterns in atomic events
   */
  private identifyTransferPattern(
    events: AtomicEvent[]
  ): TransferCandidate | null {
    const movements = this.extractTokenMovements(events);

    // Group movements by token mint
    const tokenMovements = new Map<string, TokenMovement[]>();
    for (const movement of movements) {
      const existing = tokenMovements.get(movement.mint) || [];
      existing.push(movement);
      tokenMovements.set(movement.mint, existing);
    }

    // Look for balanced token movements (same token, different accounts)
    for (const [mint, tokenMoves] of tokenMovements) {
      const inMovements = tokenMoves.filter(m => m.direction === 'IN');
      const outMovements = tokenMoves.filter(m => m.direction === 'OUT');

      // Check for direct transfers
      for (const inMove of inMovements) {
        for (const outMove of outMovements) {
          if (inMove.account !== outMove.account) {
            // Check if amounts match (within tolerance)
            if (this.amountsMatch(inMove.amount, outMove.amount)) {
              const confidence = this.calculateTransferConfidence(
                inMove,
                outMove,
                movements
              );

              if (confidence > 0.8) {
                return {
                  sender: outMove.account,
                  receiver: inMove.account,
                  token: {
                    mint: mint,
                    amount: outMove.amount, // Use the outgoing (debit) amount as primary
                    decimals: outMove.decimals,
                  },
                  atomicEvents: [inMove.atomicEvent, outMove.atomicEvent],
                  confidence,
                };
              }
            }
          }
        }
      }
    }

    return null;
  }

  /**
   * Extracts token movements from atomic events
   */
  private extractTokenMovements(events: AtomicEvent[]): TokenMovement[] {
    const movements: TokenMovement[] = [];

    for (const event of events) {
      if (event.type === 'DEBIT_SPL' || event.type === 'CREDIT_SPL') {
        const splEvent = event as SplTokenTransferEvent;
        movements.push({
          account: splEvent.account,
          mint: splEvent.mint,
          amount: splEvent.amount,
          decimals: splEvent.decimals,
          direction: event.type === 'DEBIT_SPL' ? 'OUT' : 'IN',
          atomicEvent: event,
        });
      } else if (event.type === 'DEBIT_SOL' || event.type === 'CREDIT_SOL') {
        const solEvent = event as SolTransferEvent;
        movements.push({
          account: solEvent.account,
          mint: 'SOL',
          amount: solEvent.amount,
          decimals: 9, // SOL has 9 decimals
          direction: event.type === 'DEBIT_SOL' ? 'OUT' : 'IN',
          atomicEvent: event,
        });
      }
    }

    return movements;
  }

  /**
   * Calculates confidence score for a swap candidate
   */
  private calculateSwapConfidence(
    inMove: TokenMovement,
    outMove: TokenMovement,
    allMovements: TokenMovement[]
  ): number {
    let confidence = 0.5; // Base confidence

    // Boost confidence if amounts are reasonable
    const inAmount = BigInt(inMove.amount);
    const outAmount = BigInt(outMove.amount);

    if (inAmount > 0n && outAmount > 0n) {
      confidence += 0.2;
    }

    // Boost confidence if this is the only movement for this account
    const accountMovements = allMovements.filter(
      m => m.account === inMove.account
    );
    if (accountMovements.length === 2) {
      confidence += 0.3;
    }

    return Math.min(confidence, 1.0);
  }

  /**
   * Calculates confidence score for a transfer candidate
   */
  private calculateTransferConfidence(
    inMove: TokenMovement,
    outMove: TokenMovement,
    allMovements: TokenMovement[]
  ): number {
    let confidence = 0.6; // Base confidence

    // Boost confidence if amounts match exactly or within tolerance
    if (inMove.amount === outMove.amount) {
      confidence += 0.3;
    } else if (this.amountsMatch(inMove.amount, outMove.amount)) {
      confidence += 0.25; // Slightly less confidence for tolerance matches
    }

    // Boost confidence if this is a simple 1:1 transfer
    const sameTokenMovements = allMovements.filter(m => m.mint === inMove.mint);
    if (sameTokenMovements.length === 2) {
      confidence += 0.2;
    }

    return Math.min(confidence, 1.0);
  }

  /**
   * Checks if two amounts match within tolerance
   */
  private amountsMatch(amount1: string, amount2: string): boolean {
    const a1 = BigInt(amount1);
    const a2 = BigInt(amount2);

    if (a1 === a2) return true;

    // Calculate tolerance
    const larger = a1 > a2 ? a1 : a2;
    const smaller = a1 < a2 ? a1 : a2;
    const difference = larger - smaller;

    // Convert tolerance to BigInt calculation
    // tolerance = larger * (amountTolerance / 100)
    const toleranceAmount =
      (larger * BigInt(Math.floor(this.config.amountTolerance * 100000))) /
      100000n;

    return difference <= toleranceAmount;
  }

  /**
   * Filters out fee-related events
   */
  private filterFeeEvents(
    events: AtomicEvent[],
    tx: RawTransaction
  ): AtomicEvent[] {
    // Simple heuristic: filter out SOL debits that match the transaction fee
    const fee = BigInt(tx.meta.fee);
    return events.filter(event => {
      if (event.type === 'DEBIT_SOL') {
        const amount = BigInt(event.amount);
        return amount !== fee;
      }
      return true;
    });
  }

  /**
   * Creates a failed transaction event
   */
  private createFailedTransactionEvent(
    atomicEvents: AtomicEvent[],
    tx: RawTransaction
  ): FailedTransactionEvent {
    // Find fee payer (usually the first account that paid fees)
    const feeEvent = atomicEvents.find(
      event =>
        event.type === 'DEBIT_SOL' && event.amount === tx.meta.fee.toString()
    );

    return {
      type: 'TRANSACTION_FAILED',
      signature: tx.signature,
      timestamp: tx.blockTime,
      atomicEvents,
      error: tx.meta.err,
      feePaid: tx.meta.fee.toString(),
      feePayer:
        feeEvent?.account || tx.transaction.message.accountKeys[0] || 'unknown',
    };
  }

  /**
   * Creates a swap event
   */
  private createSwapEvent(
    candidate: SwapCandidate,
    tx: RawTransaction
  ): SwapEvent {
    // Calculate swap rate
    const tokenInAmount = BigInt(candidate.tokenIn.amount);
    const tokenOutAmount = BigInt(candidate.tokenOut.amount);

    let rate: string | undefined;
    if (tokenInAmount > 0n) {
      // Simple rate calculation (tokenOut / tokenIn)
      rate = (Number(tokenOutAmount) / Number(tokenInAmount)).toString();
    }

    const swapEvent: SwapEvent = {
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
  }

  /**
   * Creates a transfer event
   */
  private createTransferEvent(
    candidate: TransferCandidate,
    tx: RawTransaction
  ): TransferEvent {
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
  }

  /**
   * Creates a complex transaction event
   */
  private createComplexTransactionEvent(
    atomicEvents: AtomicEvent[],
    tx: RawTransaction
  ): ComplexTransactionEvent {
    // For now, create a simple complex transaction
    // In the future, this could recursively identify sub-patterns
    return {
      type: 'COMPLEX_TRANSACTION',
      signature: tx.signature,
      timestamp: tx.blockTime,
      atomicEvents,
      subEvents: [], // TODO: Implement sub-event detection
      metadata: {
        aggregator: { atomicEventCount: atomicEvents.length },
      },
    };
  }

  /**
   * Creates an unknown transaction event
   */
  private createUnknownTransactionEvent(
    atomicEvents: AtomicEvent[],
    tx: RawTransaction,
    reason: string
  ): UnknownTransactionEvent {
    return {
      type: 'UNKNOWN_TRANSACTION',
      signature: tx.signature,
      timestamp: tx.blockTime,
      atomicEvents,
      reason,
      unmatchedEventsCount: atomicEvents.length,
      metadata: {
        system: {
          fee: tx.meta.fee.toString(),
          logMessages: tx.meta.logMessages,
        },
      },
    };
  }
}
