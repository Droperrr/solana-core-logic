/**
 * StateDiffAnalyzer - Core component for analyzing Solana transaction state changes
 *
 * This class implements the "State-First" philosophy by analyzing only the differences
 * between pre and post transaction states, without parsing instructions.
 */

import {
  RawTransaction,
  AtomicEvent,
  SolTransferEvent,
  SplTokenTransferEvent,
  TokenBalance,
  StateDiffAnalyzerConfig,
} from './types';

/**
 * Represents a balance change for internal processing
 */
interface BalanceChange {
  accountIndex: number;
  account: string;
  preBalance: number;
  postBalance: number;
  change: number;
}

/**
 * Represents a token balance change for internal processing
 */
interface TokenBalanceChange {
  accountIndex: number;
  tokenAccount: string;
  owner: string;
  mint: string;
  decimals: number;
  programId: string;
  preAmount: string;
  postAmount: string;
  change: string;
}

export class StateDiffAnalyzer {
  private readonly config: Required<StateDiffAnalyzerConfig>;

  constructor(config: StateDiffAnalyzerConfig = {}) {
    this.config = {
      minBalanceChange: config.minBalanceChange ?? '0',
      includeFeeChanges: config.includeFeeChanges ?? false,
      traceTemporaryAccounts: config.traceTemporaryAccounts ?? false,
    };
  }

  /**
   * Analyzes a raw transaction and returns atomic events based on state differences
   * @param rawTx - Raw transaction data from Solana RPC
   * @returns Array of atomic events representing balance changes
   */
  public analyze(rawTx: RawTransaction): AtomicEvent[] {
    const events: AtomicEvent[] = [];

    // Analyze SOL balance changes
    const solEvents = this.analyzeSolBalanceChanges(rawTx);
    events.push(...solEvents);

    // Debug: Log SOL events
    console.log('SOL events generated:', solEvents.length);

    // Analyze SPL token balance changes
    const splEvents = this.analyzeSplTokenBalanceChanges(rawTx);
    events.push(...splEvents);

    // Debug: Log SPL events
    console.log('SPL events generated:', splEvents.length);
    console.log('Total events:', events.length);

    return events;
  }

  /**
   * Analyzes SOL balance changes between pre and post transaction states
   */
  private analyzeSolBalanceChanges(rawTx: RawTransaction): SolTransferEvent[] {
    const events: SolTransferEvent[] = [];
    const { preBalances, postBalances } = rawTx.meta;
    const accountKeys = rawTx.transaction.message.accountKeys;

    // Calculate balance changes
    const balanceChanges: BalanceChange[] = [];
    const maxLength = Math.max(preBalances.length, postBalances.length);

    for (let i = 0; i < maxLength; i++) {
      const preBalance = preBalances[i] || 0;
      const postBalance = postBalances[i] || 0;
      const change = postBalance - preBalance;

      if (change !== 0) {
        balanceChanges.push({
          accountIndex: i,
          account: accountKeys[i],
          preBalance,
          postBalance,
          change,
        });
      }
    }

    // Filter out fee-related changes if configured
    const filteredChanges = this.config.includeFeeChanges
      ? balanceChanges
      : this.filterFeeChanges(balanceChanges, rawTx.meta.fee);

    // Convert balance changes to atomic events
    for (const balanceChange of filteredChanges) {
      const absChange = Math.abs(balanceChange.change);
      const minChange = parseInt(this.config.minBalanceChange, 10);

      if (absChange >= minChange) {
        const eventType = balanceChange.change > 0 ? 'CREDIT_SOL' : 'DEBIT_SOL';

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
  }

  /**
   * Analyzes SPL token balance changes between pre and post transaction states
   */
  private analyzeSplTokenBalanceChanges(
    rawTx: RawTransaction
  ): SplTokenTransferEvent[] {
    const events: SplTokenTransferEvent[] = [];
    const { preTokenBalances, postTokenBalances } = rawTx.meta;
    const accountKeys = rawTx.transaction.message.accountKeys;

    // Debug: Log input data
    console.log('SPL Analysis - Pre token balances count:', preTokenBalances.length);
    console.log('SPL Analysis - Post token balances count:', postTokenBalances.length);
    console.log('SPL Analysis - Account keys count:', accountKeys.length);

    // Create maps for efficient lookup
    const preBalanceMap = new Map<number, TokenBalance>();
    const postBalanceMap = new Map<number, TokenBalance>();

    for (const balance of preTokenBalances) {
      preBalanceMap.set(balance.accountIndex, balance);
    }

    for (const balance of postTokenBalances) {
      postBalanceMap.set(balance.accountIndex, balance);
    }

    // Find all accounts that had token balance changes
    const allAccountIndices = new Set([
      ...preTokenBalances.map(b => b.accountIndex),
      ...postTokenBalances.map(b => b.accountIndex),
    ]);

    console.log('SPL Analysis - All account indices:', Array.from(allAccountIndices));

    const tokenBalanceChanges: TokenBalanceChange[] = [];

    for (const accountIndex of allAccountIndices) {
      const preBalance = preBalanceMap.get(accountIndex);
      const postBalance = postBalanceMap.get(accountIndex);

      console.log(`SPL Analysis - Processing account ${accountIndex}:`, {
        preBalance: preBalance?.uiTokenAmount.amount,
        postBalance: postBalance?.uiTokenAmount.amount
      });

      // Handle different scenarios
      if (preBalance && postBalance) {
        // Account existed before and after - check for balance change
        if (
          preBalance.uiTokenAmount.amount !== postBalance.uiTokenAmount.amount
        ) {
          const change = this.calculateTokenAmountChange(
            preBalance.uiTokenAmount.amount,
            postBalance.uiTokenAmount.amount
          );

          console.log(`SPL Analysis - Balance change for account ${accountIndex}:`, change);

          tokenBalanceChanges.push({
            accountIndex,
            tokenAccount: accountKeys[accountIndex],
            owner: postBalance.owner,
            mint: postBalance.mint,
            decimals: postBalance.uiTokenAmount.decimals,
            programId: postBalance.programId,
            preAmount: preBalance.uiTokenAmount.amount,
            postAmount: postBalance.uiTokenAmount.amount,
            change,
          });
        }
      } else if (!preBalance && postBalance) {
        // Account was created and received tokens
        if (postBalance.uiTokenAmount.amount !== '0') {
          console.log(`SPL Analysis - Account ${accountIndex} created with amount:`, postBalance.uiTokenAmount.amount);

          tokenBalanceChanges.push({
            accountIndex,
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
      } else if (preBalance && !postBalance) {
        // Account was closed/destroyed
        if (preBalance.uiTokenAmount.amount !== '0') {
          console.log(`SPL Analysis - Account ${accountIndex} closed with amount:`, preBalance.uiTokenAmount.amount);

          tokenBalanceChanges.push({
            accountIndex,
            tokenAccount: accountKeys[accountIndex],
            owner: preBalance.owner,
            mint: preBalance.mint,
            decimals: preBalance.uiTokenAmount.decimals,
            programId: preBalance.programId,
            preAmount: preBalance.uiTokenAmount.amount,
            postAmount: '0',
            change: `-${preBalance.uiTokenAmount.amount}`,
          });
        }
      }
    }

    console.log('SPL Analysis - Token balance changes found:', tokenBalanceChanges.length);

    // Process temporary account transfers if enabled
    const processedChanges = this.config.traceTemporaryAccounts
      ? this.processTemporaryAccountTransfers(tokenBalanceChanges)
      : tokenBalanceChanges;

    // Convert token balance changes to atomic events
    for (const change of processedChanges) {
      const minChange = BigInt(this.config.minBalanceChange);
      const changeAmount = BigInt(change.change);
      const absChange = changeAmount < 0n ? -changeAmount : changeAmount;

      console.log(`SPL Analysis - Processing change: ${change.change}, abs: ${absChange}, min: ${minChange}`);

      if (absChange >= minChange) {
        const isPositive = changeAmount > 0n;
        const eventType = isPositive ? 'CREDIT_SPL' : 'DEBIT_SPL';

        console.log(`SPL Analysis - Creating ${eventType} event for account ${change.owner}`);

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

    console.log('SPL Analysis - Final events generated:', events.length);
    return events;
  }

  /**
   * Filters out fee-related balance changes
   */
  private filterFeeChanges(
    changes: BalanceChange[],
    fee: number
  ): BalanceChange[] {
    // Simple heuristic: if there's exactly one debit that matches the fee amount,
    // it's likely the fee payment
    const feeDebits = changes.filter(c => c.change === -fee);

    if (feeDebits.length === 1) {
      return changes.filter(c => c !== feeDebits[0]);
    }

    return changes;
  }

  /**
   * Calculates the change between two token amounts (as strings)
   */
  private calculateTokenAmountChange(
    preAmount: string,
    postAmount: string
  ): string {
    const pre = BigInt(preAmount);
    const post = BigInt(postAmount);
    const change = post - pre;

    return change.toString();
  }

  /**
   * Processes temporary account transfers to trace the final beneficiaries
   * This is a simplified implementation - in practice, this would need more
   * sophisticated logic to handle complex temporary account scenarios
   */
  private processTemporaryAccountTransfers(
    changes: TokenBalanceChange[]
  ): TokenBalanceChange[] {
    // For now, return changes as-is
    // TODO: Implement logic to trace temporary ATA transfers
    // This would involve detecting patterns where tokens flow through temporary accounts
    // and consolidating them into direct transfers
    return changes;
  }
}
