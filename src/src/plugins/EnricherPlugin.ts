import { SemanticEvent, RawTransaction } from '../types';

export interface EnricherPlugin {
  /**
   * Обогащает семантическое событие дополнительными метаданными.
   * @param event Семантическое событие (например, SWAP)
   * @param tx Исходная raw-транзакция
   * @returns Обогащённое событие
   */
  enrich(event: SemanticEvent, tx: RawTransaction): Promise<SemanticEvent>;
} 