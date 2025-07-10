// ft/lib/api.ts

// Базовый URL нашего локального API
const API_BASE_URL = "http://127.0.0.1:8000/api";

/**
 * Общая функция для выполнения запросов к API.
 * @param endpoint - Путь к эндпоинту (например, '/hypotheses')
 * @param options - Опции для fetch-запроса
 * @returns - Распарсенный JSON-ответ
 */
async function fetcher<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  });

  if (!response.ok) {
    const errorInfo = await response.json().catch(() => ({}));
    throw new Error(`API Error: ${response.status} ${response.statusText} - ${errorInfo.detail || 'No details'}`);
  }

  return response.json();
}

// --- Типы данных (для строгой типизации) ---

interface Hypothesis {
  id: string;
  title: string;
  description: string;
  status: "active" | "tested" | "failed" | "pending";
  priority: "high" | "medium" | "low";
  created_at: string;
  expected_outcome: string;
  success_criteria: string[];
  dataset_requirements: string[];
  analysis_approach: string;
  confidence_level: number;
  estimated_test_duration: string;
  potential_impact: string;
  related_patterns: string[];
}

interface FeatureDistribution {
  min: number;
  max: number;
  mean: number;
  median: number;
  std: number;
}

interface Feature {
  name: string;
  type: "numerical" | "categorical" | "boolean";
  category: "liquidity" | "timing" | "network" | "market";
  description: string;
  importance: number;
  correlation: number;
  nullRate: number;
  uniqueValues: number;
  distribution: FeatureDistribution;
}

// --- Функции для взаимодействия с API ---

export const getHypotheses = (): Promise<Hypothesis[]> => {
  return fetcher<Hypothesis[]>('/hypotheses');
};

export const getFeatures = (): Promise<Feature[]> => {
  return fetcher<Feature[]>('/features');
};

export const getAnalysisResultsList = (): Promise<string[]> => {
  return fetcher<string[]>('/analysis_results');
};

export const getAnalysisResult = (filename: string): Promise<Record<string, any>> => {
  return fetcher<Record<string, any>>(`/analysis_results/${filename}`);
};

export const runAnalysisScript = (scriptName: string, params: string[] = []): Promise<any> => {
  return fetcher('/analysis/run', {
    method: 'POST',
    body: JSON.stringify({
      script_name: scriptName,
      parameters: params,
    }),
  });
};

// --- Функции для работы с группами токенов ---

interface TokenGroup {
  name: string;
  token_count: number;
  created_at: number;
  modified_at: number;
  file_size: number;
}

interface TokenGroupDetail {
  name: string;
  token_count: number;
  tokens: string[];
  created_at: number;
  modified_at: number;
  file_size: number;
}

export const listTokenGroups = (): Promise<TokenGroup[]> => {
  return fetcher<TokenGroup[]>('/groups');
};

export const getGroupDetails = (groupName: string): Promise<TokenGroupDetail> => {
  return fetcher<TokenGroupDetail>(`/groups/${groupName}`);
};

export const createTokenGroup = (groupName: string, tokens: string[]): Promise<any> => {
  return fetcher('/groups', {
    method: 'POST',
    body: JSON.stringify({
      group_name: groupName,
      tokens: tokens,
    }),
  });
};

export const deleteTokenGroup = (groupName: string): Promise<any> => {
  return fetcher(`/groups/${groupName}`, {
    method: 'DELETE',
  });
};

// --- Функции для работы с прогрессом сбора данных ---

interface TokenProgress {
  token_address: string;
  db_tx_count: number;
  on_chain_tx_count: number | null;
  completeness_ratio: number | null;
  last_checked_at: number | null;
  status: 'unknown' | 'checking' | 'collecting' | 'complete' | 'error';
  error_message: string | null;
  last_collection_at: number | null;
  // НОВЫЕ АНАЛИТИЧЕСКИЕ ПОЛЯ
  first_dump_signature: string | null;
  first_dump_time: number | null;
  creation_time: number | null;
  first_dump_price_drop_percent: number | null;
  pre_dump_tx_count: number | null;
}

interface GroupProgress {
  group_name: string;
  total_tokens: number;
  group_status: 'idle' | 'refreshing' | 'collecting';
  progress_percent: number | null;
  current_step_description: string | null;
  tokens: TokenProgress[];
}

export const getGroupProgress = (groupName: string, preDumpMode: boolean = false): Promise<GroupProgress> => {
  const param = preDumpMode ? '?pre_dump_only=true' : '';
  return fetcher<GroupProgress>(`/groups/${groupName}/progress${param}`);
};

export const refreshGroupOnChainCounts = (groupName: string): Promise<any> => {
  return fetcher(`/groups/${groupName}/refresh`, {
    method: 'POST',
  });
};

export const startGroupCollection = (groupName: string): Promise<any> => {
  return fetcher(`/groups/${groupName}/collect`, {
    method: 'POST',
  });
};

export const startTokenCollection = (tokenAddress: string): Promise<any> => {
  return fetcher(`/tokens/${tokenAddress}/collect`, {
    method: 'POST',
  });
};

export const updateTokenGroup = (groupName: string, tokens: string[]): Promise<any> => {
  return fetcher(`/groups/${groupName}`, {
    method: 'PUT',
    body: JSON.stringify({
      tokens: tokens,
    }),
  });
};

export const getTokenDumpDetails = async (tokenAddress: string): Promise<any | null> => {
  const res = await fetcher<any | null>(`/tokens/${tokenAddress}/dump_details`);
  return res;
};

export const getTokenDossier = async (tokenAddress: string): Promise<any> => {
  return fetcher<any>(`/tokens/${tokenAddress}/dossier`);
};

export const refreshTokenOnChainCount = (tokenAddress: string): Promise<any> => {
  return fetcher(`/tokens/${tokenAddress}/refresh`, {
    method: 'POST',
  });
};

export const findTokenDump = (tokenAddress: string): Promise<any> => {
  return fetcher(`/tokens/${tokenAddress}/find_dump`, {
    method: 'POST',
  });
};

// Экспортируем типы для использования в компонентах
export type { TokenProgress, GroupProgress, TokenGroup, TokenGroupDetail, Hypothesis, Feature }; 