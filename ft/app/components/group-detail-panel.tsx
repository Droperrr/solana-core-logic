"use client";

import React, { useState, useEffect } from 'react';
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import { AlertCircle, CheckCircle, Clock, Download, RefreshCw, Activity, Edit, Save, X, FileSearch, Info } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { 
  getGroupProgress, 
  refreshGroupOnChainCounts, 
  startGroupCollection,
  startTokenCollection,
  updateTokenGroup,
  type TokenProgress, 
  type GroupProgress,
  getTokenDumpDetails,
  getTokenDossier,
  refreshTokenOnChainCount,
  findTokenDump
} from "@/lib/api";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface GroupDetailPanelProps {
  groupName: string;
  preDumpMode: boolean;
}

function TokenDossier({ tokenAddress }: { tokenAddress: string }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dossier, setDossier] = useState<any | null>(null);

  useEffect(() => {
    if (!tokenAddress) return;
    setLoading(true);
    setError(null);
    getTokenDossier(tokenAddress)
      .then(setDossier)
      .catch(() => setError("Ошибка загрузки досье токена"))
      .finally(() => setLoading(false));
  }, [tokenAddress]);

  if (!tokenAddress) return null;
  if (loading) return <div className="p-4">Загрузка...</div>;
  if (error) return <div className="p-4 text-red-500">{error}</div>;
  if (!dossier) return <div className="p-4">Нет данных по токену.</div>;

  const dump = dossier.dump_info || {};
  const txStats = dossier.tx_stats || {};

  return (
    <div className="space-y-4 p-2">
      <h2 className="text-xl font-bold">Досье токена: {tokenAddress}</h2>
      <div>
        <b>Всего транзакций:</b> {txStats.tx_count ?? '—'}<br />
        <b>Первый блок:</b> {txStats.first_block_time ? new Date(txStats.first_block_time * 1000).toLocaleString() : '—'}<br />
        <b>Последний блок:</b> {txStats.last_block_time ? new Date(txStats.last_block_time * 1000).toLocaleString() : '—'}
      </div>
      <div>
        <h3 className="font-semibold">Детали дампа</h3>
        {dump.first_dump_signature ? (
          <>
            <div><b>Signature:</b> {dump.first_dump_signature}</div>
            <div><b>Time:</b> {dump.first_dump_time ? new Date(dump.first_dump_time * 1000).toLocaleString() : '—'}</div>
            <div><b>Price Drop %:</b> {dump.first_dump_price_drop_percent ?? '—'}</div>
            <div><b>Last Processed:</b> {dump.last_processed_signature || '—'}</div>
          </>
        ) : (
          <div className="text-slate-500">Информация о первом дампе еще не зафиксирована.</div>
        )}
      </div>
    </div>
  );
}

const GroupDetailPanel: React.FC<GroupDetailPanelProps> = ({ groupName, preDumpMode }) => {
  const [progress, setProgress] = useState<GroupProgress | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [collecting, setCollecting] = useState(false);
  
  // Локальное состояние для немедленной обратной связи
  const [isTaskRunning, setIsTaskRunning] = useState(false);
  
  // Состояние для режима редактирования
  const [isEditing, setIsEditing] = useState(false);
  const [editableTokens, setEditableTokens] = useState("");
  const [saving, setSaving] = useState(false);

  // Состояние для polling
  const [isPolling, setIsPolling] = useState(false);

  const [isDumpModalOpen, setIsDumpModalOpen] = useState(false);
  const [dumpDetails, setDumpDetails] = useState<any | null>(null);
  const [dumpLoading, setDumpLoading] = useState(false);
  const [dumpError, setDumpError] = useState<string | null>(null);

  const [dossierToken, setDossierToken] = useState<string | null>(null);

  // Загружаем прогресс при монтировании и изменении группы или режима
  useEffect(() => {
    loadProgress();
  }, [groupName, preDumpMode]);

  // Polling для обновления статуса активных задач
  useEffect(() => {
    let intervalId: NodeJS.Timeout | null = null;

    if (isPolling || (progress && progress.group_status !== 'idle')) {
      intervalId = setInterval(async () => {
        try {
          const data = await getGroupProgress(groupName, preDumpMode);
          setProgress(data);
          
          // Если задача завершена, останавливаем polling
          if (data.group_status === 'idle') {
            setIsPolling(false);
            setRefreshing(false);
            setCollecting(false);
            setIsTaskRunning(false);
          }
        } catch (err) {
          console.error('Ошибка при polling:', err);
          // Не показываем ошибку в UI при polling, только в консоли
        }
      }, 5000); // Опрашиваем каждые 5 секунд
    }

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [isPolling, progress?.group_status, groupName, preDumpMode]);

  const loadProgress = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getGroupProgress(groupName, preDumpMode);
      setProgress(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка загрузки прогресса');
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshOnChainCounts = async () => {
    try {
      setRefreshing(true);
      setIsTaskRunning(true);
      setIsPolling(true);
      await refreshGroupOnChainCounts(groupName);
      
      // Обновляем данные сразу для отображения статуса
      await loadProgress();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка обновления статистики');
      setRefreshing(false);
      setIsTaskRunning(false);
      setIsPolling(false);
    }
  };

  const handleStartCollection = async () => {
    try {
      setCollecting(true);
      setIsTaskRunning(true);
      setIsPolling(true);
      await startGroupCollection(groupName);
      
      // Обновляем данные сразу для отображения статуса
      await loadProgress();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка запуска сбора');
      setCollecting(false);
      setIsTaskRunning(false);
      setIsPolling(false);
    }
  };

  const handleTokenCollection = async (tokenAddress: string) => {
    try {
      await startTokenCollection(tokenAddress);
      alert(`Сбор данных для токена ${tokenAddress.slice(0, 8)}... запущен в фоне.`);
      
      // Обновляем данные через некоторое время
      setTimeout(loadProgress, 2000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка запуска сбора токена');
    }
  };

  const handleTokenRefresh = async (tokenAddress: string) => {
    try {
      await refreshTokenOnChainCount(tokenAddress);
      alert(`Обновление статистики для токена ${tokenAddress.slice(0, 8)}... запущено в фоне.`);
      
      // Обновляем данные через некоторое время
      setTimeout(loadProgress, 2000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка запуска обновления токена');
    }
  };

  const handleFindDump = async (tokenAddress: string) => {
    try {
      await findTokenDump(tokenAddress);
      alert(`Поиск дампа для токена ${tokenAddress.slice(0, 8)}... запущен в фоне.`);
      
      // Обновляем данные через некоторое время
      setTimeout(loadProgress, 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка запуска поиска дампа');
    }
  };

  const handleStartEditing = () => {
    if (progress?.tokens) {
      // Заполняем текстовое поле текущим списком токенов
      const tokenList = progress.tokens.map(token => token.token_address).join('\n');
      setEditableTokens(tokenList);
      setIsEditing(true);
    }
  };

  const handleCancelEditing = () => {
    setIsEditing(false);
    setEditableTokens("");
    setError(null);
  };

  const handleSaveTokens = async () => {
    try {
      setSaving(true);
      setError(null);
      
      // Парсим токены из текстового поля
      const tokens = editableTokens
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0);
      
      if (tokens.length === 0) {
        setError('Список токенов не может быть пустым');
        return;
      }
      
      // Отправляем обновленный список на backend
      await updateTokenGroup(groupName, tokens);
      
      // Уведомляем об успехе
      alert(`Группа "${groupName}" успешно обновлена!\nТокенов: ${tokens.length}`);
      
      // Выходим из режима редактирования
      setIsEditing(false);
      setEditableTokens("");
      
      // Перезагружаем данные о прогрессе
      await loadProgress();
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка сохранения группы');
    } finally {
      setSaving(false);
    }
  };

  const getAnalyticalStatusBadge = (token: TokenProgress) => {
    // НОВАЯ АНАЛИТИЧЕСКАЯ ЛОГИКА СТАТУСОВ
    
    // Если найден дамп - это главный результат
    if (token.first_dump_signature) {
      return (
        <Badge variant="destructive" className="flex items-center gap-1 font-semibold">
          <AlertCircle className="w-3 h-3" />
          ДАМП ОБНАРУЖЕН
        </Badge>
      );
    }
    
    // Если сбор данных в процессе
    if (['checking', 'collecting'].includes(token.status)) {
      const label = token.status === 'checking' ? `Проверка... (${token.db_tx_count} / ???)` : 'Сбор данных...';
      return (
        <Badge variant="outline" className="flex items-center gap-1">
          <RefreshCw className="w-3 h-3 animate-spin" />
          {label}
        </Badge>
      );
    }
    
    // Если данные полные и дамп не найден - отличный результат
    if ((token.completeness_ratio || 0) >= 1.0 && token.status !== 'error') {
      return (
        <Badge variant="default" className="flex items-center gap-1 bg-green-100 text-green-800 border-green-300">
          <CheckCircle className="w-3 h-3" />
          Дамп не найден
        </Badge>
      );
    }
    
    // Если есть ошибка
    if (token.status === 'error') {
      return (
        <Badge variant="destructive" className="flex items-center gap-1">
          <AlertCircle className="w-3 h-3" />
          Ошибка
        </Badge>
      );
    }
    
    // Если требуется дополнительный сбор
    if ((token.completeness_ratio || 0) < 1.0) {
      return (
        <Badge variant="secondary" className="flex items-center gap-1">
          <Download className="w-3 h-3" />
          Требуется сбор
        </Badge>
      );
    }
    
    // Неизвестное состояние
    return (
      <Badge variant="secondary" className="flex items-center gap-1">
        <AlertCircle className="w-3 h-3" />
        Неизвестно
      </Badge>
    );
  };

  const formatTimestamp = (timestamp: number | null) => {
    if (!timestamp) return 'Никогда';
    return new Date(timestamp * 1000).toLocaleString('ru-RU');
  };

  const formatTokenAddress = (address: string) => {
    return `${address.slice(0, 8)}...${address.slice(-8)}`;
  };

  const formatDumpSignature = (signature: string | null) => {
    if (!signature) return '—';
    return (
      <a 
        href={`https://solscan.io/tx/${signature}`}
        target="_blank"
        rel="noopener noreferrer"
        className="text-blue-600 hover:text-blue-800 underline font-mono text-xs"
        onClick={(e) => e.stopPropagation()}
      >
        {signature.slice(0, 8)}...{signature.slice(-8)}
      </a>
    );
  };

  const formatLifetimeToSeconds = (dumpTime: number | null, creationTime: number | null) => {
    if (!dumpTime || !creationTime) return '—';
    const lifetimeSeconds = dumpTime - creationTime;
    
    if (lifetimeSeconds < 60) return `${lifetimeSeconds}с`;
    if (lifetimeSeconds < 3600) return `${Math.floor(lifetimeSeconds / 60)}м ${lifetimeSeconds % 60}с`;
    
    const hours = Math.floor(lifetimeSeconds / 3600);
    const minutes = Math.floor((lifetimeSeconds % 3600) / 60);
    const seconds = lifetimeSeconds % 60;
    
    return `${hours}ч ${minutes}м ${seconds}с`;
  };

  const calculateOverallProgress = () => {
    if (!progress?.tokens.length) return 0;
    
    const tokensWithProgress = progress.tokens.filter(t => t.completeness_ratio !== null);
    if (tokensWithProgress.length === 0) return 0;
    
    const totalRatio = tokensWithProgress.reduce((sum, token) => sum + (token.completeness_ratio || 0), 0);
    return Math.round((totalRatio / tokensWithProgress.length) * 100);
  };

  const getAnalyticalStats = () => {
    if (!progress?.tokens.length) return { total: 0, dumpsFound: 0, noDumps: 0, inProgress: 0, needsCollection: 0, error: 0 };
    
    const total = progress.tokens.length;
    const dumpsFound = progress.tokens.filter(t => t.first_dump_signature).length;
    const error = progress.tokens.filter(t => t.status === 'error').length;
    const inProgress = progress.tokens.filter(t => ['checking', 'collecting'].includes(t.status)).length;
    const noDumps = progress.tokens.filter(t => !t.first_dump_signature && (t.completeness_ratio || 0) >= 1.0 && t.status !== 'error').length;
    const needsCollection = total - dumpsFound - error - inProgress - noDumps;
    
    return { total, dumpsFound, noDumps, inProgress, needsCollection, error };
  };

  // Deduplicate tokens to avoid React key conflicts
  const uniqueTokens = React.useMemo(() => {
    if (!progress?.tokens.length) return [];
    const seen = new Set<string>();
    return progress.tokens.filter(token => {
      if (seen.has(token.token_address)) {
        return false;
      }
      seen.add(token.token_address);
      return true;
    });
  }, [progress?.tokens]);

  const handleShowDumpDetails = async (tokenAddress: string) => {
    setDumpLoading(true);
    setDumpError(null);
    setIsDumpModalOpen(true);
    try {
      const details = await getTokenDumpDetails(tokenAddress);
      setDumpDetails(details);
    } catch (err) {
      setDumpError('Ошибка загрузки деталей дампа');
      setDumpDetails(null);
    } finally {
      setDumpLoading(false);
    }
  };

  const tokenHasDump = (tokenAddress: string) => {
    if (!progress || !progress.tokens) return false;
    // Можно расширить, если есть кэш дампов
    // Для простоты: если в dumpDetails есть first_dump_signature
    return dumpDetails && dumpDetails.token_address === tokenAddress && dumpDetails.first_dump_signature;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center p-8">
        <RefreshCw className="w-6 h-6 animate-spin mr-2" />
        <span>Загрузка прогресса...</span>
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>
          {error}
          <Button variant="outline" size="sm" onClick={loadProgress} className="mt-2">
            Повторить
          </Button>
        </AlertDescription>
      </Alert>
    );
  }

  if (!progress) {
    return (
      <Alert>
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>
          Данные о прогрессе не найдены
        </AlertDescription>
      </Alert>
    );
  }

  const overallProgress = calculateOverallProgress();
          const stats = getAnalyticalStats();

  return (
    <div className="space-y-6">
      {/* Заголовок и общая статистика */}
      <div>
        <div className="flex items-center gap-3 mb-4">
          <h3 className="text-lg font-semibold">Прогресс сбора данных: {groupName}</h3>
          {progress?.group_status !== 'idle' && (
            <div className="flex items-center gap-2 px-3 py-1 bg-blue-100 rounded-full">
              <RefreshCw className="w-4 h-4 animate-spin text-blue-600" />
              <span className="text-sm text-blue-800">
                {progress?.group_status === 'refreshing' && 'Обновление статистики...'}
                {progress?.group_status === 'collecting' && 'Сбор данных...'}
              </span>
            </div>
          )}
        </div>
        
        {/* Прогресс-бар для фоновых задач */}
        {progress?.group_status !== 'idle' && (
          <Card className="mb-4">
            <CardContent className="pt-6">
              <div className="space-y-3">
                <div className="flex justify-between text-sm">
                  <span className="font-medium">
                    {progress?.group_status === 'refreshing' && 'Прогресс обновления статистики'}
                    {progress?.group_status === 'collecting' && 'Прогресс сбора данных'}
                  </span>
                  <span className="text-muted-foreground">
                    {progress?.progress_percent ? `${Math.round(progress.progress_percent)}%` : '0%'}
                  </span>
                </div>
                <Progress 
                  value={progress?.progress_percent || 0} 
                  className="h-2"
                />
                {progress?.current_step_description && (
                  <div className="text-xs text-muted-foreground">
                    {progress.current_step_description}
                  </div>
                )}
              </div>
                         </CardContent>
           </Card>
         )}
          
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="w-5 h-5" />
              Общий прогресс
              {isPolling && (
                <span className="flex items-center gap-1 text-xs text-blue-500 ml-2">
                  <RefreshCw className="w-3 h-3 animate-spin" />
                  Обновление статусов...
                </span>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <div className="flex justify-between text-sm mb-2">
                <span>Общая полнота данных</span>
                <span>{overallProgress}%</span>
              </div>
              <Progress value={overallProgress} className="h-2" />
            </div>
            
            <div className="grid grid-cols-2 md:grid-cols-6 gap-4 text-sm">
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">{stats.total}</div>
                <div className="text-muted-foreground">Всего токенов</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600 font-bold">{stats.dumpsFound}</div>
                <div className="text-muted-foreground">🔥 Дампы найдены</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">{stats.noDumps}</div>
                <div className="text-muted-foreground">✅ Дамп не найден</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-orange-600">{stats.inProgress}</div>
                <div className="text-muted-foreground">⚡ В процессе</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-600">{stats.needsCollection}</div>
                <div className="text-muted-foreground">📝 Требуется сбор</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600">{stats.error}</div>
                <div className="text-muted-foreground">❌ Ошибки</div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Кнопки управления */}
      <div className="flex gap-2 flex-wrap">
        {!isEditing ? (
          <>
            <Button 
              onClick={handleStartEditing}
              variant="outline"
            >
              <Edit className="w-4 h-4 mr-2" />
              Редактировать состав группы
            </Button>
            
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button 
                    onClick={handleRefreshOnChainCounts} 
                    disabled={isTaskRunning || progress?.group_status !== 'idle'}
                    variant="outline"
                  >
                    {(refreshing || progress?.group_status === 'refreshing') ? (
                      <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    ) : (
                      <RefreshCw className="w-4 h-4 mr-2" />
                    )}
                    {(refreshing || progress?.group_status === 'refreshing') ? 'Обновление...' : 'Пересчитать ончейн'}
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Получить актуальное количество транзакций в сети.<br/>
                  Используйте, если подозреваете, что появились новые транзакции.</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
            
            <Button 
              onClick={handleStartCollection} 
              disabled={isTaskRunning || progress?.group_status !== 'idle'}
            >
              {(collecting || progress?.group_status === 'collecting') ? (
                <Download className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Download className="w-4 h-4 mr-2" />
              )}
              {(collecting || progress?.group_status === 'collecting') ? 'Сбор данных...' : 'Дособрать все токены'}
            </Button>
            
            <Button 
              onClick={loadProgress} 
              variant="ghost"
              size="sm"
            >
              <RefreshCw className="w-4 h-4 mr-2" />
              Обновить данные
            </Button>
          </>
        ) : (
          <>
            <Button 
              onClick={handleSaveTokens}
              disabled={saving}
            >
              {saving ? (
                <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Save className="w-4 h-4 mr-2" />
              )}
              Сохранить изменения
            </Button>
            
            <Button 
              onClick={handleCancelEditing}
              variant="outline"
              disabled={saving}
            >
              <X className="w-4 h-4 mr-2" />
              Отмена
            </Button>
          </>
        )}
      </div>

      {/* Таблица токенов или режим редактирования */}
      <Card>
        <CardHeader>
          <CardTitle>
            {isEditing ? 'Редактирование состава группы' : 'Детали по токенам'}
          </CardTitle>
          {isEditing && (
            <p className="text-sm text-muted-foreground">
              Введите адреса токенов по одному на строку. Можете добавлять новые или удалять существующие.
            </p>
          )}
        </CardHeader>
        <CardContent>
          {isEditing ? (
            // Режим редактирования: показываем текстовое поле
            <div className="space-y-4">
              <Textarea
                value={editableTokens}
                onChange={(e) => setEditableTokens(e.target.value)}
                placeholder="Введите адреса токенов по одному на строку..."
                rows={12}
                className="font-mono text-sm"
              />
              <div className="text-sm text-muted-foreground">
                Токенов в списке: {editableTokens.split('\n').filter(line => line.trim()).length}
              </div>
            </div>
          ) : (
            // Режим просмотра: показываем таблицу с прогрессом
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Токен</TableHead>
                  <TableHead>Транзакций в БД</TableHead>
                  <TableHead>Аналитический статус</TableHead>
                  <TableHead>Сигнатура дампа</TableHead>
                  <TableHead>Время жизни до дампа</TableHead>
                  <TableHead>Транзакций до дампа</TableHead>
                  <TableHead>Действия</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {progress?.tokens?.map((token) => (
                  <TableRow
                    key={token.token_address}
                    className="cursor-pointer hover:bg-slate-100"
                    onClick={() => setDossierToken(token.token_address)}
                  >
                    <TableCell className="font-mono text-sm">
                      {formatTokenAddress(token.token_address)}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        <Badge variant="outline" className="font-mono">
                          {token.db_tx_count.toLocaleString()}
                        </Badge>
                        <TooltipProvider>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Button
                                size="sm"
                                variant="ghost"
                                className="h-6 w-6 p-0"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleTokenRefresh(token.token_address);
                                }}
                                disabled={['checking', 'collecting'].includes(token.status)}
                              >
                                <RefreshCw className="w-3 h-3" />
                              </Button>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Обновить статистику из сети</p>
                            </TooltipContent>
                          </Tooltip>
                        </TooltipProvider>
                      </div>
                    </TableCell>
                    <TableCell>
                      {getAnalyticalStatusBadge(token)}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-2">
                        {token.first_dump_signature ? (
                          formatDumpSignature(token.first_dump_signature)
                        ) : (
                          <>
                            <span className="text-muted-foreground">—</span>
                            {token.db_tx_count > 0 && (
                              <TooltipProvider>
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Button
                                      size="sm"
                                      variant="ghost"
                                      className="h-6 w-6 p-0"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        handleFindDump(token.token_address);
                                      }}
                                      disabled={['checking', 'collecting'].includes(token.status)}
                                    >
                                      <FileSearch className="w-3 h-3" />
                                    </Button>
                                  </TooltipTrigger>
                                  <TooltipContent>
                                    <p className="text-xs">Принудительный поиск дампа</p>
                                  </TooltipContent>
                                </Tooltip>
                              </TooltipProvider>
                            )}
                          </>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {formatLifetimeToSeconds(token.first_dump_time, token.creation_time)}
                    </TableCell>
                    <TableCell>
                      {token.pre_dump_tx_count !== null ? (
                        <Badge variant="outline" className="font-mono text-xs bg-amber-50 text-amber-700 border-amber-300">
                          {token.pre_dump_tx_count.toLocaleString()}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </TableCell>
                    <TableCell>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={(e) => {
                                e.stopPropagation();
                                handleTokenCollection(token.token_address);
                              }}
                              disabled={
                                (token.completeness_ratio || 0) >= 1.0 ||
                                ['checking', 'collecting'].includes(token.status)
                              }
                            >
                              <Download className="w-3 h-3 mr-1" />
                              Дособрать
                            </Button>
                          </TooltipTrigger>
                          {((token.completeness_ratio || 0) >= 1.0 || ['checking', 'collecting'].includes(token.status)) && (
                            <TooltipContent>
                              <p className="text-sm">
                                {(token.completeness_ratio || 0) >= 1.0 
                                  ? "Данные уже полные (100%)" 
                                  : "Задача уже выполняется"}
                              </p>
                            </TooltipContent>
                          )}
                        </Tooltip>
                      </TooltipProvider>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Dialog open={isDumpModalOpen} onOpenChange={setIsDumpModalOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Детали дампа</DialogTitle>
          </DialogHeader>
          {dumpLoading ? (
            <p>Загрузка...</p>
          ) : dumpError ? (
            <p className="text-red-500">{dumpError}</p>
          ) : dumpDetails ? (
            <div className="space-y-2">
              <div><b>Signature:</b> {dumpDetails.first_dump_signature || '—'}</div>
              <div><b>Time:</b> {dumpDetails.first_dump_time ? formatTimestamp(dumpDetails.first_dump_time) : '—'}</div>
              <div><b>Price Drop %:</b> {dumpDetails.first_dump_price_drop_percent ?? '—'}</div>
              <div><b>Last Processed:</b> {dumpDetails.last_processed_signature || '—'}</div>
            </div>
          ) : (
            <p>Информация о дампе еще не найдена.</p>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={!!dossierToken} onOpenChange={() => setDossierToken(null)}>
        <DialogContent className="max-w-2xl w-full">
          <DialogTitle>Досье токена: {dossierToken}</DialogTitle>
          {dossierToken && <TokenDossier tokenAddress={dossierToken} />}
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default GroupDetailPanel; 