"use client"

import { useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Play, Pause, Square, Settings, BarChart3, Clock, Cpu } from "lucide-react"
import { runAnalysisScript } from "@/lib/api"

interface Experiment {
  id: string
  hypothesisId: string
  hypothesisTitle: string
  status: "queued" | "running" | "completed" | "failed" | "paused"
  progress: number
  model: string
  dataset: string
  startTime: string
  estimatedDuration: string
  currentMetrics: {
    accuracy?: number
    precision?: number
    recall?: number
    f1Score?: number
  }
  hyperparameters: Record<string, any>
}

export function ExperimentRunner() {
  const [experiments, setExperiments] = useState<Experiment[]>([
    {
      id: "exp_001",
      hypothesisId: "1",
      hypothesisTitle: "Early Liquidity Predicts Success",
      status: "running",
      progress: 67,
      model: "Random Forest",
      dataset: "solana_tokens_v2.1",
      startTime: "2024-01-25 14:30",
      estimatedDuration: "45 min",
      currentMetrics: {
        accuracy: 0.742,
        precision: 0.689,
        recall: 0.801,
        f1Score: 0.741,
      },
      hyperparameters: {
        n_estimators: 100,
        max_depth: 10,
        min_samples_split: 5,
      },
    },
    {
      id: "exp_002",
      hypothesisId: "2",
      hypothesisTitle: "Multi-DEX Presence Indicates Quality",
      status: "completed",
      progress: 100,
      model: "XGBoost",
      dataset: "solana_tokens_v2.1",
      startTime: "2024-01-25 12:15",
      estimatedDuration: "30 min",
      currentMetrics: {
        accuracy: 0.834,
        precision: 0.812,
        recall: 0.856,
        f1Score: 0.833,
      },
      hyperparameters: {
        learning_rate: 0.1,
        max_depth: 6,
        n_estimators: 200,
      },
    },
    {
      id: "exp_003",
      hypothesisId: "3",
      hypothesisTitle: "Provider Concentration Risk",
      status: "queued",
      progress: 0,
      model: "Logistic Regression",
      dataset: "solana_tokens_v2.1",
      startTime: "",
      estimatedDuration: "20 min",
      currentMetrics: {},
      hyperparameters: {
        C: 1.0,
        penalty: "l2",
        solver: "liblinear",
      },
    },
  ])

  // ФУНКЦИЯ-ОБРАБОТЧИК С ДЕТАЛЬНЫМ ЛОГГИРОВАНИЕМ
  const handleRunAnalysis = async (scriptName: string) => {
    console.log(`[1] Кнопка нажата. Скрипт: ${scriptName}`); // ШАГ 1: Проверяем, что обработчик вообще вызывается
    try {
      alert(`[2] Вызов API для скрипта: ${scriptName}. Следите за вкладкой 'Network' в F12.`);
      
      console.log(`[2] Вызов runAnalysisScript('${scriptName}')...`);
      const response = await runAnalysisScript(scriptName);
      
      console.log("[3] Ответ от API получен:", response);
      alert(`[3] Запрос на запуск ${scriptName} успешно отправлен! Ответ: ${JSON.stringify(response)}`);
    
    } catch (error) {
      console.error("[4] ОШИБКА в блоке catch:", error);
      alert(`[4] Ошибка при запуске анализа: ${error}`);
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case "queued":
        return "bg-gray-100 text-gray-800"
      case "running":
        return "bg-blue-100 text-blue-800"
      case "completed":
        return "bg-green-100 text-green-800"
      case "failed":
        return "bg-red-100 text-red-800"
      case "paused":
        return "bg-yellow-100 text-yellow-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "queued":
        return <Clock className="w-4 h-4" />
      case "running":
        return <Cpu className="w-4 h-4 animate-pulse" />
      case "completed":
        return <BarChart3 className="w-4 h-4" />
      case "failed":
        return <Square className="w-4 h-4" />
      case "paused":
        return <Pause className="w-4 h-4" />
      default:
        return <Clock className="w-4 h-4" />
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Experiment Runner</h2>
          <p className="text-slate-600">Monitor and manage ML experiments</p>
        </div>
        <div className="flex items-center space-x-2">
          <Select defaultValue="all">
            <SelectTrigger className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="running">Running</SelectItem>
              <SelectItem value="completed">Completed</SelectItem>
              <SelectItem value="queued">Queued</SelectItem>
            </SelectContent>
          </Select>
          <Button className="bg-green-600 hover:bg-green-700">
            <Play className="w-4 h-4 mr-2" />
            Run New Experiment
          </Button>
        </div>
      </div>

      <div className="grid gap-4">
        {experiments.map((experiment) => (
          <Card key={experiment.id} className="hover:shadow-md transition-shadow">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div>
                  <CardTitle className="text-lg flex items-center space-x-2">
                    {getStatusIcon(experiment.status)}
                    <span>{experiment.hypothesisTitle}</span>
                  </CardTitle>
                  <CardDescription className="mt-1">
                    {experiment.model} • {experiment.dataset} • ID: {experiment.id}
                  </CardDescription>
                </div>
                <Badge className={getStatusColor(experiment.status)}>{experiment.status}</Badge>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {/* Progress Bar */}
                {experiment.status === "running" && (
                  <div>
                    <div className="flex justify-between text-sm mb-2">
                      <span>Progress</span>
                      <span>{experiment.progress}%</span>
                    </div>
                    <Progress value={experiment.progress} className="h-2" />
                  </div>
                )}

                {/* Metrics */}
                {Object.keys(experiment.currentMetrics).length > 0 && (
                  <div>
                    <p className="text-sm font-medium text-slate-700 mb-2">Current Metrics:</p>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      {Object.entries(experiment.currentMetrics).map(([metric, value]) => (
                        <div key={metric} className="text-center p-2 bg-slate-50 rounded">
                          <p className="text-xs text-slate-600 uppercase">{metric}</p>
                          <p className="text-lg font-bold text-slate-900">
                            {typeof value === "number" ? (value * 100).toFixed(1) + "%" : value}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Hyperparameters */}
                <div>
                  <p className="text-sm font-medium text-slate-700 mb-2">Hyperparameters:</p>
                  <div className="flex flex-wrap gap-1">
                    {Object.entries(experiment.hyperparameters).map(([param, value]) => (
                      <Badge key={param} variant="outline" className="text-xs">
                        {param}: {String(value)}
                      </Badge>
                    ))}
                  </div>
                </div>

                {/* Timing Info */}
                <div className="flex items-center justify-between pt-4 border-t">
                  <div className="flex items-center space-x-4 text-sm text-slate-500">
                    {experiment.startTime && <span>Started: {experiment.startTime}</span>}
                    <span>Duration: {experiment.estimatedDuration}</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Button variant="outline" size="sm">
                      <Settings className="w-4 h-4 mr-1" />
                      Config
                    </Button>
                    {experiment.status === "running" && (
                      <Button variant="outline" size="sm">
                        <Pause className="w-4 h-4 mr-1" />
                        Pause
                      </Button>
                    )}
                    {experiment.status === "queued" && (
                      <Button size="sm" className="bg-green-600 hover:bg-green-700">
                        <Play className="w-4 h-4 mr-1" />
                        Start
                      </Button>
                    )}
                    {experiment.status === "completed" && (
                      <Button size="sm" className="bg-blue-600 hover:bg-blue-700">
                        <BarChart3 className="w-4 h-4 mr-1" />
                        View Results
                      </Button>
                    )}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* ДОБАВЛЯЕМ НОВУЮ КАРТОЧКУ ДЛЯ ЗАПУСКА СКРИПТОВ */}
      <Card>
        <CardHeader>
          <CardTitle>Запуск аналитических скриптов</CardTitle>
          <CardDescription>
            Запустите один из ваших Python-скриптов для анализа. Результат будет виден в консоли, где запущен uvicorn.
          </CardDescription>
        </CardHeader>
        <CardContent className="flex gap-4 flex-wrap">
          {/* ТЕСТОВАЯ КНОПКА ДЛЯ ОТЛАДКИ */}
          <Button 
            onClick={() => {
              console.log("[DEBUG] Простая тестовая кнопка нажата!");
              alert("[DEBUG] Простая тестовая кнопка работает!");
            }}
            variant="outline" 
            className="bg-red-50 hover:bg-red-100 border-red-200"
          >
            🔴 ТЕСТ: Простая кнопка
          </Button>
          
          <Button onClick={() => handleRunAnalysis('demo_analysis.py')} variant="outline" className="bg-green-50 hover:bg-green-100">
            🚀 Демо анализ (быстрый тест)
          </Button>
          <Button onClick={() => handleRunAnalysis('phase2_2_test_transaction_hypothesis.py')}>
            Проверить гипотезу о тестовой транзакции
          </Button>
          <Button onClick={() => handleRunAnalysis('phase2_3_coordinated_activity_analysis.py')}>
            Анализ координации
          </Button>
          <Button onClick={() => handleRunAnalysis('phase2_1_data_profiling_fixed.py')}>
            Профилирование данных (исправленная версия)
          </Button>
          <Button onClick={() => handleRunAnalysis('anomaly_detection.py')}>
            Обнаружение аномалий
          </Button>
          <Button onClick={() => handleRunAnalysis('cross_token_unknown_analyzer.py')}>
            Анализ неизвестных токенов
          </Button>
          <Button onClick={() => handleRunAnalysis('complete_data_analysis.py')}>
            Полный анализ данных
          </Button>
        </CardContent>
      </Card>
    </div>
  )
}
