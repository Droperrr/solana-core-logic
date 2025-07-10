"use client"

import React, { useState, useEffect } from "react"
import { getAnalysisResultsList, getAnalysisResult } from "@/lib/api"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { TrendingUp, Download, Share, Target, Award } from "lucide-react"

interface ExperimentResult {
  id: string
  hypothesisTitle: string
  model: string
  status: "success" | "partial" | "failed"
  completedAt: string
  metrics: {
    accuracy?: number
    precision?: number
    recall?: number
    f1Score?: number
    rocAuc?: number
    [key: string]: any
  }
  featureImportance: Array<{
    feature: string
    importance: number
  }>
  confusionMatrix: number[][]
  insights: string[]
  recommendations: string[]
}

export function ResultsAnalyzer() {
  const [reportList, setReportList] = useState<{ id: string; title: string }[]>([])
  const [selectedReportId, setSelectedReportId] = useState<string | null>(null)
  const [selectedReportContent, setSelectedReportContent] = useState<ExperimentResult | null>(null)
  const [isLoadingList, setIsLoadingList] = useState(true)
  const [isLoadingContent, setIsLoadingContent] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const loadResultsList = async () => {
      try {
        setIsLoadingList(true)
        setError(null)
        const fileList = await getAnalysisResultsList()
        const reports = fileList.map(filename => ({
          id: filename,
          title: filename.replace('.json', '').replace(/_/g, ' '),
        }))
        setReportList(reports)

        if (reports.length > 0) {
          setSelectedReportId(reports[0].id)
        }
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : "An unknown error occurred"
        setError(`Не удалось загрузить список отчетов: ${errorMessage}`)
        console.error("Ошибка при загрузке списка отчетов:", err)
      } finally {
        setIsLoadingList(false)
      }
    }

    loadResultsList()
  }, [])

  useEffect(() => {
    if (!selectedReportId) {
      setSelectedReportContent(null)
      return
    }

    const loadResultContent = async () => {
      try {
        setIsLoadingContent(true)
        setError(null)
        const content = await getAnalysisResult(selectedReportId)
        
        const mappedContent: ExperimentResult = {
          id: content.analysis_id || selectedReportId,
          hypothesisTitle: content.analysis_type || selectedReportId.replace('.json', '').replace(/_/g, ' '),
          model: content.model_name || "Custom Analysis",
          status: content.summary?.status === "SUCCESS" ? "success" : "partial",
          completedAt: content.timestamp ? new Date(content.timestamp).toLocaleString() : "N/A",
          metrics: content.model_performance || {},
          featureImportance: content.results?.feature_importance || content.results?.top_features || [],
          confusionMatrix: content.results?.confusion_matrix || [[0, 0], [0, 0]],
          insights: content.results?.insights || content.results?.anomalies_detected?.map((anomaly: any) => 
            `${anomaly.anomaly_type} detected for ${anomaly.token} with ${anomaly.severity} severity`
          ) || ["No insights provided."],
          recommendations: content.recommendations || ["No recommendations provided."]
        }
        setSelectedReportContent(mappedContent)
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : "An unknown error occurred"
        setError(`Не удалось загрузить отчет '${selectedReportId}': ${errorMessage}`)
        console.error("Ошибка при загрузке содержимого отчета:", err)
        setSelectedReportContent(null)
      } finally {
        setIsLoadingContent(false)
      }
    }

    loadResultContent()
  }, [selectedReportId])

  const getStatusColor = (status: string) => {
    switch (status) {
      case "success":
        return "bg-green-100 text-green-800"
      case "partial":
        return "bg-yellow-100 text-yellow-800"
      case "failed":
        return "bg-red-100 text-red-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "success":
        return <Award className="w-4 h-4" />
      case "partial":
        return <Target className="w-4 h-4" />
      case "failed":
        return <Target className="w-4 h-4" />
      default:
        return <Target className="w-4 h-4" />
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Results Analysis</h2>
          <p className="text-slate-600">Analyze experiment results and extract insights</p>
        </div>
        <div className="flex items-center space-x-2">
          <Select
            value={selectedReportId || ''}
            onValueChange={setSelectedReportId}
            disabled={isLoadingList || reportList.length === 0}
          >
            <SelectTrigger className="w-64">
              <SelectValue placeholder="Выберите отчет..." />
            </SelectTrigger>
            <SelectContent>
              {isLoadingList ? (
                <SelectItem value="loading" disabled>Загрузка...</SelectItem>
              ) : (
                reportList.map((report) => (
                  <SelectItem key={report.id} value={report.id}>
                    {report.title}
                  </SelectItem>
                ))
              )}
            </SelectContent>
          </Select>
          <Button variant="outline">
            <Download className="w-4 h-4 mr-2" />
            Export
          </Button>
          <Button variant="outline">
            <Share className="w-4 h-4 mr-2" />
            Share
          </Button>
        </div>
      </div>

      {/* Loading and Error States */}
      {isLoadingContent && (
        <div className="text-center py-8">
          <p className="text-lg">Загрузка отчета...</p>
        </div>
      )}

      {error && (
        <div className="text-center py-8">
          <p className="text-red-500 text-lg">Ошибка: {error}</p>
          <Button 
            className="mt-4" 
            onClick={() => window.location.reload()}
          >
            Повторить попытку
          </Button>
        </div>
      )}

      {!isLoadingList && !isLoadingContent && !selectedReportContent && (
        <div className="text-center py-8">
          <p className="text-lg text-slate-600">Отчетов не найдено или не удалось загрузить.</p>
        </div>
      )}

      {selectedReportContent && !isLoadingContent && (
        <Card>
          <CardHeader>
            <div className="flex items-start justify-between">
              <div>
                <CardTitle className="text-xl flex items-center space-x-2">
                  {getStatusIcon(selectedReportContent.status)}
                  <span>{selectedReportContent.hypothesisTitle}</span>
                </CardTitle>
                <CardDescription className="mt-1">
                  {selectedReportContent.model} • Completed: {selectedReportContent.completedAt}
                </CardDescription>
              </div>
              <Badge className={getStatusColor(selectedReportContent.status)}>{selectedReportContent.status}</Badge>
            </div>
          </CardHeader>
        <CardContent>
          <Tabs defaultValue="metrics" className="space-y-4">
            <TabsList className="grid w-full grid-cols-4">
              <TabsTrigger value="metrics">Metrics</TabsTrigger>
              <TabsTrigger value="features">Feature Importance</TabsTrigger>
              <TabsTrigger value="insights">Insights</TabsTrigger>
              <TabsTrigger value="recommendations">Recommendations</TabsTrigger>
            </TabsList>

            <TabsContent value="metrics" className="space-y-4">
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                {Object.entries(selectedReportContent.metrics).map(([metric, value]) => (
                  <Card key={metric}>
                    <CardContent className="p-4 text-center">
                      <p className="text-sm text-slate-600 uppercase mb-1">
                        {metric.replace(/([A-Z])/g, " $1").trim()}
                      </p>
                      <p className="text-2xl font-bold text-slate-900">
                        {typeof value === 'number' ? (value * 100).toFixed(1) + '%' : value || 'N/A'}
                      </p>
                    </CardContent>
                  </Card>
                ))}
              </div>

              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Confusion Matrix</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-2 max-w-xs">
                    {selectedReportContent.confusionMatrix.map((row, i) =>
                      row.map((value, j) => (
                        <div
                          key={`${i}-${j}`}
                          className={`p-4 text-center rounded ${
                            i === j ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800"
                          }`}
                        >
                          <p className="text-2xl font-bold">{value}</p>
                          <p className="text-xs">
                            {i === 0 && j === 0
                              ? "True Neg"
                              : i === 0 && j === 1
                                ? "False Pos"
                                : i === 1 && j === 0
                                  ? "False Neg"
                                  : "True Pos"}
                          </p>
                        </div>
                      )),
                    )}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="features" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Feature Importance</CardTitle>
                  <CardDescription>Relative importance of features in the model</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {selectedReportContent.featureImportance.map((item, idx) => (
                      <div key={idx} className="flex items-center space-x-3">
                        <div className="w-32 text-sm font-medium text-slate-700">{item.feature}</div>
                        <div className="flex-1 bg-slate-200 rounded-full h-2">
                          <div
                            className="bg-blue-600 h-2 rounded-full"
                            style={{ width: `${item.importance * 100}%` }}
                          />
                        </div>
                        <div className="w-16 text-sm text-slate-600 text-right">
                          {(item.importance * 100).toFixed(1)}%
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="insights" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Key Insights</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-3">
                    {selectedReportContent.insights.map((insight, idx) => (
                      <li key={idx} className="flex items-start space-x-3">
                        <TrendingUp className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" />
                        <span className="text-slate-700">{insight}</span>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="recommendations" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Recommendations</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-3">
                    {selectedReportContent.recommendations.map((recommendation, idx) => (
                      <li key={idx} className="flex items-start space-x-3">
                        <Target className="w-5 h-5 text-green-600 mt-0.5 flex-shrink-0" />
                        <span className="text-slate-700">{recommendation}</span>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
      )}
    </div>
  )
}
