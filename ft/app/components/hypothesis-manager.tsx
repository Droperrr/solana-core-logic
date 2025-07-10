"use client"

import React, { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { PlusCircle, Calendar, Target, TrendingUp } from "lucide-react"
import { getHypotheses } from "@/lib/api"

interface Hypothesis {
  id: string
  title: string
  description: string
  status: "active" | "tested" | "failed" | "pending"
  priority: "high" | "medium" | "low"
  created_at: string
  expected_outcome: string
  success_criteria: string[]
  dataset_requirements: string[]
  analysis_approach: string
  confidence_level: number
  estimated_test_duration: string
  potential_impact: string
  related_patterns: string[]
}

export function HypothesisManager() {
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const loadHypotheses = async () => {
      try {
        setIsLoading(true)
        setError(null)
        const data = await getHypotheses()
        setHypotheses(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "An unknown error occurred")
        console.error("Ошибка при загрузке гипотез:", err)
      } finally {
        setIsLoading(false)
      }
    }

    loadHypotheses()
  }, [])

  const getStatusColor = (status: string) => {
    switch (status) {
      case "active":
        return "bg-green-100 text-green-800"
      case "tested":
        return "bg-blue-100 text-blue-800"
      case "failed":
        return "bg-red-100 text-red-800"
      case "pending":
        return "bg-yellow-100 text-yellow-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case "high":
        return "bg-red-100 text-red-800"
      case "medium":
        return "bg-orange-100 text-orange-800"
      case "low":
        return "bg-green-100 text-green-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold tracking-tight">Hypothesis Manager</h2>
          <p className="text-muted-foreground">
            Управление и отслеживание гипотез для анализа поведения алгоритмических кошельков
          </p>
        </div>
        <Button>
          <PlusCircle className="mr-2 h-4 w-4" />
          Новая гипотеза
        </Button>
      </div>

      {isLoading && (
        <div className="text-center py-8">
          <p className="text-lg">Загрузка гипотез...</p>
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

      {!isLoading && !error && (
        <div className="grid gap-4">
          {hypotheses.length === 0 ? (
            <div className="text-center py-12">
              <p className="text-lg text-muted-foreground">
                Гипотезы не найдены. Создайте новую гипотезу для начала анализа.
              </p>
            </div>
          ) : (
            hypotheses.map((hypothesis, idx) => (
              <Card key={hypothesis.id + '-' + idx} className="w-full">
                <CardHeader>
                  <div className="flex justify-between items-start">
                    <div className="space-y-1">
                      <CardTitle className="text-xl">{hypothesis.title}</CardTitle>
                      <CardDescription className="text-sm">
                        {hypothesis.description}
                      </CardDescription>
                    </div>
                    <div className="flex space-x-2">
                      <Badge className={getStatusColor(hypothesis.status)}>
                        {hypothesis.status}
                      </Badge>
                      <Badge className={getPriorityColor(hypothesis.priority)}>
                        {hypothesis.priority}
                      </Badge>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    <div className="space-y-2">
                      <div className="flex items-center space-x-2">
                        <Calendar className="h-4 w-4 text-muted-foreground" />
                        <span className="text-sm font-medium">Создано</span>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {new Date(hypothesis.created_at).toLocaleDateString('ru-RU')}
                      </p>
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex items-center space-x-2">
                        <Target className="h-4 w-4 text-muted-foreground" />
                        <span className="text-sm font-medium">Ожидаемый результат</span>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {hypothesis.expected_outcome}
                      </p>
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex items-center space-x-2">
                        <TrendingUp className="h-4 w-4 text-muted-foreground" />
                        <span className="text-sm font-medium">Уровень доверия</span>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {hypothesis.confidence_level}%
                      </p>
                    </div>
                  </div>
                  
                  <div className="mt-4 space-y-3">
                    <div>
                      <h4 className="text-sm font-medium mb-2">Критерии успеха:</h4>
                      <div className="flex flex-wrap gap-1">
                        {(hypothesis.success_criteria ?? []).map((criteria, index) => (
                          <Badge key={index} variant="outline" className="text-xs">
                            {criteria}
                          </Badge>
                        ))}
                      </div>
                    </div>
                    
                    <div>
                      <h4 className="text-sm font-medium mb-2">Связанные паттерны:</h4>
                      <div className="flex flex-wrap gap-1">
                        {(hypothesis.related_patterns ?? []).map((pattern, index) => (
                          <Badge key={index} variant="secondary" className="text-xs">
                            {pattern}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </div>
                  
                  <div className="mt-4 flex justify-end space-x-2">
                    <Button variant="outline" size="sm">
                      Просмотр
                    </Button>
                    <Button variant="outline" size="sm">
                      Редактировать
                    </Button>
                    <Button size="sm">
                      Запустить тест
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))
          )}
        </div>
      )}
    </div>
  )
}
