"use client"

import React, { useState, useEffect } from "react"
import { getFeatures } from "@/lib/api"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Search, BarChart3, Database, Eye } from "lucide-react"

interface Feature {
  name: string
  type: "numerical" | "categorical" | "boolean"
  category: "liquidity" | "timing" | "network" | "market"
  description: string
  importance: number
  correlation: number
  nullRate: number
  uniqueValues: number
  distribution: {
    min: number
    max: number
    mean: number
    median: number
    std: number
  }
}

export function FeatureExplorer() {
  const [features, setFeatures] = useState<Feature[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState("")
  const [categoryFilter, setCategoryFilter] = useState("all")
  const [typeFilter, setTypeFilter] = useState("all")

  useEffect(() => {
    const loadFeatures = async () => {
      try {
        setIsLoading(true)
        setError(null)
        const data = await getFeatures()
        setFeatures(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "An unknown error occurred")
        console.error("Ошибка при загрузке признаков:", err)
      } finally {
        setIsLoading(false)
      }
    }

    loadFeatures()
  }, [])

  const filteredFeatures = features.filter((feature) => {
    const matchesSearch =
      feature.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      feature.description.toLowerCase().includes(searchTerm.toLowerCase())
    const matchesCategory = categoryFilter === "all" || feature.category === categoryFilter
    const matchesType = typeFilter === "all" || feature.type === typeFilter

    return matchesSearch && matchesCategory && matchesType
  })

  const getCategoryColor = (category: string) => {
    switch (category) {
      case "liquidity":
        return "bg-blue-100 text-blue-800"
      case "timing":
        return "bg-purple-100 text-purple-800"
      case "network":
        return "bg-green-100 text-green-800"
      case "market":
        return "bg-orange-100 text-orange-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getTypeColor = (type: string) => {
    switch (type) {
      case "numerical":
        return "bg-indigo-100 text-indigo-800"
      case "categorical":
        return "bg-pink-100 text-pink-800"
      case "boolean":
        return "bg-teal-100 text-teal-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  const getCorrelationColor = (correlation: number) => {
    const abs = Math.abs(correlation)
    if (abs > 0.7) return "text-green-600"
    if (abs > 0.4) return "text-yellow-600"
    return "text-red-600"
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Feature Explorer</h2>
          <p className="text-slate-600">Explore and analyze feature characteristics</p>
        </div>
        <Button className="bg-blue-600 hover:bg-blue-700">
          <Database className="w-4 h-4 mr-2" />
          Generate New Features
        </Button>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="p-4">
          <div className="flex items-center space-x-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400 w-4 h-4" />
                <Input
                  placeholder="Search features..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={categoryFilter} onValueChange={setCategoryFilter}>
              <SelectTrigger className="w-40">
                <SelectValue placeholder="Category" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Categories</SelectItem>
                <SelectItem value="liquidity">Liquidity</SelectItem>
                <SelectItem value="timing">Timing</SelectItem>
                <SelectItem value="network">Network</SelectItem>
                <SelectItem value="market">Market</SelectItem>
              </SelectContent>
            </Select>
            <Select value={typeFilter} onValueChange={setTypeFilter}>
              <SelectTrigger className="w-40">
                <SelectValue placeholder="Type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                <SelectItem value="numerical">Numerical</SelectItem>
                <SelectItem value="categorical">Categorical</SelectItem>
                <SelectItem value="boolean">Boolean</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Loading and Error States */}
      {isLoading && (
        <div className="text-center py-8">
          <p className="text-lg">Загрузка признаков...</p>
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
        <>
          {/* Feature List */}
          <div className="grid gap-4">
            {filteredFeatures.map((feature) => (
              <Card key={feature.name} className="hover:shadow-md transition-shadow">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div>
                  <CardTitle className="text-lg font-mono">{feature.name}</CardTitle>
                  <CardDescription className="mt-1">{feature.description}</CardDescription>
                </div>
                <div className="flex items-center space-x-2">
                  <Badge className={getCategoryColor(feature.category)}>{feature.category}</Badge>
                  <Badge className={getTypeColor(feature.type)}>{feature.type}</Badge>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <Tabs defaultValue="stats" className="space-y-4">
                <TabsList className="grid w-full grid-cols-3">
                  <TabsTrigger value="stats">Statistics</TabsTrigger>
                  <TabsTrigger value="quality">Quality</TabsTrigger>
                  <TabsTrigger value="importance">Importance</TabsTrigger>
                </TabsList>

                <TabsContent value="stats">
                  {feature.type === "numerical" && (
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                      <div className="text-center p-3 bg-slate-50 rounded">
                        <p className="text-xs text-slate-600">MIN</p>
                        <p className="text-lg font-bold">{feature.distribution.min}</p>
                      </div>
                      <div className="text-center p-3 bg-slate-50 rounded">
                        <p className="text-xs text-slate-600">MAX</p>
                        <p className="text-lg font-bold">{feature.distribution.max}</p>
                      </div>
                      <div className="text-center p-3 bg-slate-50 rounded">
                        <p className="text-xs text-slate-600">MEAN</p>
                        <p className="text-lg font-bold">{feature.distribution.mean.toFixed(1)}</p>
                      </div>
                      <div className="text-center p-3 bg-slate-50 rounded">
                        <p className="text-xs text-slate-600">MEDIAN</p>
                        <p className="text-lg font-bold">{feature.distribution.median}</p>
                      </div>
                      <div className="text-center p-3 bg-slate-50 rounded">
                        <p className="text-xs text-slate-600">STD</p>
                        <p className="text-lg font-bold">{feature.distribution.std.toFixed(1)}</p>
                      </div>
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="quality">
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    <div className="text-center p-3 bg-slate-50 rounded">
                      <p className="text-xs text-slate-600">NULL RATE</p>
                      <p className="text-lg font-bold">{(feature.nullRate * 100).toFixed(1)}%</p>
                    </div>
                    <div className="text-center p-3 bg-slate-50 rounded">
                      <p className="text-xs text-slate-600">UNIQUE VALUES</p>
                      <p className="text-lg font-bold">{feature.uniqueValues.toLocaleString()}</p>
                    </div>
                    <div className="text-center p-3 bg-slate-50 rounded">
                      <p className="text-xs text-slate-600">CORRELATION</p>
                      <p className={`text-lg font-bold ${getCorrelationColor(feature.correlation)}`}>
                        {feature.correlation.toFixed(2)}
                      </p>
                    </div>
                  </div>
                </TabsContent>

                <TabsContent value="importance">
                  <div className="space-y-4">
                    <div>
                      <div className="flex justify-between text-sm mb-2">
                        <span>Feature Importance</span>
                        <span>{(feature.importance * 100).toFixed(1)}%</span>
                      </div>
                      <div className="w-full bg-slate-200 rounded-full h-3">
                        <div
                          className="bg-blue-600 h-3 rounded-full"
                          style={{ width: `${feature.importance * 100}%` }}
                        />
                      </div>
                    </div>
                    <div className="flex justify-end space-x-2">
                      <Button variant="outline" size="sm">
                        <Eye className="w-4 h-4 mr-1" />
                        View Distribution
                      </Button>
                      <Button variant="outline" size="sm">
                        <BarChart3 className="w-4 h-4 mr-1" />
                        Analyze
                      </Button>
                    </div>
                  </div>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>
        ))}
      </div>

          {filteredFeatures.length === 0 && (
            <Card>
              <CardContent className="p-8 text-center">
                <Database className="w-12 h-12 text-slate-400 mx-auto mb-4" />
                <p className="text-slate-600">Признаки не найдены по заданным критериям</p>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  )
}
