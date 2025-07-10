"use client"

import { useState } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Plus, TrendingUp, Database, Target, BarChart3 } from "lucide-react"
import { HypothesisManager } from "./components/hypothesis-manager"
import { ExperimentRunner } from "./components/experiment-runner"
import { ResultsAnalyzer } from "./components/results-analyzer"
import { FeatureExplorer } from "./components/feature-explorer"
import { GroupManager } from "./components/group-manager"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"

interface DashboardStats {
  totalHypotheses: number
  activeExperiments: number
  completedExperiments: number
  bestAccuracy: number
  totalFeatures: number
  tokensAnalyzed: number
}

export default function SolanaDashboard() {
  const [stats, setStats] = useState<DashboardStats>({
    totalHypotheses: 12,
    activeExperiments: 3,
    completedExperiments: 8,
    bestAccuracy: 0.847,
    totalFeatures: 24,
    tokensAnalyzed: 1547,
  })
  const [preDumpMode, setPreDumpMode] = useState(false)

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">Solana ML Pipeline</h1>
            <p className="text-slate-600 mt-1">Hypothesis Management & Experiment Tracking</p>
          </div>
          <div className="flex items-center space-x-4">
            <div className="flex items-center space-x-2">
              <Switch
                id="pre-dump-mode"
                checked={preDumpMode}
                onCheckedChange={setPreDumpMode}
              />
              <Label htmlFor="pre-dump-mode">Остановить сбор после первого дампа</Label>
            </div>
            <Button className="bg-purple-600 hover:bg-purple-700">
              <Plus className="w-4 h-4 mr-2" />
              New Hypothesis
            </Button>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4">
          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <Target className="w-5 h-5 text-purple-600" />
                <div>
                  <p className="text-sm text-slate-600">Hypotheses</p>
                  <p className="text-2xl font-bold">{stats.totalHypotheses}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <TrendingUp className="w-5 h-5 text-blue-600" />
                <div>
                  <p className="text-sm text-slate-600">Active</p>
                  <p className="text-2xl font-bold">{stats.activeExperiments}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <BarChart3 className="w-5 h-5 text-green-600" />
                <div>
                  <p className="text-sm text-slate-600">Completed</p>
                  <p className="text-2xl font-bold">{stats.completedExperiments}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <Target className="w-5 h-5 text-orange-600" />
                <div>
                  <p className="text-sm text-slate-600">Best Accuracy</p>
                  <p className="text-2xl font-bold">{(stats.bestAccuracy * 100).toFixed(1)}%</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <Database className="w-5 h-5 text-indigo-600" />
                <div>
                  <p className="text-sm text-slate-600">Features</p>
                  <p className="text-2xl font-bold">{stats.totalFeatures}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardContent className="p-4">
              <div className="flex items-center space-x-2">
                <Database className="w-5 h-5 text-teal-600" />
                <div>
                  <p className="text-sm text-slate-600">Tokens</p>
                  <p className="text-2xl font-bold">{stats.tokensAnalyzed}</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Main Content */}
        <Tabs defaultValue="hypotheses" className="space-y-4">
          <TabsList className="grid w-full grid-cols-5">
            <TabsTrigger value="hypotheses">Hypotheses</TabsTrigger>
            <TabsTrigger value="experiments">Experiments</TabsTrigger>
            <TabsTrigger value="results">Results</TabsTrigger>
            <TabsTrigger value="features">Features</TabsTrigger>
            <TabsTrigger value="groups">Groups</TabsTrigger>
          </TabsList>

          <TabsContent value="hypotheses">
            <HypothesisManager />
          </TabsContent>

          <TabsContent value="experiments">
            <ExperimentRunner />
          </TabsContent>

          <TabsContent value="results">
            <ResultsAnalyzer />
          </TabsContent>

          <TabsContent value="features">
            <FeatureExplorer />
          </TabsContent>

          <TabsContent value="groups">
            <GroupManager preDumpMode={preDumpMode} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}
