"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Separator } from "@/components/ui/separator"
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog"
import { Drawer, DrawerContent, DrawerDescription, DrawerHeader, DrawerTitle, DrawerTrigger } from "@/components/ui/drawer"
import { Plus, Trash2, Users, Calendar, FileText, Activity } from "lucide-react"
import { listTokenGroups, getGroupDetails, createTokenGroup, deleteTokenGroup } from "@/lib/api"
import GroupDetailPanel from "./group-detail-panel"

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

interface GroupManagerProps {
  preDumpMode: boolean;
}

export function GroupManager({ preDumpMode }: GroupManagerProps) {
  const [groups, setGroups] = useState<TokenGroup[]>([])
  const [selectedGroup, setSelectedGroup] = useState<TokenGroupDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [creating, setCreating] = useState(false)
  
  // Состояние для Drawer с прогрессом
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedGroupForProgress, setSelectedGroupForProgress] = useState<string | null>(null)
  
  // Форма для создания новой группы
  const [newGroupName, setNewGroupName] = useState("")
  const [newGroupTokens, setNewGroupTokens] = useState("")
  
  // Загрузка списка групп
  const loadGroups = async () => {
    try {
      setLoading(true)
      const groupsList = await listTokenGroups()
      setGroups(groupsList)
    } catch (error) {
      console.error("Ошибка загрузки групп:", error)
      alert(`Ошибка загрузки групп: ${error}`)
    } finally {
      setLoading(false)
    }
  }

  // Загрузка деталей группы
  const loadGroupDetails = async (groupName: string) => {
    try {
      const details = await getGroupDetails(groupName)
      setSelectedGroup(details)
    } catch (error) {
      console.error("Ошибка загрузки деталей группы:", error)
      alert(`Ошибка загрузки деталей группы: ${error}`)
    }
  }

  // Открытие панели прогресса для группы
  const openProgressPanel = (groupName: string) => {
    setSelectedGroupForProgress(groupName)
    setDrawerOpen(true)
  }

  // Создание новой группы
  const handleCreateGroup = async () => {
    if (!newGroupName.trim()) {
      alert("Введите имя группы")
      return
    }

    if (!newGroupTokens.trim()) {
      alert("Введите список токенов")
      return
    }

    try {
      setCreating(true)
      
      // Парсим токены (по строкам)
      const tokens = newGroupTokens
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0)

      if (tokens.length === 0) {
        alert("Нет валидных токенов для создания группы")
        return
      }

      const response = await createTokenGroup(newGroupName.trim(), tokens)
      
      alert(`Группа успешно создана!\n\nИмя: ${response.group_name}\nТокенов: ${tokens.length}`)
      
      // Очищаем форму
      setNewGroupName("")
      setNewGroupTokens("")
      
      // Перезагружаем список групп
      await loadGroups()
      
    } catch (error) {
      console.error("Ошибка создания группы:", error)
      alert(`Ошибка создания группы: ${error}`)
    } finally {
      setCreating(false)
    }
  }

  // Удаление группы
  const handleDeleteGroup = async (groupName: string) => {
    try {
      await deleteTokenGroup(groupName)
      alert(`Группа "${groupName}" успешно удалена`)
      
      // Если удалили выбранную группу, очищаем детали
      if (selectedGroup?.name === groupName) {
        setSelectedGroup(null)
      }
      
      // Перезагружаем список групп
      await loadGroups()
      
    } catch (error) {
      console.error("Ошибка удаления группы:", error)
      alert(`Ошибка удаления группы: ${error}`)
    }
  }

  // Форматирование даты
  const formatDate = (timestamp: number) => {
    return new Date(timestamp * 1000).toLocaleString()
  }

  // Загружаем группы при загрузке компонента
  useEffect(() => {
    loadGroups()
  }, [])

  return (
    <div className="space-y-6">
      {/* Заголовок */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Управление группами токенов</h2>
          <p className="text-slate-600">Создавайте и управляйте группами токенов для целевого анализа</p>
        </div>
        <Badge variant="outline" className="px-3 py-1">
          <Users className="w-4 h-4 mr-1" />
          {groups.length} групп
        </Badge>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Левая панель: Список групп */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <FileText className="w-5 h-5 mr-2" />
              Существующие группы
            </CardTitle>
            <CardDescription>
              Выберите группу для просмотра деталей
            </CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <p className="text-center text-slate-500 py-4">Загрузка...</p>
            ) : groups.length === 0 ? (
              <p className="text-center text-slate-500 py-4">
                Пока нет созданных групп.<br />
                Создайте первую группу справа.
              </p>
            ) : (
              <div className="space-y-2 max-h-96 overflow-y-auto">
                {groups.map((group) => (
                  <div
                    key={group.name}
                    className={`p-3 border rounded-lg transition-colors border-gray-200 hover:border-gray-300`}
                  >
                    <div className="flex items-center justify-between">
                      <div 
                        className="cursor-pointer flex-1"
                        onClick={() => openProgressPanel(group.name)}
                      >
                        <p className="font-medium text-blue-600 hover:text-blue-800">{group.name}</p>
                        <p className="text-sm text-slate-500">
                          {group.token_count} токенов • Нажмите для просмотра прогресса
                        </p>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => loadGroupDetails(group.name)}
                          className="text-gray-600 hover:text-gray-800"
                          title="Показать детали"
                        >
                          <FileText className="w-4 h-4" />
                        </Button>
                        <Badge variant="secondary" className="text-xs">
                          {(group.file_size / 1024).toFixed(1)} KB
                        </Badge>
                        <AlertDialog>
                          <AlertDialogTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="text-red-600 hover:text-red-800"
                              onClick={(e) => e.stopPropagation()}
                            >
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </AlertDialogTrigger>
                          <AlertDialogContent>
                            <AlertDialogHeader>
                              <AlertDialogTitle>Удалить группу?</AlertDialogTitle>
                              <AlertDialogDescription>
                                Вы действительно хотите удалить группу "{group.name}"? 
                                Это действие нельзя отменить.
                              </AlertDialogDescription>
                            </AlertDialogHeader>
                            <AlertDialogFooter>
                              <AlertDialogCancel>Отмена</AlertDialogCancel>
                              <AlertDialogAction
                                onClick={() => handleDeleteGroup(group.name)}
                                className="bg-red-600 hover:bg-red-700"
                              >
                                Удалить
                              </AlertDialogAction>
                            </AlertDialogFooter>
                          </AlertDialogContent>
                        </AlertDialog>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Правая панель: Детали группы или форма создания */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              {selectedGroup ? (
                <>
                  <Users className="w-5 h-5 mr-2" />
                  Детали группы: {selectedGroup.name}
                </>
              ) : (
                <>
                  <Plus className="w-5 h-5 mr-2" />
                  Создать новую группу
                </>
              )}
            </CardTitle>
            <CardDescription>
              {selectedGroup 
                ? `${selectedGroup.token_count} токенов • Создана ${formatDate(selectedGroup.created_at)}`
                : "Введите имя группы и список токенов"
              }
            </CardDescription>
          </CardHeader>
          <CardContent>
            {selectedGroup ? (
              /* Отображение деталей выбранной группы */
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <p className="font-medium text-slate-700">Создана:</p>
                    <p className="text-slate-600">{formatDate(selectedGroup.created_at)}</p>
                  </div>
                  <div>
                    <p className="font-medium text-slate-700">Изменена:</p>
                    <p className="text-slate-600">{formatDate(selectedGroup.modified_at)}</p>
                  </div>
                </div>
                
                <Separator />
                
                <div>
                  <p className="font-medium text-slate-700 mb-2">
                    Список токенов ({selectedGroup.token_count}):
                  </p>
                  <div className="bg-slate-50 rounded-lg p-3 max-h-60 overflow-y-auto">
                    <code className="text-sm">
                      {selectedGroup.tokens.join('\n')}
                    </code>
                  </div>
                </div>
                
                <Button 
                  variant="outline" 
                  onClick={() => setSelectedGroup(null)}
                  className="w-full"
                >
                  Создать новую группу
                </Button>
              </div>
            ) : (
              /* Форма создания новой группы */
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">
                    Имя группы
                  </label>
                  <Input
                    placeholder="Например: Сумма 1659.0 SOL"
                    value={newGroupName}
                    onChange={(e) => setNewGroupName(e.target.value)}
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-2">
                    Список токенов (по одному на строку)
                  </label>
                  <Textarea
                    placeholder={`EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
So11111111111111111111111111111111111111112
4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R`}
                    rows={10}
                    value={newGroupTokens}
                    onChange={(e) => setNewGroupTokens(e.target.value)}
                    className="font-mono text-sm"
                  />
                  <p className="text-xs text-slate-500 mt-1">
                    Вставьте адреса токенов, каждый на новой строке
                  </p>
                </div>
                
                <Button 
                  onClick={handleCreateGroup}
                  disabled={creating}
                  className="w-full"
                >
                  {creating ? "Создание..." : "Создать группу"}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Drawer для панели прогресса */}
      <Drawer open={drawerOpen} onOpenChange={setDrawerOpen}>
        <DrawerContent className="max-h-[85vh]">
          <DrawerHeader>
            <DrawerTitle className="flex items-center gap-2">
              <Activity className="w-5 h-5" />
              Центр управления сбором данных
            </DrawerTitle>
            <DrawerDescription>
              Отслеживайте прогресс и управляйте сбором транзакций для группы токенов
            </DrawerDescription>
          </DrawerHeader>
          <div className="px-4 pb-4 overflow-y-auto">
            {selectedGroupForProgress && (
              <GroupDetailPanel groupName={selectedGroupForProgress} preDumpMode={preDumpMode} />
            )}
          </div>
        </DrawerContent>
      </Drawer>
    </div>
  )
} 