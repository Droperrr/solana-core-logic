import subprocess
import datetime
import os

LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
QC_SCRIPT = "analysis/qc_check.py"
QC_REPORT = os.path.join(LOG_DIR, 'qc_report.log')
QC_ALERTS = os.path.join(LOG_DIR, 'qc_alerts.log')

# Запуск QC-скрипта и сбор вывода
result = subprocess.run(["python", QC_SCRIPT], capture_output=True, text=True)
qc_output = result.stdout

with open(QC_REPORT, "w", encoding="utf-8") as f:
    f.write(qc_output)

# Примитивный анализ на критические ошибки
alerts = []
if "enrich_errors" in qc_output or "orphan_transfers" in qc_output or "missing_enrich" in qc_output:
    alerts.append("[ALERT] Обнаружены enrich_errors, orphan-записи или пропуски enrichment!")
    # Можно добавить более детальный парсинг и условия

if alerts:
    with open(QC_ALERTS, "w", encoding="utf-8") as f:
        for alert in alerts:
            f.write(alert + "\n")
    print("[QC ALERT] Критические ошибки обнаружены! См. файл:", QC_ALERTS)
else:
    print("[QC] Критических ошибок не обнаружено. См. отчёт:", QC_REPORT) 