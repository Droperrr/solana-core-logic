import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st
from pathlib import Path
import re
import pandas as pd
import db.db_manager as dbm
from analysis.qc_check import (
    check_missing_fields,
    check_duplicate_signatures,
    check_orphan_token_transfers,
    check_transactions_without_transfers,
    check_enrich_errors,
    check_enrich_context_by_type,
)
import json
from collections import Counter
import subprocess
import tempfile
import os

REPORTS_DIR = Path("logs/reports/")

st.set_page_config(layout="wide", page_title="Solana QC Dashboard")

st.sidebar.title("Навигация")
page = st.sidebar.radio("Выберите раздел", ["Обзор БД", "Аудит QC-тегов", "Инструменты"])

@st.cache_data(ttl=600)
def load_qc_data():
    try:
        dbm.initialize_engine()
        engine = dbm.get_engine()
        missing_fields = check_missing_fields(engine)
        duplicates = check_duplicate_signatures(engine)
        orphans = check_orphan_token_transfers(engine)
        tx_without_transfers = check_transactions_without_transfers(engine)
        enrich_errors = check_enrich_errors(engine)
        by_type = check_enrich_context_by_type(engine)
        return {
            'missing_fields': missing_fields,
            'duplicates': duplicates,
            'orphans': orphans,
            'tx_without_transfers': tx_without_transfers,
            'enrich_errors': enrich_errors,
            'by_type': by_type,
        }
    except Exception as e:
        st.error(f"Ошибка при загрузке данных из БД: {e}")
        return None

@st.cache_data(ttl=600)
def load_enrich_qc_log(log_path="logs/enrich_qc.log"):
    records = []
    path = Path(log_path)
    if not path.exists():
        return pd.DataFrame()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                # Ожидаем поля: signature, protocol, qc_tags, ...
                records.append(rec)
            except Exception:
                continue  # Пропускаем невалидные строки
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    # qc_tags могут быть списком, нормализуем для фильтрации
    df["qc_tags"] = df["qc_tags"].apply(lambda x: x if isinstance(x, list) else ([x] if pd.notnull(x) else []))
    return df

def render_db_overview_page():
    st.title("Общее состояние Базы Данных")
    data = load_qc_data()
    if data is None:
        st.error("Не удалось загрузить данные из БД. Проверьте подключение и параметры.")
        return
    missing_fields = data['missing_fields']
    duplicates = data['duplicates']
    orphans = data['orphans']
    tx_without_transfers = data['tx_without_transfers']
    enrich_errors = data['enrich_errors']
    by_type = data['by_type']

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Всего транзакций", int(missing_fields['total'][0]))
    col2.metric("Дубликаты signature", len(duplicates))
    col3.metric("Orphan token_transfers", int(orphans['orphan_transfers'][0]))
    col4.metric("Tx без transfers", int(tx_without_transfers['tx_without_transfers'][0]))

    st.subheader("Ошибки enrich_errors (top 20)")
    st.dataframe(enrich_errors['errors'].head(20))

    st.subheader("Дубликаты signature")
    if not duplicates.empty:
        st.dataframe(duplicates)
    else:
        st.success("Дубликаты не найдены.")

    st.subheader("Распределение enrichment по типу транзакции")
    st.dataframe(by_type)
    st.bar_chart(by_type.set_index('transaction_type')[['with_raydium_enrich', 'with_jupiter_enrich', 'with_orca_enrich']])

def render_qc_audit_page():
    st.title("Аудит QC-тегов: проблемные транзакции")
    df = load_enrich_qc_log()
    if df.empty:
        st.warning("Лог enrich_qc.log пуст или не найден.")
        return
    # Собираем уникальные теги и протоколы
    all_tags = sorted({tag for tags in df["qc_tags"] for tag in tags})
    all_protocols = sorted(df["protocol"].dropna().unique())
    # --- Новое: фильтр по дате и сигнатуре ---
    date_col = None
    if "block_time" in df.columns:
        df["block_time"] = pd.to_datetime(df["block_time"], errors="coerce")
        min_date = df["block_time"].min().date() if df["block_time"].notnull().any() else None
        max_date = df["block_time"].max().date() if df["block_time"].notnull().any() else None
        date_col = st.date_input("Дата транзакции (UTC, optional)", [], min_value=min_date, max_value=max_date)
    sig_search = st.text_input("Поиск по сигнатуре", "")
    # --- Фильтры по тегу и протоколу ---
    selected_tags = st.multiselect("QC-теги для фильтрации", all_tags, default=all_tags)
    selected_protocols = st.multiselect("Протоколы", all_protocols, default=all_protocols)
    # --- Фильтрация ---
    mask = df["protocol"].isin(selected_protocols) & df["qc_tags"].apply(lambda tags: any(tag in selected_tags for tag in tags))
    filtered = df[mask].copy()
    if date_col and len(date_col) > 0 and "block_time" in filtered.columns:
        filtered = filtered[filtered["block_time"].dt.date.isin(date_col)]
    if sig_search:
        filtered = filtered[filtered["signature"].str.contains(sig_search)]
    st.write(f"Найдено {len(filtered)} проблемных транзакций.")
    # --- Новое: summary по QC-тегам ---
    st.subheader("Сводка по QC-тегам")
    tag_counter = Counter(tag for tags in filtered["qc_tags"] for tag in tags)
    if tag_counter:
        summary_df = pd.DataFrame.from_dict(tag_counter, orient="index", columns=["count"])
        summary_df["percent"] = summary_df["count"] / summary_df["count"].sum() * 100
        # Топ протоколы по каждому тегу
        top_protocols = {}
        for tag in summary_df.index:
            protos = filtered[filtered["qc_tags"].apply(lambda tags: tag in tags)]["protocol"].value_counts()
            top_protocols[tag] = ", ".join([f"{p}({c})" for p, c in protos.head(3).items()])
        summary_df["top_protocols"] = summary_df.index.map(top_protocols)
        st.dataframe(summary_df.sort_values("count", ascending=False))
        st.bar_chart(summary_df["count"])
    # --- Экспорт ---
    st.download_button("Экспорт в CSV", filtered.to_csv(index=False).encode("utf-8"), "qc_errors.csv", "text/csv")
    st.download_button("Экспорт в JSON", filtered.to_json(orient='records', force_ascii=False), "qc_errors.json", "application/json")
    # --- Таблица с expander для enrich_errors ---
    st.subheader("Проблемные транзакции")
    for idx, row in filtered.iterrows():
        with st.expander(f"{row.get('signature', 'no_sig')} | {row.get('protocol', 'no_proto')} | QC: {', '.join(row['qc_tags'])}"):
            enrich_errors = row.get('enrich_errors', [])
            if isinstance(enrich_errors, str):
                try:
                    enrich_errors = json.loads(enrich_errors)
                except Exception:
                    enrich_errors = []
            if isinstance(enrich_errors, dict):
                enrich_errors = [enrich_errors]
            if not enrich_errors:
                st.info("Нет enrich_errors для этой транзакции.")
            else:
                for i, err in enumerate(enrich_errors):
                    st.markdown(f"**Enrich error #{i+1}:**")
                    st.write(f"QC-теги: {err.get('qc_tags')}")
                    st.write(f"Сообщения: {err.get('messages')}")
                    st.json(err.get('debug_context', {}))
    if filtered.empty:
        st.info("Нет транзакций, соответствующих выбранным фильтрам.")

def render_tools_page():
    st.title("Интерактивные инструменты QC")
    st.header("Валидация транзакции по сигнатуре")
    sig = st.text_input("Введите сигнатуру транзакции для валидации", key="validate_sig")
    raw_path = st.text_input("Путь к raw.json (опционально)", value="", key="validate_raw")
    enrich_path = st.text_input("Путь к enrich.json (опционально)", value="", key="validate_enrich")
    if st.button("Запустить валидацию"):
        if not sig:
            st.warning("Введите сигнатуру.")
        else:
            with st.spinner("Выполняется валидация..."):
                args = ["python", "-m", "qc.validate_transaction", "--signature", sig, "--format", "json"]
                if raw_path:
                    args.extend(["--raw", raw_path])
                if enrich_path:
                    args.extend(["--enrich", enrich_path])
                result = subprocess.run(args, capture_output=True, text=True)
                if result.stderr:
                    st.error(f"Ошибка: {result.stderr}")
                else:
                    try:
                        diff = json.loads(result.stdout)
                        st.success("Валидация завершена. См. отчет ниже.")
                        st.write(f"CRITICAL: {diff['summary']['critical']}  WARNING: {diff['summary']['warning']}  INFO: {diff['summary']['info']}")
                        for d in diff["diffs"]:
                            color = "red" if d["severity"] == "CRITICAL" else ("orange" if d["severity"] == "WARNING" else "gray")
                            st.markdown(f"<span style='color:{color};font-weight:bold'>[{d['severity']}] {d['field']}</span>", unsafe_allow_html=True)
                            st.markdown(f"<span style='color:{color}'>Etalon: {d['etalon']}</span>", unsafe_allow_html=True)
                            st.markdown(f"<span style='color:{color}'>Enrich: {d['enrich']}</span>", unsafe_allow_html=True)
                            st.markdown("---")
                        with st.expander("Полный JSON diff-отчета"):
                            st.json(diff)
                    except Exception as e:
                        st.error(f"Ошибка парсинга JSON: {e}")
    st.header("Экспорт фикстуры для тестов")
    sig2 = st.text_input("Сигнатура для экспорта фикстуры", key="export_sig")
    platform = st.selectbox("Платформа", ["raydium_enrich", "jupiter_enrich", "orca_enrich"], key="export_platform")
    if st.button("Экспортировать фикстуру"):
        if not sig2:
            st.warning("Введите сигнатуру для экспорта.")
        else:
            with st.spinner("Экспорт фикстуры..."):
                with tempfile.TemporaryDirectory() as tmpdir:
                    out_path = os.path.join(tmpdir, f"{sig2}_{platform}.json")
                    args = ["python", "-m", "qc.export_fixture", "--signature", sig2, "--platform", platform, "--output-dir", tmpdir]
                    result = subprocess.run(args, capture_output=True, text=True)
                    if result.stderr:
                        st.error(f"Ошибка: {result.stderr}")
                    elif not os.path.exists(out_path):
                        st.error("Файл фикстуры не был создан.")
                    else:
                        with open(out_path, "rb") as f:
                            st.download_button(
                                label=f"Скачать фикстуру {sig2}_{platform}.json",
                                data=f,
                                file_name=f"{sig2}_{platform}.json",
                                mime="application/json"
                            )
                        st.success("Фикстура успешно сгенерирована и готова к скачиванию.")

if page == "Обзор БД":
    render_db_overview_page()
elif page == "Аудит QC-тегов":
    render_qc_audit_page()
elif page == "Инструменты":
    render_tools_page()

# Список доступных отчётов
reports = sorted(REPORTS_DIR.glob("qc_report_*.md"), reverse=True)
if not reports:
    st.warning("Нет доступных QC-отчётов. Сначала запустите run_nightly_audit.py.")
    st.stop()

selected = st.selectbox("Выберите отчёт:", [r.name for r in reports])
try:
    with open(REPORTS_DIR / selected, "r", encoding="utf-8") as f:
        content = f.read()
    # Парсим QC-теги и статистику
    st.markdown(content)
except Exception as e:
    st.error(f"Ошибка при чтении отчёта {selected}: {e}")
    st.stop()

# (MVP) Можно добавить фильтры, поиск по тегам, визуализацию bar chart и т.д. 