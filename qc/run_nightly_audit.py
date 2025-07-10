import os
import sys
import json
import psycopg2
import subprocess
import requests
from qc.qc_catalog import QCTag
from datetime import datetime, timedelta
from config import config

DB_CONN_STR = os.getenv("QC_DB_CONN", "dbname=postgres user=postgres password=postgres host=localhost port=5432")
REGRESSION_FIXTURES_DIR = "tests/fixtures/regression/"

# –ü–æ–ª—É—á–∏—Ç—å –≤—Å–µ —É–Ω–∏–∫–∞–ª—å–Ω—ã–µ qc_tags –∏–∑ enrich_errors –∑–∞ –ø–æ—Å–ª–µ–¥–Ω–∏–µ 24 —á–∞—Å–∞
QUERY = """
SELECT signature, additional_context
FROM transactions
WHERE block_time > extract(epoch from now() - interval '1 day')
  AND additional_context IS NOT NULL;
"""

def extract_qc_tags(additional_context):
    try:
        ctx = json.loads(additional_context)
        tags = set()
        for err in ctx.get("enrich_errors", []):
            for tag in err.get("qc_tags", []):
                tags.add(tag)
        return tags
    except Exception:
        return set()

def generate_fixture_for_new_error(signature: str, tag: str) -> bool:
    """–í—ã–∑—ã–≤–∞–µ—Ç —Å–∫—Ä–∏–ø—Ç qc/export_fixture.py –¥–ª—è —Å–æ–∑–¥–∞–Ω–∏—è –∞—Ä—Ç–µ—Ñ–∞–∫—Ç–æ–≤. –í–æ–∑–≤—Ä–∞—â–∞–µ—Ç True –µ—Å–ª–∏ —É—Å–ø–µ—à–Ω–æ."""
    print(f"INFO: –û–±–Ω–∞—Ä—É–∂–µ–Ω –Ω–æ–≤—ã–π —Ç–µ–≥ '{tag}'. –ì–µ–Ω–µ—Ä–∏—Ä—É—é —Ñ–∏–∫—Å—Ç—É—Ä—É –¥–ª—è —Å–∏–≥–Ω–∞—Ç—É—Ä—ã {signature}...")
    os.makedirs(REGRESSION_FIXTURES_DIR, exist_ok=True)
    try:
        result = subprocess.run([
            sys.executable, "-m", "qc.export_fixture",
            "--signature", signature,
            "--platform", "all",
            "--output-dir", REGRESSION_FIXTURES_DIR
        ], check=True, capture_output=True, text=True)
        print(f"INFO: –§–∏–∫—Å—Ç—É—Ä–∞ –¥–ª—è {signature} —É—Å–ø–µ—à–Ω–æ —Å–≥–µ–Ω–µ—Ä–∏—Ä–æ–≤–∞–Ω–∞.")
        print(f"STDOUT: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: –ù–µ —É–¥–∞–ª–æ—Å—å —Å–≥–µ–Ω–µ—Ä–∏—Ä–æ–≤–∞—Ç—å —Ñ–∏–∫—Å—Ç—É—Ä—É –¥–ª—è {signature}.")
        print(f"ERROR: –ö–æ–¥ –≤–æ–∑–≤—Ä–∞—Ç–∞: {e.returncode}")
        print(f"ERROR: STDOUT: {e.stdout}")
        print(f"ERROR: STDERR: {e.stderr}")
        return False
    except FileNotFoundError:
        print("ERROR: –ù–µ —É–¥–∞–ª–æ—Å—å –Ω–∞–π—Ç–∏ 'python'. –£–±–µ–¥–∏—Ç–µ—Å—å, —á—Ç–æ –æ–Ω –≤ –≤–∞—à–µ–º PATH.")
        return False

def create_jira_ticket(tag: str, signature: str, debug_info: str) -> str | None:
    """–°–æ–∑–¥–∞–µ—Ç –∑–∞–¥–∞—á—É –≤ Jira –¥–ª—è –Ω–æ–≤–æ–≥–æ QC-—Ç–µ–≥–∞. –í–æ–∑–≤—Ä–∞—â–∞–µ—Ç –∫–ª—é—á —Ç–∏–∫–µ—Ç–∞ –∏–ª–∏ None."""
    if not all([
        getattr(config, 'JIRA_SERVER_URL', None),
        getattr(config, 'JIRA_PROJECT_KEY', None),
        getattr(config, 'JIRA_USER_EMAIL', None),
        getattr(config, 'JIRA_API_TOKEN', None)
    ]):
        print("WARN: –ü–µ—Ä–µ–º–µ–Ω–Ω—ã–µ –¥–ª—è –∏–Ω—Ç–µ–≥—Ä–∞—Ü–∏–∏ —Å Jira –Ω–µ –Ω–∞—Å—Ç—Ä–æ–µ–Ω—ã. –ü—Ä–æ–ø—É—Å–∫ —Å–æ–∑–¥–∞–Ω–∏—è —Ç–∏–∫–µ—Ç–∞.")
        return None
    url = f"{config.JIRA_SERVER_URL}/rest/api/3/issue"
    auth = (config.JIRA_USER_EMAIL, config.JIRA_API_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    # Atlassian doc format for rich text
    description = {
        "type": "doc", "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": f"–ü—Ä–∏–º–µ—Ä —Ç—Ä–∞–Ω–∑–∞–∫—Ü–∏–∏: {signature}"}]},
            {"type": "codeBlock", "attrs": {"language": "json"}, "content": [{"type": "text", "text": debug_info}]}
        ]
    }
    data = {
        "fields": {
            "project": {"key": config.JIRA_PROJECT_KEY},
            "summary": f"[–ê–≤—Ç–æ–º–∞—Ç–∏—á–µ—Å–∫–∏] –û–±–Ω–∞—Ä—É–∂–µ–Ω –Ω–æ–≤—ã–π QC-—Ç–µ–≥: {tag}",
            "description": description,
            "issuetype": {"name": "Bug"}
        }
    }
    try:
        resp = requests.post(url, data=json.dumps(data), auth=auth, headers=headers, timeout=20)
        resp.raise_for_status()
        issue_key = resp.json()["key"]
        print(f"INFO: –£—Å–ø–µ—à–Ω–æ —Å–æ–∑–¥–∞–Ω —Ç–∏–∫–µ—Ç –≤ Jira: {issue_key}")
        return issue_key
    except requests.RequestException as e:
        print(f"ERROR: –ù–µ —É–¥–∞–ª–æ—Å—å —Å–æ–∑–¥–∞—Ç—å —Ç–∏–∫–µ—Ç –≤ Jira: {e}")
        return None

def send_slack_alert(message: str):
    """–û—Ç–ø—Ä–∞–≤–ª—è–µ—Ç —Å–æ–æ–±—â–µ–Ω–∏–µ –Ω–∞ URL –≤–µ–±-—Ö—É–∫–∞ Slack."""
    if not getattr(config, 'SLACK_WEBHOOK_URL', None):
        print("WARN: SLACK_WEBHOOK_URL –Ω–µ –Ω–∞—Å—Ç—Ä–æ–µ–Ω. –ü—Ä–æ–ø—É—Å–∫ –æ—Ç–ø—Ä–∞–≤–∫–∏ –≤ Slack.")
        return
    try:
        payload = {"text": message}
        response = requests.post(config.SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print("INFO: –û–ø–æ–≤–µ—â–µ–Ω–∏–µ –≤ Slack —É—Å–ø–µ—à–Ω–æ –æ—Ç–ø—Ä–∞–≤–ª–µ–Ω–æ.")
    except requests.RequestException as e:
        print(f"ERROR: –ù–µ —É–¥–∞–ª–æ—Å—å –æ—Ç–ø—Ä–∞–≤–∏—Ç—å –æ–ø–æ–≤–µ—â–µ–Ω–∏–µ –≤ Slack: {e}")

def main():
    conn = psycopg2.connect(DB_CONN_STR)
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    found_tags = set()
    tag_to_sig = {}
    tag_to_debug = {}
    for sig, ctx in rows:
        tags = extract_qc_tags(ctx)
        found_tags.update(tags)
        for tag in tags:
            if tag not in tag_to_sig:
                tag_to_sig[tag] = sig
                tag_to_debug[tag] = ctx
    known_tags = set(str(tag) for tag in QCTag)
    new_tags = found_tags - known_tags
    if new_tags:
        print(f"New QC tags found: {new_tags}")
        for tag in new_tags:
            sig = tag_to_sig[tag]
            debug_info = tag_to_debug[tag]
            print(f"  Example signature: {sig}")
            fixture_ok = generate_fixture_for_new_error(sig, tag)
            if fixture_ok:
                ticket_key = create_jira_ticket(tag, sig, debug_info)
                if ticket_key:
                    jira_url = f"{config.JIRA_SERVER_URL}/browse/{ticket_key}"
                    alert_message = (
                        f"üö® *–û–±–Ω–∞—Ä—É–∂–µ–Ω –Ω–æ–≤—ã–π QC-—Ç–µ–≥: `{tag}`*\n"
                        f"–ü—Ä–∏–º–µ—Ä —Ç—Ä–∞–Ω–∑–∞–∫—Ü–∏–∏: `{sig}`\n"
                        f"–°–æ–∑–¥–∞–Ω–∞ –∑–∞–¥–∞—á–∞ –≤ Jira: <{jira_url}|{ticket_key}>\n"
                        f"–†–µ–≥—Ä–µ—Å—Å–∏–æ–Ω–Ω–∞—è —Ñ–∏–∫—Å—Ç—É—Ä–∞ –∞–≤—Ç–æ–º–∞—Ç–∏—á–µ—Å–∫–∏ —Å–≥–µ–Ω–µ—Ä–∏—Ä–æ–≤–∞–Ω–∞."
                    )
                    send_slack_alert(alert_message)
                else:
                    print("WARN: –¢–∏–∫–µ—Ç –≤ Jira –Ω–µ –±—ã–ª —Å–æ–∑–¥–∞–Ω, Slack-–æ–ø–æ–≤–µ—â–µ–Ω–∏–µ –Ω–µ –æ—Ç–ø—Ä–∞–≤–ª–µ–Ω–æ.")
            else:
                print("WARN: –§–∏–∫—Å—Ç—É—Ä–∞ –Ω–µ —Å–æ–∑–¥–∞–Ω–∞, —Ç–∏–∫–µ—Ç –≤ Jira –∏ Slack-–æ–ø–æ–≤–µ—â–µ–Ω–∏–µ –Ω–µ –æ—Ç–ø—Ä–∞–≤–ª—è—é—Ç—Å—è.")
    else:
        print("No new QC tags found.")
    print(f"Total unique tags found: {len(found_tags)}")
    print(f"Known tags: {sorted(known_tags)}")
    print(f"All tags in DB: {sorted(found_tags)}")

if __name__ == "__main__":
    main() 