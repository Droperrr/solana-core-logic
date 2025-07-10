import argparse
import asyncio
import json
from pathlib import Path
from qc.etalon_generator import generate_from_reparse
from qc.diff_engine import diff_etalon_vs_enrich

async def validate_one(sig, raw_dir, enrich_dir, out_dir):
    raw_path = Path(raw_dir) / f"{sig}.raw.json"
    enrich_path = Path(enrich_dir) / f"{sig}.enrich.json"
    if not raw_path.exists() or not enrich_path.exists():
        return {"signature": sig, "error": "Missing raw or enrich file"}
    with open(raw_path, "r", encoding="utf-8") as f:
        raw_json = json.load(f)
    with open(enrich_path, "r", encoding="utf-8") as f:
        enrich_json = json.load(f)
    etalon = generate_from_reparse(raw_json)
    diff = diff_etalon_vs_enrich(etalon, enrich_json)
    # Сохраняем diff-отчёт
    out_path = Path(out_dir) / f"{sig}_diff.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(diff, f, ensure_ascii=False, indent=2)
    return {"signature": sig, "critical": diff["summary"]["critical"], "warning": diff["summary"]["warning"]}

async def main_async(args):
    with open(args.file, "r", encoding="utf-8") as f:
        sigs = [line.strip() for line in f if line.strip()]
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    tasks = [validate_one(sig, args.raw_dir, args.enrich_dir, args.out_dir) for sig in sigs]
    results = await asyncio.gather(*tasks)
    # Сохраняем summary
    with open(Path(args.out_dir) / "batch_summary.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Batch validation complete. Results saved to {args.out_dir}/batch_summary.json")

def main():
    parser = argparse.ArgumentParser(description="Batch QC validation for enrichment fixtures.")
    parser.add_argument("--file", type=str, required=True, help="File with list of signatures")
    parser.add_argument("--raw-dir", type=str, required=True, help="Directory with raw JSONs")
    parser.add_argument("--enrich-dir", type=str, required=True, help="Directory with enrichment JSONs")
    parser.add_argument("--out-dir", type=str, required=True, help="Directory to save diff reports")
    args = parser.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main() 