"""Interactive Text-to-SQL demo.

Usage:
    # Local mode (no MinIO required — reads data/local/*.parquet)
    python scripts/demo.py

    # MinIO mode
    python scripts/demo.py --minio
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from text_to_sql.pipeline import build_local_pipeline, build_pipeline

DEFAULT_LOCAL_DATA_DIR = Path(__file__).parent.parent / "data" / "local"

BANNER = """
╔══════════════════════════════════════════╗
║        Text-to-SQL POC  (電商場景)       ║
╚══════════════════════════════════════════╝

輸入自然語言查詢，系統自動生成 SQL 並執行。
輸入 'exit' 或 Ctrl+C 結束。

示例查詢：
  - 庫存不足 50 件的商品有哪些？
  - 最近銷售額最高的前 3 件商品是什麼？
  - 哪個地區的用戶消費總金額最高？
  - 列出所有已取消的訂單數量
"""


def _print_result(result) -> None:
    print(f"\n{'─'*60}")
    print(f"路由表：{result.route.tables}  (信心度: {result.route.confidence:.0%})")
    print(f"推理：{result.route.reasoning}")
    print(f"\n生成 SQL：\n{result.generated.sql}")
    print(f"\n說明：{result.generated.explanation}")
    print(f"\n執行引擎：{result.engine}")
    print(f"執行結果（{result.execution.row_count} 筆）：")

    if not result.execution.success:
        print(f"  [錯誤] {result.execution.error}")
        return

    if result.execution.csv_url:
        print(f"  結果已儲存至：{result.execution.csv_url}")
        return

    if result.execution.row_count == 0:
        print("  （無資料）")
        return

    # print table
    cols = result.execution.columns
    col_widths = [max(len(c), max((len(str(r[i])) for r in result.execution.rows), default=0)) for i, c in enumerate(cols)]
    header = "  " + "  ".join(c.ljust(w) for c, w in zip(cols, col_widths))
    print(header)
    print("  " + "  ".join("─" * w for w in col_widths))
    for row in result.execution.rows[:20]:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))
    if result.execution.row_count > 20:
        print(f"  ... (顯示前 20 筆，共 {result.execution.row_count} 筆)")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Text-to-SQL interactive demo")
    parser.add_argument(
        "--minio",
        action="store_true",
        help="Use MinIO backend (default: local Parquet files in data/local/)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_LOCAL_DATA_DIR,
        help="Local Parquet directory (only used without --minio)",
    )
    args = parser.parse_args()

    print(BANNER)

    if args.minio:
        pipeline = build_pipeline()
        print("模式：MinIO（S3 Parquet）\n")
    else:
        data_dir: Path = args.data_dir
        if not data_dir.exists():
            print(f"[提示] 找不到資料目錄：{data_dir}")
            print("請先執行：python scripts/init_local_data.py\n")
            sys.exit(1)
        pipeline = build_local_pipeline(data_dir=data_dir)
        print(f"模式：本地 Parquet（{data_dir}）\n")

    while True:
        try:
            query = input("查詢> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not query:
            continue
        if query.lower() in ("exit", "quit", "q"):
            print("Bye!")
            break

        try:
            result = pipeline.run(query)
            _print_result(result)
        except Exception as exc:
            print(f"\n[錯誤] {exc}\n")


if __name__ == "__main__":
    main()
