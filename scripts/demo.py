"""Interactive Text-to-SQL demo.

Usage:
    # Local mode (no MinIO required — reads data/local/*.parquet)
    python scripts/demo.py

    # DuckDB file mode (reads database/ecommerce.duckdb)
    python scripts/demo.py --duckdb

    # MinIO mode
    python scripts/demo.py --minio
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from text_to_sql.pipeline import build_duckdb_file_pipeline, build_local_pipeline, build_pipeline

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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Text-to-SQL interactive demo")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--minio",
        action="store_true",
        default=False,
        help="Use MinIO backend (S3 Parquet)",
    )
    mode.add_argument(
        "--duckdb",
        action="store_true",
        default=False,
        help="Use DuckDB file backend (database/ecommerce.duckdb)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_LOCAL_DATA_DIR,
        help="Local Parquet directory (only used without --minio / --duckdb)",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    print(BANNER)

    if args.minio:
        pipeline = build_pipeline()
        print("模式：MinIO（S3 Parquet）\n")

    elif args.duckdb:
        from text_to_sql.config import DUCKDB_PATH
        db_path = Path(DUCKDB_PATH)
        if not db_path.exists():
            print(f"[提示] 找不到 DuckDB 檔案：{db_path}")
            print("請先執行：python scripts/init_duckdb.py\n")
            sys.exit(1)
        pipeline = build_duckdb_file_pipeline()
        print(f"模式：DuckDB 檔案（{db_path}）\n")

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
