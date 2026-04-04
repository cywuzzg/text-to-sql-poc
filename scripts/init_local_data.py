"""Save seed Parquet files to a local directory (no MinIO required)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from text_to_sql.database.seed import generate_dataframes

DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data" / "local"


def init_local_data(data_dir: Path = DEFAULT_DATA_DIR) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)

    dataframes = generate_dataframes()
    for table_name, df in dataframes.items():
        out_path = data_dir / f"{table_name}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        print(f"  Wrote {len(df):>4} rows → {out_path}")

    print(f"\nSeed data written to: {data_dir}")
    print("Tables: users.parquet, products.parquet, orders.parquet, order_items.parquet")


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DATA_DIR
    init_local_data(target)
