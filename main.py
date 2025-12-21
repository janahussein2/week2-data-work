from pathlib import Path
from src.bootcamp_data.config import make_paths
from src.bootcamp_data.io import read_orders_csv, write_parquet
# إعداد المسارات
paths = make_paths(Path('.').resolve())

# قراءة CSV
df = read_orders_csv(paths.raw / "orders.csv")

# كتابة Parquet
write_parquet(df, paths.processed / "orders.parquet")

print("orders.parquet has been created successfully!")