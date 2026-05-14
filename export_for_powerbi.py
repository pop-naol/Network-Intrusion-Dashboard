import duckdb
import os

db_path    = os.path.join('duck_db', 'network_intrusion.duckdb')
export_dir = os.path.join('Dashboard', 'powerbi_data')
os.makedirs(export_dir, exist_ok=True)

con = duckdb.connect(db_path)

views = [
    'v_dataset_overview',
    'v_attack_distribution',
    'v_top_attack_categories',
]

for view in views:
    df = con.execute(f'SELECT * FROM {view}').df()
    out = os.path.join(export_dir, f'{view}.csv')
    df.to_csv(out, index=False)
    print(f'Exported {view} -> {out}  ({len(df)} rows)')

# Also export the full summary table for flexible slicing in Power BI
df = con.execute('SELECT * FROM attack_summary').df()
out = os.path.join(export_dir, 'attack_summary.csv')
df.to_csv(out, index=False)
print(f'Exported attack_summary -> {out}  ({len(df)} rows)')

con.close()
print('\nAll exports done. Open Power BI and load from:', export_dir)
