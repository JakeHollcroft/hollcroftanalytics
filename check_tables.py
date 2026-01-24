import duckdb

DB="/var/data/housecall_data.duckdb"
con = duckdb.connect(DB)

print("DB:", DB)

for t in ["pricebook_services", "pricebook_materials"]:
    n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    print(f"{t}: {n}")

print("\npricebook_services sample:")
print(con.execute("""
    SELECT service_uuid, name, price, cost, duration, updated_at
    FROM pricebook_services
    ORDER BY updated_at DESC NULLS LAST
    LIMIT 10
""").df())

print("\npricebook_materials sample:")
print(con.execute("""
    SELECT *
    FROM pricebook_materials
    LIMIT 10
""").df())

print(con.execute("""
    SELECT key, value
    FROM metadata
    WHERE key IN ('last_refresh','last_window_start','last_window_end')
    ORDER BY key
""").df())

con.close()
