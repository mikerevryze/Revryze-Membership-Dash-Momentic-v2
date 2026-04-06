import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_private_key():
    pem_str = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
    pem_str = pem_str.replace("\\n", "\n").strip()
    if not pem_str.startswith("-----"):
        pem_str = pem_str.replace(" ", "\n")
        pem_str = f"-----BEGIN PRIVATE KEY-----\n{pem_str}\n-----END PRIVATE KEY-----"
    pk = serialization.load_pem_private_key(pem_str.encode(), password=None, backend=default_backend())
    return pk.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption())

pk_bytes = load_private_key()
conn = snowflake.connector.connect(
    account="VSC78986.us-east-1", user="MIKEPRINCE", private_key=pk_bytes,
    database="REVRYZE", warehouse="DASHBOARD_WH",
)
cur = conn.cursor()

# Check LOCATION_MAP DDL
print("=== LOCATION_MAP DDL ===")
cur.execute("SELECT GET_DDL('VIEW', 'REVRYZE.ANALYTICS.LOCATION_MAP')")
print(cur.fetchone()[0])

# Check GHL_MEMBERSHIPS DDL fully
print("\n=== GHL_MEMBERSHIPS DDL ===")
cur.execute("SELECT GET_DDL('VIEW', 'REVRYZE.ANALYTICS.GHL_MEMBERSHIPS')")
print(cur.fetchone()[0])

# Check CORE.LOCATIONS
print("\n=== CORE.LOCATIONS contents ===")
cur.execute("SELECT * FROM REVRYZE.CORE.LOCATIONS")
cols = [d[0] for d in cur.description]
print(f"Columns: {cols}")
for r in cur.fetchall():
    print(f"  {r}")

# Check all tables in RAW schema
print("\n=== All tables in RAW ===")
cur.execute("SHOW TABLES IN SCHEMA REVRYZE.RAW")
for r in cur.fetchall():
    print(f"  {r[1]}")

# Check columns with PIPELINE_NAME or LOCATION in RAW tables
print("\n=== RAW tables with PIPELINE_NAME ===")
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM REVRYZE.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'RAW' AND COLUMN_NAME LIKE '%PIPELINE%'
""")
for r in cur.fetchall():
    print(f"  {r[0]}.{r[1]}")

# Check for 'Westlake' in GHL source tables
print("\n=== Checking RAW GHL tables for 'Westlake' ===")
cur.execute("""
    SELECT TABLE_NAME
    FROM REVRYZE.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'RAW' AND COLUMN_NAME = 'PIPELINE_NAME'
""")
for r in cur.fetchall():
    tbl = r[0]
    cur.execute(f"SELECT COUNT(*) FROM REVRYZE.RAW.{tbl} WHERE PIPELINE_NAME = 'Westlake'")
    cnt = cur.fetchone()[0]
    print(f"  RAW.{tbl}: {cnt} rows with PIPELINE_NAME='Westlake'")
    if cnt > 0:
        cur.execute(f"SELECT DISTINCT PIPELINE_NAME FROM REVRYZE.RAW.{tbl}")
        print(f"    All pipeline names: {[x[0] for x in cur.fetchall()]}")

# Also check CORE tables  
print("\n=== All tables in CORE ===")
cur.execute("SHOW TABLES IN SCHEMA REVRYZE.CORE")
for r in cur.fetchall():
    print(f"  {r[1]}")

# Check MODELED tables
print("\n=== All tables in MODELED ===")
cur.execute("SHOW TABLES IN SCHEMA REVRYZE.MODELED")
for r in cur.fetchall():
    print(f"  {r[1]}")

conn.close()
