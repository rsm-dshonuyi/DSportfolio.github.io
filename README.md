    # Upload the CSV
    cs.execute(f"PUT 'file://{CSV_OUT}' @pgstage OVERWRITE=TRUE AUTO_COMPRESS=FALSE")

    # Create the table from the file’s inferred schema
    cs.execute("""
        CREATE OR REPLACE TABLE supplier_case USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION=>'@pgstage/supplier_case.csv',
              FILE_FORMAT=>'csv_std'
            )
          )
        )
    """)

    # Load the data
    cs.execute("""
        COPY INTO supplier_case
        FROM @pgstage/supplier_case.csv
        FILE_FORMAT=(FORMAT_NAME=csv_std)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        ON_ERROR='CONTINUE'
    """)

    cs.execute("SELECT COUNT(*) FROM supplier_case")
    print("row_count:", cs.fetchone()[0])

#Number 7
def _show(cs, sql):
    cs.execute(sql); return cs.fetchall()

def find_relation_fqn(cs, name_upper: str):
    """Find first DB.SCHEMA.OBJECT (TABLE or VIEW) named name_upper (case-insensitive)."""
    for db in [r[1] for r in _show(cs, "SHOW DATABASES")]:
        try:
            for sch in [r[1] for r in _show(cs, f"SHOW SCHEMAS IN DATABASE {db}")]:
                try:
                    for t in [r[1] for r in _show(cs, f"SHOW TABLES IN SCHEMA {db}.{sch}")]:
                        if t.upper() == name_upper.upper():
                            return f"{db}.{sch}.{t}"
                except: pass
                try:
                    for v in [r[1] for r in _show(cs, f"SHOW VIEWS IN SCHEMA {db}.{sch}")]:
                        if v.upper() == name_upper.upper():
                            return f"{db}.{sch}.{v}"
                except: pass
        except: pass
    return None

def desc_relation(cs, fqn: str):
    """DESC TABLE first; fallback to DESC VIEW."""
    try:
        cs.execute(f"DESC TABLE {fqn}"); return cs.fetchall()
    except:
        cs.execute(f"DESC VIEW {fqn}");  return cs.fetchall()

def pick_col(cs, fqn: str, candidates):
    cols = {r[0].upper() for r in desc_relation(cs, fqn)}
    for c in candidates:
        if c in cols: return c
    return next(iter(cols))  # fallback to any column to fail fast later

# --- REBUILD WEATHER PIPELINE WITH 5-DIGIT ZIP NORMALIZATION ---

with conn.cursor() as cs:
    # 7a) Supplier ZIPs (strict 5-digit from any ZIP+4 or messy strings)
    cs.execute("""
        CREATE OR REPLACE VIEW VW_SUPPLIER_ZIPS AS
        SELECT DISTINCT
            LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', ''), 1, 5), 5, '0') AS ZIP
        FROM supplier_case
        WHERE "postalpostalcode" IS NOT NULL
          AND REGEXP_REPLACE(TRIM("postalpostalcode"), '[^0-9]', '') <> ''
    """)

    # ZCTA centroids unchanged
    cs.execute("""
        CREATE OR REPLACE VIEW VW_ZIP_LATLON AS
        SELECT
          LPAD(TRIM(zcta5), 5, '0')        AS ZIP,
          TRY_TO_DOUBLE(centroid_lat)      AS ZIP_LAT,
          TRY_TO_DOUBLE(centroid_lon)      AS ZIP_LON
        FROM zcta_2021
        WHERE zcta5 IS NOT NULL
    """)

    # Nearest station per ZIP
    stations_fqn   = find_relation_fqn(cs, "NOAA_WEATHER_STATION_INDEX")
    timeseries_fqn = find_relation_fqn(cs, "NOAA_WEATHER_METRICS_TIMESERIES")
    if not stations_fqn or not timeseries_fqn:
        raise RuntimeError("NOAA Marketplace objects not found/authorized. Ensure subscription & privileges, then re-run.")

    date_col = pick_col(cs, timeseries_fqn, ("DATE","OBSERVATION_DATE","DAY","OBS_DATE"))
    high_col = pick_col(cs, timeseries_fqn, ("TMAX","MAX_TEMP","DAILY_MAX_TEMPERATURE","TMAX_F","TMAX_C"))

    cs.execute("""
        CREATE OR REPLACE VIEW VW_ZIP_STATION_NEAREST AS
        WITH cand AS (
          SELECT
            zl.ZIP,
            si.NOAA_WEATHER_STATION_ID,
            2 * 6371 * ASIN(SQRT(
              POWER(SIN(RADIANS((zl.ZIP_LAT - TRY_TO_DOUBLE(si.LATITUDE))/2)), 2) +
              COS(RADIANS(zl.ZIP_LAT)) * COS(RADIANS(TRY_TO_DOUBLE(si.LATITUDE))) *
              POWER(SIN(RADIANS((zl.ZIP_LON - TRY_TO_DOUBLE(si.LONGITUDE))/2)), 2)
            )) AS DIST_KM
          FROM VW_SUPPLIER_ZIPS sz
          JOIN VW_ZIP_LATLON zl ON zl.ZIP = sz.ZIP
          JOIN {stations_fqn} si
            ON si.LATITUDE IS NOT NULL AND si.LONGITUDE IS NOT NULL
        )
        SELECT ZIP, NOAA_WEATHER_STATION_ID
        FROM (
          SELECT ZIP, NOAA_WEATHER_STATION_ID, DIST_KM,
                 ROW_NUMBER() OVER (PARTITION BY ZIP ORDER BY DIST_KM) AS rn
          FROM cand
        )
        WHERE rn = 1
    """.format(stations_fqn=stations_fqn))

    # Rebuild the weather MV/table to carry 5-digit ZIPs
    try:
        cs.execute("DROP MATERIALIZED VIEW IF EXISTS supplier_zip_code_weather")
    except Exception:
        pass
    cs.execute("DROP TABLE IF EXISTS supplier_zip_code_weather")

    try:
        cs.execute(f"""
            CREATE MATERIALIZED VIEW supplier_zip_code_weather AS
            SELECT
              z.ZIP,
              ts.{date_col}::DATE AS WX_DATE,
              ts.{high_col}       AS HIGH_TEMP
            FROM VW_ZIP_STATION_NEAREST z
            JOIN {timeseries_fqn} ts
              ON ts.NOAA_WEATHER_STATION_ID = z.NOAA_WEATHER_STATION_ID
            WHERE ts.{high_col} IS NOT NULL
        """)
    except Exception:
        cs.execute(f"""
            CREATE TABLE supplier_zip_code_weather AS
            SELECT
              z.ZIP,
              ts.{date_col}::DATE AS WX_DATE,
              ts.{high_col}       AS HIGH_TEMP
            FROM VW_ZIP_STATION_NEAREST z
            JOIN {timeseries_fqn} ts
              ON ts.NOAA_WEATHER_STATION_ID = z.NOAA_WEATHER_STATION_ID
            WHERE ts.{high_col} IS NOT NULL
        """)

    # Quick overlap sanity check (optional)
    cs.execute("SELECT MIN(WX_DATE), MAX(WX_DATE), COUNT(*) FROM supplier_zip_code_weather")
    print("weather date range & rows:", cs.fetchone())

# Number 8 (final join using purchase_orders_and_invoices)
with conn.cursor() as cs:
    # 5-digit ZIP for suppliers (ZIP+4 safe)
    cs.execute("""
        CREATE OR REPLACE VIEW VW_SUPPLIER_ZIP5 AS
        SELECT
            "supplierid" AS supplier_id,
            LPAD(SUBSTR(REGEXP_REPLACE(TRIM("postalpostalcode"),'[^0-9]',''),1,5),5,'0') AS zip5
        FROM supplier_case
        WHERE "postalpostalcode" IS NOT NULL AND TRIM("postalpostalcode") <> ''
    """)

    cs.execute("DROP TABLE IF EXISTS FINAL_PO_INV_SUPPLIER_WEATHER")
    cs.execute("""
        CREATE TABLE FINAL_PO_INV_SUPPLIER_WEATHER AS
        SELECT
            p.purchase_order_id,
            p.supplier_id,
            z.ZIP5                     AS zip_code,
            p.transaction_date         AS wx_date,
            p.po_total_amount,
            p.amount_excluding_tax,
            p.invoiced_vs_quoted,
            w.HIGH_TEMP
        FROM purchase_orders_and_invoices p
        JOIN VW_SUPPLIER_ZIP5 z
          ON z.supplier_id = p.supplier_id
        JOIN supplier_zip_code_weather w
          ON w.ZIP = z.ZIP5
         AND w.WX_DATE = p.transaction_date
    """)

    cs.execute("SELECT COUNT(*) FROM FINAL_PO_INV_SUPPLIER_WEATHER")
    print("final joined rows:", cs.fetchone()[0])

    cs.execute("""
        SELECT purchase_order_id, supplier_id, zip_code, wx_date,
               po_total_amount, amount_excluding_tax, invoiced_vs_quoted, HIGH_TEMP
        FROM FINAL_PO_INV_SUPPLIER_WEATHER
        ORDER BY purchase_order_id
        LIMIT 10
    """)
    print(cs.fetchall())
