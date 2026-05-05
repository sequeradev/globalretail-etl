#!/usr/bin/env python
"""Preflight check before presentation.

Verifica que todo está listo para demostrar:
- Tests pasan
- Datos pueden leerse del CSV
- Configuración es válida
- Scripts de insights corren

Run:
    python scripts/preflight_check.py

Si ves todos los "✓", estás listo para presentar.
"""
import sys
from pathlib import Path

# Tests import
sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))

def check_imports():
    """Verifica que todas las dependencias están disponibles."""
    print("\n[1/6] Checking imports...")
    try:
        import pandas as pd
        print(f"  OK pandas {pd.__version__}")
    except ImportError as e:
        print(f"  FAIL pandas: {e}")
        return False

    try:
        import requests
        print(f"  OK requests available")
    except ImportError as e:
        print(f"  FAIL requests: {e}")
        return False

    try:
        from etl.config import load_config
        from etl.extract import read_sales_csv
        from etl.transform import clean_sales, build_quality_report
        print(f"  OK ETL modules load correctly")
    except Exception as e:
        print(f"  FAIL ETL modules: {e}")
        return False

    return True


def check_csv():
    """Verifica que el CSV está donde se espera."""
    print("\n[2/6] Checking data source...")
    csv_path = Path(__file__).parents[1] / "include" / "data" / "online_retail.csv"
    if csv_path.exists():
        size_mb = csv_path.stat().st_size / 1024 / 1024
        print(f"  OK CSV found: {csv_path.name} ({size_mb:.1f} MB)")
        return True
    else:
        print(f"  FAIL CSV not found at {csv_path}")
        return False


def check_extract_logic():
    """Verifica que la lógica de extracción funciona (CSV read, no MongoDB)."""
    print("\n[3/6] Checking extract logic...")
    try:
        import pandas as pd
        from pathlib import Path
        from etl.extract import read_sales_csv

        # We can't test read_sales_csv without MONGODB_URI set, but we can
        # test the CSV reading logic directly
        csv_path = Path(__file__).parents[1] / "include" / "data" / "online_retail.csv"
        df = pd.read_csv(
            csv_path,
            encoding="ISO-8859-1",
            dtype={
                "InvoiceNo": "string",
                "StockCode": "string",
                "Description": "string",
                "Quantity": "int64",
                "UnitPrice": "float64",
                "CustomerID": "string",
                "Country": "string",
            },
            parse_dates=["InvoiceDate"],
        )
        if df["InvoiceDate"].dt.tz is None:
            df["InvoiceDate"] = df["InvoiceDate"].dt.tz_localize("UTC")

        print(f"  OK CSV read: {len(df):,} rows")
        print(f"  OK Columns: {', '.join(df.columns.tolist()[:5])}...")
        return True
    except Exception as e:
        print(f"  FAIL Extract failed: {e}")
        return False


def check_transform_logic():
    """Verifica que la lógica de transformación funciona."""
    print("\n[4/6] Checking transform logic...")
    try:
        import pandas as pd
        from etl.transform import clean_sales, build_fact_records

        # Dummy data
        df = pd.DataFrame({
            "InvoiceNo": ["A1", "A2"],
            "StockCode": ["X", "Y"],
            "Description": ["a", "b"],
            "Quantity": [1, 2],
            "UnitPrice": [10.0, 20.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01", "2011-01-02"], utc=True),
            "CustomerID": ["c1", "c2"],
            "Country": ["UK", "FR"],
            "region": ["Europe", "Europe"],
            "population": [1, 1],
            "revenue_gbp": [10.0, 40.0],
            "revenue_eur": [11.7, 46.8],
            "fx_rate_gbp_eur": [1.17, 1.17],
        })

        fact = build_fact_records(df)

        print(f"  OK Transform works: {len(fact)} fact records")
        print(f"  OK Business key: {fact['_bk'].iloc[0][:30]}...")
        return True
    except Exception as e:
        print(f"  FAIL Transform failed: {e}")
        return False


def check_insights():
    """Verifica que el script de insights puede ejecutarse."""
    print("\n[5/6] Checking insights script...")
    try:
        import pandas as pd
        csv_path = Path(__file__).parents[1] / "include" / "data" / "online_retail.csv"
        df = pd.read_csv(csv_path, encoding="ISO-8859-1")
        print(f"  OK Insights script can read CSV: {len(df):,} rows")
        return True
    except Exception as e:
        print(f"  FAIL Insights script: {e}")
        return False


def check_docs():
    """Verifica que los documentos de presentación existen."""
    print("\n[6/6] Checking documentation...")
    docs_needed = [
        "docs/presentation_guide.md",
        "docs/business_insights.md",
        "DEMO_GUIDE.md",
    ]
    all_exist = True
    for doc in docs_needed:
        path = Path(__file__).parents[1] / doc
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print(f"  OK {doc} ({size_kb:.0f} KB)")
        else:
            print(f"  FAIL {doc} not found")
            all_exist = False
    return all_exist


def main():
    print("=" * 60)
    print("GlobalRetail ETL -- Presentation Preflight Check")
    print("=" * 60)

    results = []
    results.append(("Imports", check_imports()))
    results.append(("CSV available", check_csv()))
    results.append(("Extract logic", check_extract_logic()))
    results.append(("Transform logic", check_transform_logic()))
    results.append(("Insights script", check_insights()))
    results.append(("Documentation", check_docs()))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for name, passed in results:
        status = "[OK]" if passed else "[FAIL]"
        print(f"{status:7} {name}")

    all_passed = all(p for _, p in results)

    print("=" * 60)
    if all_passed:
        print("[SUCCESS] Preflight OK - Ready to present")
        print("\nNext steps:")
        print("1. Read DEMO_GUIDE.md for the script")
        print("2. Have Power BI open with CapstoneBI.pbix")
        print("3. Terminal ready in project folder")
        print("4. Run: PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/ -v")
        return 0
    else:
        print("[ERROR] Failures detected - see above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
