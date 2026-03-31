"""
Generate knowledge YAML files for the Talk to Data Pipeline.
Profiles Oracle views to create column_values, entity_aliases, and synonym_map.

Usage:
    python generate_knowledge.py --host <host> --port <port> --service <service_name> \
        --user <username> --password <password> \
        --view PISVIEW.VW_DIRECT_SPEND_ALL --label direct \
        --output-dir ./knowledge_direct

    python generate_knowledge.py --host <host> --port <port> --service <service_name> \
        --user <username> --password <password> \
        --view PISVIEW.VW_INDIRECT_SPEND_ALL --label indirect \
        --output-dir ./knowledge_indirect

Generates:
    1. column_values_{label}.yaml   — cardinality + sample values per column
    2. entity_aliases_{label}.yaml  — supplier/customer name abbreviations & variations
    3. synonym_map_{label}.yaml     — NL term → column name mappings
    4. data_context_{label}.txt     — human-readable data profile (row count, date ranges,
                                      column relationships, code columns, numeric ranges, NULL %)
"""

import argparse
import sys
import yaml
from collections import OrderedDict

try:
    import oracledb
except ImportError:
    print("ERROR: pip install oracledb")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────
# CONFIG — which columns to profile, skip, and how many samples
# ─────────────────────────────────────────────────────────────────────────

# Columns to SKIP profiling (too many unique values, binary, or not useful)
SKIP_COLUMNS = {
    "INVOICE_NO", "INVOICE_POS_NO", "ORDER_NO", "REFERENZBELEG",
    "POSTAL_KEY", "SAP Project No", "OEM_PART_NUMBER",
    "VCHR_LOC_CURRENCY_AMT", "VCHR_LOC_CURRENCY",
}

# Max distinct values to fetch per column (for high-cardinality columns)
MAX_SAMPLE_VALUES = 500

# Max columns to profile for entity alias detection
ENTITY_ALIAS_COLUMNS = {
    "SUPPLIER_NAME", "CUSTOMER", "OEM", "PLANT_NAME",
    "Parent Supplier", "COMMODITY_DESCRIPTION", "MATERIAL_GROUP",
    "MG_DESCRIPTION", "MAIN_ACCOUNT_DESCRIPTION",
}

# Mandatory date filter to apply during profiling
DATE_FILTER = "INVOICE_DATE > DATE '2024-04-01'"


def get_connection(args):
    dsn = oracledb.makedsn(args.host, args.port, service_name=args.service)
    return oracledb.connect(user=args.user, password=args.password, dsn=dsn)


# ─────────────────────────────────────────────────────────────────────────
# 1. COLUMN VALUE PROFILING
# ─────────────────────────────────────────────────────────────────────────

def profile_column_values(conn, view_name, date_filter):
    """Profile each column: cardinality, data type, sample values."""
    cur = conn.cursor()

    # Get all columns from the view
    schema, table = view_name.split(".") if "." in view_name else ("", view_name)
    cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = :tname
        {"AND OWNER = :owner" if schema else ""}
        ORDER BY COLUMN_ID
    """, {"tname": table, "owner": schema} if schema else {"tname": table})
    columns = cur.fetchall()

    results = OrderedDict()
    total = len(columns)

    for idx, (col_name, data_type, data_length) in enumerate(columns):
        if col_name in SKIP_COLUMNS:
            print(f"  [{idx+1}/{total}] SKIP {col_name}")
            continue

        print(f"  [{idx+1}/{total}] Profiling {col_name} ({data_type})...", end=" ", flush=True)

        try:
            # Get cardinality
            cur.execute(f"""
                SELECT COUNT(DISTINCT "{col_name}")
                FROM {view_name}
                WHERE {date_filter}
            """)
            cardinality = cur.fetchone()[0]

            # Get sample values (top N by frequency)
            if cardinality == 0:
                print(f"0 distinct")
                continue

            limit = min(cardinality, MAX_SAMPLE_VALUES)
            cur.execute(f"""
                SELECT val, cnt FROM (
                    SELECT "{col_name}" AS val, COUNT(*) AS cnt
                    FROM {view_name}
                    WHERE {date_filter} AND "{col_name}" IS NOT NULL
                    GROUP BY "{col_name}"
                    ORDER BY cnt DESC
                )
                WHERE ROWNUM <= :lim
            """, {"lim": limit})
            rows = cur.fetchall()

            examples = []
            for val, cnt in rows:
                if val is not None:
                    v = str(val).strip()
                    if v and len(v) < 200:
                        examples.append(v)

            results[col_name] = {
                "data_type": data_type,
                "cardinality": cardinality,
                "sample_count": len(examples),
                "examples": examples,
            }
            print(f"{cardinality} distinct, {len(examples)} samples")

        except Exception as e:
            print(f"ERROR: {e}")
            continue

    cur.close()
    return results


# ─────────────────────────────────────────────────────────────────────────
# 2. ENTITY ALIAS DETECTION
# ─────────────────────────────────────────────────────────────────────────

def detect_entity_aliases(conn, view_name, date_filter, column_values):
    """Detect common abbreviations and aliases in entity columns.

    For each entity column (SUPPLIER_NAME, CUSTOMER, etc.), find:
    - Short forms that map to full names (e.g. "Bosch" → "Bosch GmbH")
    - Common abbreviations
    - Parent-child relationships
    """
    aliases = OrderedDict()
    cur = conn.cursor()

    for col_name in ENTITY_ALIAS_COLUMNS:
        if col_name not in column_values:
            continue

        examples = column_values[col_name].get("examples", [])
        if not examples:
            continue

        print(f"  Detecting aliases for {col_name} ({len(examples)} values)...")

        # Strategy 1: Find values that are substrings of other values
        # e.g. "Bosch" appears inside "Bosch GmbH" and "Robert Bosch GmbH"
        for short_val in examples:
            if len(short_val) < 3 or len(short_val) > 50:
                continue
            matches = [v for v in examples if v != short_val and short_val.lower() in v.lower()]
            if len(matches) == 1:
                # Unique match — this is a good alias
                full_val = matches[0]
                alias_key = short_val.lower()
                if alias_key not in aliases:
                    aliases[alias_key] = {
                        "canonical_value": full_val,
                        "column": col_name,
                        "sql_filter": f"UPPER({col_name}) LIKE UPPER('%{short_val}%')",
                    }

        # Strategy 2: Common corporate suffixes — create short aliases
        suffixes = [" GmbH", " AG", " Ltd", " Inc", " Corp", " SE", " SA", " S.A.",
                     " Co.", " LLC", " Pvt", " S.p.A.", " B.V.", " N.V.", " A/S"]
        for val in examples:
            for suffix in suffixes:
                if val.endswith(suffix):
                    short = val[:-len(suffix)].strip()
                    if len(short) >= 3 and short.lower() not in aliases:
                        aliases[short.lower()] = {
                            "canonical_value": val,
                            "column": col_name,
                            "sql_filter": f"UPPER({col_name}) LIKE UPPER('%{short}%')",
                        }

    cur.close()
    return aliases


# ─────────────────────────────────────────────────────────────────────────
# 3. SYNONYM MAP GENERATION
# ─────────────────────────────────────────────────────────────────────────

def generate_synonym_map(column_values, label):
    """Generate NL term → column name mappings based on column names."""

    # Base synonyms that apply to all views
    base_synonyms = {
        "spend": {"column": "AMOUNT", "description": "Total spend (use AMOUNT / EXCH_RATE for EUR)"},
        "amount": {"column": "AMOUNT", "description": "Invoice amount in local currency"},
        "cost": {"column": "AMOUNT", "description": "Cost / spend amount"},
        "value": {"column": "AMOUNT", "description": "Monetary value"},
        "supplier": {"column": "SUPPLIER_NAME", "description": "Supplier / vendor name"},
        "vendor": {"column": "SUPPLIER_NAME", "description": "Supplier / vendor name"},
        "supplier name": {"column": "SUPPLIER_NAME", "description": "Supplier / vendor name"},
        "supplier number": {"column": "SUPPLIER_NO", "description": "Supplier ID number"},
        "plant": {"column": "PLANT_NAME", "description": "Manufacturing plant name"},
        "factory": {"column": "PLANT_NAME", "description": "Manufacturing plant / factory"},
        "location": {"column": "PLANT_NAME", "description": "Plant location"},
        "plant number": {"column": "PLANT_NO", "description": "Plant ID number"},
        "region": {"column": "REGION", "description": "Geographic region"},
        "country": {"column": "COUNTRY", "description": "Country"},
        "date": {"column": "INVOICE_DATE", "description": "Invoice date"},
        "invoice date": {"column": "INVOICE_DATE", "description": "Date of invoice"},
        "year": {"column": "INVOICE_DATE", "description": "Year (use EXTRACT(YEAR FROM INVOICE_DATE))"},
        "month": {"column": "INVOICE_DATE", "description": "Month (use TO_CHAR(INVOICE_DATE, 'YYYY-MM'))"},
        "quantity": {"column": "QUANTITY", "description": "Invoice line quantity"},
        "volume": {"column": "QUANTITY", "description": "Quantity / volume"},
        "units": {"column": "QUANTITY", "description": "Unit count"},
        "payment terms": {"column": "PAYMENT_TERM", "description": "Payment terms code"},
        "currency": {"column": "EXCH_CURRENCY", "description": "Local currency code"},
        "exchange rate": {"column": "EXCH_RATE", "description": "Exchange rate to EUR"},
        "invoice number": {"column": "INVOICE_NO", "description": "Invoice number"},
        "parent supplier": {"column": "Parent Supplier", "description": "Parent / group supplier"},
        "supplier group": {"column": "Parent Supplier", "description": "Parent supplier group"},
        "purchasing agent": {"column": "PURCHASING_AGENT_NAME", "description": "Buyer / purchasing agent"},
        "buyer": {"column": "PURCHASING_AGENT_NAME", "description": "Purchasing agent / buyer"},
        "material type": {"column": "MATERIAL_TYPE", "description": "Material type classification"},
        "article": {"column": "ARTICLE_DESCRIPTION", "description": "Article / part description"},
        "part": {"column": "ARTICLE_DESCRIPTION", "description": "Part / article description"},
        "part number": {"column": "ARTICLE_NO", "description": "Article / part number"},
        "item number": {"column": "ARTICLE_NO", "description": "Article / item number"},
        "material group": {"column": "Material Group", "description": "Material group code"},
        "main account": {"column": "MAIN_ACCOUNT", "description": "Main account code"},
    }

    # View-specific synonyms
    direct_synonyms = {
        "commodity": {"column": "COMMODITY", "description": "Commodity code"},
        "commodity description": {"column": "COMMODITY_DESCRIPTION", "description": "Commodity description"},
        "category": {"column": "COMMODITY_DESCRIPTION", "description": "Commodity category"},
        "customer": {"column": "CUSTOMER", "description": "Customer / OEM name"},
        "oem": {"column": "OEM", "description": "OEM (Original Equipment Manufacturer)"},
        "kss": {"column": "KSS", "description": "KSS code"},
        "sbu": {"column": "SBU", "description": "Strategic Business Unit"},
        "business unit": {"column": "SBU", "description": "Strategic Business Unit"},
        "vehicle type": {"column": "FAHRZEUGTYP", "description": "Vehicle type (Fahrzeugtyp)"},
        "fahrzeugtyp": {"column": "FAHRZEUGTYP", "description": "Vehicle type"},
        "project": {"column": "PROJECT_NAME", "description": "Project name"},
        "negotiated price": {"column": "ORG_NEGOTIATED_PRICE", "description": "Original negotiated price"},
        "abc indicator": {"column": "ABCINDICATOR", "description": "ABC classification indicator"},
        "ingredient": {"column": "INGREDIENT", "description": "Material ingredient"},
    }

    indirect_synonyms = {
        "commodity": {"column": "MATERIAL_GROUP", "description": "Material group (indirect commodity)"},
        "category": {"column": "MG_DESCRIPTION", "description": "Material group description"},
        "net price": {"column": "NET_PRICE", "description": "Net price per unit"},
        "industry": {"column": "INDUSTRY", "description": "Industry classification"},
        "source": {"column": "SOURCE", "description": "Procurement source"},
        "sales org": {"column": "SALES_ORG", "description": "Sales organization"},
        "sales organization": {"column": "SALES_ORG", "description": "Sales organization"},
        "voucher amount": {"column": "VOUCHER_AMOUNT", "description": "Voucher amount"},
        "voucher": {"column": "VOUCHER_AMOUNT", "description": "Voucher amount"},
    }

    synonyms = OrderedDict()
    synonyms.update(base_synonyms)

    if label == "direct":
        synonyms.update(direct_synonyms)
    elif label == "indirect":
        synonyms.update(indirect_synonyms)

    # Only include synonyms for columns that actually exist in the view
    existing_cols = set(column_values.keys())
    filtered = OrderedDict()
    for term, info in synonyms.items():
        col = info["column"]
        # Check both exact match and quoted column names
        if col in existing_cols or col.replace('"', '') in existing_cols:
            filtered[term] = info

    return filtered


# ─────────────────────────────────────────────────────────────────────────
# 4. DATA CONTEXT — comprehensive text profile
# ─────────────────────────────────────────────────────────────────────────

# Known code→name column pairs for relationship detection
KNOWN_RELATIONSHIPS = [
    ("SUPPLIER_NO", "SUPPLIER_NAME"),
    ("PLANT_NO", "PLANT_NAME"),
    ("Main Plant No", "Main Plant Name"),
    ("MAIN_ACCOUNT", "Main_Account_Description"),
    ("Material Group", "MG Description"),
    ("COMMODITY", "COMMODITY_DESCRIPTION"),
    ("Com. Supplier", "Com. Desr. Supp."),
]

# Columns that use short codes (not full names)
CODE_COLUMN_HINTS = {
    "Incoterms (Supplier Master)", "Terms of payment Supplier", "COUNTRY",
    "PLANT_NO", "Main Plant No", "PAYMENT_TERM", "EXCH_CURRENCY", "G_JAHR",
    "VCHR_LOC_CURRENCY", "POSTAL_KEY", "PURCHASE_ORG", "MATERIAL_TYPE",
    "COMMODITY", "OEM", "LIFETIME", "ABCINDICATOR", "Com. Supplier",
    "SUPPLIER_NO", "Material Group", "MAIN_ACCOUNT",
}

# Numeric columns to profile for ranges
NUMERIC_COLUMNS = {
    "AMOUNT", "QUANTITY", "EXCH_RATE", "SUPPLIER_NO",
    "ORG_NEGOTIATED_PRICE", "LAST_SAP_PRICE", "PRICE_AFTER_DISCOUNT",
    "NEGOTIATED_PRICE", "NET_PRICE", "VOUCHER_AMOUNT",
    "VCHR_LOC_CURRENCY_AMT",
}

# Date columns to profile
DATE_COLUMNS = {"INVOICE_DATE"}


def generate_data_context(conn, view_name, date_filter, column_values):
    """Generate a comprehensive text data profile similar to data_context.txt."""
    cur = conn.cursor()
    lines = []
    from datetime import datetime

    lines.append(f"Data Profile (auto-generated from {view_name}, {datetime.now().strftime('%Y-%m-%d')}):")

    # Total row count
    print("  Counting total rows...")
    try:
        cur.execute(f"SELECT COUNT(*) FROM {view_name} WHERE {date_filter}")
        total_rows = cur.fetchone()[0]
        lines.append(f"Total rows in scope: {total_rows:,} (filtered: {date_filter})")
    except Exception as e:
        lines.append(f"Total rows: ERROR — {e}")

    # ── Filter columns (low-to-medium cardinality with all values) ──
    lines.append("")
    lines.append("FILTER COLUMNS — Use these EXACT values in WHERE clauses:")

    existing_cols = set(column_values.keys())
    for col_name, info in column_values.items():
        card = info.get("cardinality", 0)
        examples = info.get("examples", [])
        if card == 0 or not examples:
            continue
        if card > 200:
            continue  # high cardinality, skip for filter section

        is_code = col_name in CODE_COLUMN_HINTS
        code_tag = ", values are SHORT CODES" if is_code else ""

        # Check NULL percentage
        null_pct = ""
        try:
            cur.execute(f"""
                SELECT ROUND(100 * SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                FROM {view_name} WHERE {date_filter}
            """)
            np = cur.fetchone()[0]
            if np and float(np) > 5:
                null_pct = f", {float(np):.1f}% NULL"
        except Exception:
            pass

        vals_str = ", ".join(f'"{v}"' for v in examples[:20])
        more = f" ... (+{card - 20} more)" if card > 20 else ""
        lines.append(f'{col_name} ({card} values{code_tag}{null_pct}): {vals_str}{more}')

    # ── High cardinality columns (top values only) ──
    lines.append("")
    lines.append("HIGH-CARDINALITY COLUMNS — Top values by frequency:")

    for col_name, info in column_values.items():
        card = info.get("cardinality", 0)
        examples = info.get("examples", [])
        if card <= 200 or not examples:
            continue

        null_pct = ""
        try:
            cur.execute(f"""
                SELECT ROUND(100 * SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                FROM {view_name} WHERE {date_filter}
            """)
            np = cur.fetchone()[0]
            if np and float(np) > 5:
                null_pct = f", {float(np):.1f}% NULL"
        except Exception:
            pass

        top_vals = ", ".join(f'"{v}"' for v in examples[:10])
        lines.append(f'{col_name} ({card} distinct{null_pct}): {top_vals}, ...')

    # ── Numeric ranges ──
    lines.append("")
    lines.append("NUMERIC RANGES:")

    for col_name in NUMERIC_COLUMNS:
        if col_name not in existing_cols:
            continue
        try:
            cur.execute(f"""
                SELECT MIN("{col_name}"), MAX("{col_name}"),
                       ROUND(AVG("{col_name}"), 2), ROUND(MEDIAN("{col_name}"), 2)
                FROM {view_name} WHERE {date_filter}
            """)
            row = cur.fetchone()
            if row and row[0] is not None:
                lines.append(f'{col_name}: {row[0]:,.2f} to {row[1]:,.2f} (avg: {row[2]:,.2f}, median: {row[3]:,.2f})')
        except Exception as e:
            print(f"    WARN: numeric range for {col_name}: {e}")

    # ── Date ranges ──
    lines.append("")
    lines.append("DATE RANGES:")

    for col_name in DATE_COLUMNS:
        if col_name not in existing_cols:
            continue
        try:
            cur.execute(f"""
                SELECT MIN("{col_name}"), MAX("{col_name}")
                FROM {view_name} WHERE {date_filter}
            """)
            row = cur.fetchone()
            if row and row[0]:
                lines.append(f'{col_name}: {row[0].strftime("%Y-%m-%d")} to {row[1].strftime("%Y-%m-%d")}')
        except Exception as e:
            print(f"    WARN: date range for {col_name}: {e}")

    # ── Column relationships ──
    lines.append("")
    lines.append("COLUMN RELATIONSHIPS (functional dependencies — code → name, always paired):")

    for code_col, name_col in KNOWN_RELATIONSHIPS:
        if code_col in existing_cols and name_col in existing_cols:
            lines.append(
                f'{code_col} → {name_col} — When displaying, SELECT both. '
                f'Use code for filtering, name for display.'
            )

    # ── High-NULL columns ──
    lines.append("")
    lines.append("HIGH-NULL COLUMNS — Use NVL() or filter with IS NOT NULL:")

    null_cols = []
    for col_name in existing_cols:
        try:
            cur.execute(f"""
                SELECT ROUND(100 * SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)
                FROM {view_name} WHERE {date_filter}
            """)
            np = cur.fetchone()[0]
            if np and float(np) > 10:
                null_cols.append((col_name, float(np)))
        except Exception:
            pass

    null_cols.sort(key=lambda x: x[1], reverse=True)
    for col_name, pct in null_cols[:15]:
        lines.append(f'{col_name}: {pct:.1f}% NULL')

    # ── Code columns list ──
    lines.append("")
    lines.append("CODE COLUMNS — These use short codes, NOT full names (e.g. 'DE' not 'Germany'):")
    code_cols_in_view = [c for c in CODE_COLUMN_HINTS if c in existing_cols]
    if code_cols_in_view:
        lines.append(", ".join(code_cols_in_view))
        lines.append("ALWAYS use UPPER() for case-insensitive matching on these columns.")

    cur.close()
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────
# YAML OUTPUT HELPERS
# ─────────────────────────────────────────────────────────────────────────

def yaml_str_representer(dumper, data):
    """Use block style for long strings."""
    if '\n' in data or len(data) > 80:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


yaml.add_representer(str, yaml_str_representer)
yaml.add_representer(OrderedDict, lambda d, data: d.represent_mapping('tag:yaml.org,2002:map', data.items()))


def save_yaml(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, width=200)
    print(f"  Saved: {filepath}")


# ─────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate knowledge YAML files from Oracle views")
    parser.add_argument("--host", required=True, help="Oracle host")
    parser.add_argument("--port", type=int, default=1521, help="Oracle port")
    parser.add_argument("--service", required=True, help="Oracle service name")
    parser.add_argument("--user", required=True, help="Oracle username")
    parser.add_argument("--password", required=True, help="Oracle password")
    parser.add_argument("--view", required=True, help="View name (e.g. PISVIEW.VW_DIRECT_SPEND_ALL)")
    parser.add_argument("--label", required=True, choices=["direct", "indirect"], help="View label")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    parser.add_argument("--date-filter", default=DATE_FILTER, help="Mandatory date filter")
    parser.add_argument("--max-samples", type=int, default=MAX_SAMPLE_VALUES, help="Max sample values per column")
    args = parser.parse_args()

    global MAX_SAMPLE_VALUES
    MAX_SAMPLE_VALUES = args.max_samples

    import os
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"\nConnecting to {args.host}:{args.port}/{args.service}...")
    conn = get_connection(args)
    conn.call_timeout = 120000  # 2 min timeout for profiling queries

    # 1. Column value profiling
    print(f"\n{'='*60}")
    print(f"1. COLUMN VALUE PROFILING — {args.view}")
    print(f"{'='*60}")
    column_values = profile_column_values(conn, args.view, args.date_filter)
    save_yaml(
        dict(column_values),
        os.path.join(args.output_dir, f"column_values_{args.label}.yaml"),
    )

    # 2. Entity alias detection
    print(f"\n{'='*60}")
    print(f"2. ENTITY ALIAS DETECTION")
    print(f"{'='*60}")
    aliases = detect_entity_aliases(conn, args.view, args.date_filter, column_values)
    save_yaml(
        dict(aliases),
        os.path.join(args.output_dir, f"entity_aliases_{args.label}.yaml"),
    )
    print(f"  Found {len(aliases)} aliases")

    # 3. Synonym map
    print(f"\n{'='*60}")
    print(f"3. SYNONYM MAP")
    print(f"{'='*60}")
    synonyms = generate_synonym_map(column_values, args.label)
    save_yaml(
        dict(synonyms),
        os.path.join(args.output_dir, f"synonym_map_{args.label}.yaml"),
    )
    print(f"  Generated {len(synonyms)} synonym mappings")

    # 4. Data context (text profile)
    print(f"\n{'='*60}")
    print(f"4. DATA CONTEXT")
    print(f"{'='*60}")
    context_text = generate_data_context(conn, args.view, args.date_filter, column_values)
    context_path = os.path.join(args.output_dir, f"data_context_{args.label}.txt")
    with open(context_path, "w", encoding="utf-8") as f:
        f.write(context_text)
    print(f"  Saved: {context_path}")

    conn.close()

    print(f"\n{'='*60}")
    print(f"DONE — files saved to {args.output_dir}/")
    print(f"{'='*60}")
    print(f"\nGenerated files:")
    print(f"  1. column_values_{args.label}.yaml   — {len(column_values)} columns profiled")
    print(f"  2. entity_aliases_{args.label}.yaml   — {len(aliases)} aliases detected")
    print(f"  3. synonym_map_{args.label}.yaml      — {len(synonyms)} synonym mappings")
    print(f"  4. data_context_{args.label}.txt      — full data profile (row count, ranges, relationships, NULLs)")
    print(f"\nFiles your team should prepare manually:")
    print(f"  5. business_rules_{args.label}.yaml   — domain rules, metrics, oracle syntax tips")
    print(f"  6. examples_{args.label}.yaml         — Q&A pairs with SQL (10-20 examples)")


if __name__ == "__main__":
    main()
