# budget.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import json
import os
import threading

# ---- File-based simple storage (thread-safe) ----

_DATA_LOCK = threading.Lock()
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_PATH = os.path.join(_THIS_DIR, "budgets.json")

# JSON structure:
# {
#   "months": {
#     "YYYY-MM": {
#       "income": { "planned": 0.0, "actual": 0.0|null },
#       "expenses": [{ "category": "Housing", "planned": 2500.0, "actual": 2550.0|null }],
#       "notes": ""
#     },
#     ...
#   }
# }

def _ensure_store() -> Dict[str, Any]:
    if not os.path.exists(_DATA_PATH):
        with open(_DATA_PATH, "w", encoding="utf-8") as f:
            json.dump({"months": {}}, f)
    with open(_DATA_PATH, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = {"months": {}}
    if "months" not in data or not isinstance(data["months"], dict):
        data["months"] = {}
    return data

def _save_store(data: Dict[str, Any]) -> None:
    tmp_path = _DATA_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, _DATA_PATH)

def _norm_month(month: str) -> str:
    # Expect "YYYY-MM"
    try:
        dt = datetime.strptime(month, "%Y-%m")
        return dt.strftime("%Y-%m")
    except Exception:
        raise ValueError("Invalid month format. Use 'YYYY-MM' (e.g., 2026-02).")

def _month_iter(from_month: str, to_month: str) -> List[str]:
    start = datetime.strptime(_norm_month(from_month), "%Y-%m")
    end = datetime.strptime(_norm_month(to_month), "%Y-%m")
    if start > end:
        raise ValueError("from > to")
    months = []
    cur_y, cur_m = start.year, start.month
    while cur_y < end.year or (cur_y == end.year and cur_m <= end.month):
        months.append(f"{cur_y:04d}-{cur_m:02d}")
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1
    return months

def _clean_amount(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        raise ValueError("Amounts must be numbers.")
    if abs(v) > 1e8:
        raise ValueError("Amount too large.")
    # We assume expenses are entered as positive numbers (we handle signs later)
    if v < 0:
        # Allow negative only if user explicitly wants to; keep simple: normalize to abs
        v = abs(v)
    return round(v, 2)

def _merge_expenses(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Merge by category name (case-insensitive), update planned/actual if provided
    by_key: Dict[str, Dict[str, Any]] = {}
    for e in existing:
        key = e["category"].strip().lower()
        by_key[key] = {"category": e["category"], "planned": e.get("planned", 0.0), "actual": e.get("actual")}

    for e in incoming:
        if "category" not in e or not str(e["category"]).strip():
            raise ValueError("Each expense item must have a non-empty 'category'.")
        key = str(e["category"]).strip().lower()
        planned = _clean_amount(e.get("planned", 0.0) or 0.0)
        actual = _clean_amount(e.get("actual", None)) if e.get("actual", None) is not None else None
        if key in by_key:
            # update/overwrite
            if planned is not None:
                by_key[key]["planned"] = planned
            by_key[key]["actual"] = actual if actual is not None else by_key[key].get("actual", None)
        else:
            by_key[key] = {"category": e["category"], "planned": planned or 0.0, "actual": actual}

    # Sort by category name stable
    merged = sorted(by_key.values(), key=lambda x: x["category"].lower())
    return merged

# ---- Public API (called by FastAPI routes) ----

def save_month_budget(
    month: str,
    income_planned: float,
    income_actual: Optional[float],
    expenses: List[Dict[str, Any]],
    notes: Optional[str] = None,
    merge: bool = True
) -> Dict[str, Any]:
    """
    Upsert monthly budget (plan & actual). By default merges expense categories.
    """
    m = _norm_month(month)
    ip = _clean_amount(income_planned) or 0.0
    ia = _clean_amount(income_actual) if income_actual is not None else None

    # normalize incoming expenses
    incoming_expenses: List[Dict[str, Any]] = []
    for item in expenses or []:
        if "category" not in item:
            raise ValueError("Expense items must include 'category'.")
        incoming_expenses.append({
            "category": str(item["category"]).strip(),
            "planned": _clean_amount(item.get("planned", 0.0) or 0.0) or 0.0,
            "actual": _clean_amount(item["actual"]) if item.get("actual", None) is not None else None
        })

    with _DATA_LOCK:
        store = _ensure_store()
        existing = store["months"].get(m)
        if not existing:
            # create new record
            store["months"][m] = {
                "income": {"planned": ip, "actual": ia},
                "expenses": incoming_expenses,
                "notes": notes or ""
            }
        else:
            # update
            existing["income"]["planned"] = ip
            existing["income"]["actual"] = ia
            existing["notes"] = notes if notes is not None else existing.get("notes", "")
            if merge:
                existing["expenses"] = _merge_expenses(existing.get("expenses", []), incoming_expenses)
            else:
                existing["expenses"] = incoming_expenses
        _save_store(store)

    return {"status": "ok", "month": m}

def _calc_month_rollup(rec: Dict[str, Any]) -> Dict[str, Any]:
    income_p = float(rec["income"].get("planned", 0.0) or 0.0)
    income_a_raw = rec["income"].get("actual", None)
    income_a = float(income_a_raw) if income_a_raw is not None else None

    exp_p = 0.0
    exp_a = 0.0
    cat_rows = []
    for e in rec.get("expenses", []):
        p = float(e.get("planned", 0.0) or 0.0)
        a_raw = e.get("actual", None)
        a = float(a_raw) if a_raw is not None else None
        exp_p += p
        exp_a += (a or 0.0)
        cat_rows.append({
            "category": e["category"],
            "planned": p,
            "actual": a,
            "variance": (a - p) if a is not None else None
        })

    net_p = income_p - exp_p
    # If actual income provided, use it; else compare to planned
    if income_a is not None:
        net_a = income_a - exp_a
        sr_a = (net_a / income_a) if income_a > 0 else None
    else:
        net_a = None
        sr_a = None

    sr_p = (net_p / income_p) if income_p > 0 else None

    return {
        "income": {"planned": round(income_p, 2), "actual": (round(income_a, 2) if income_a is not None else None)},
        "expenses_total": {"planned": round(exp_p, 2), "actual": (round(exp_a, 2) if exp_a is not None else None)},
        "categories": cat_rows,
        "net_savings": {
            "planned": round(net_p, 2),
            "actual": (round(net_a, 2) if net_a is not None else None),
            "variance": (round((net_a - net_p), 2) if (income_a is not None) else None)
        },
        "savings_rate": {
            "planned": (round(sr_p, 4) if sr_p is not None else None),
            "actual": (round(sr_a, 4) if sr_a is not None else None)
        }
    }

def get_month_budget(month: str) -> Dict[str, Any]:
    m = _norm_month(month)
    with _DATA_LOCK:
        store = _ensure_store()
        rec = store["months"].get(m)
        if not rec:
            raise ValueError(f"No budget saved for {m}.")
        roll = _calc_month_rollup(rec)
        roll["month"] = m
        roll["notes"] = rec.get("notes", "")
        roll["categories"] = sorted(roll["categories"], key=lambda x: x["category"].lower())
        return roll

def get_year_overview(year: int) -> Dict[str, Any]:
    # Return each month (present or not) for the whole year
    months = [f"{year:04d}-{i:02d}" for i in range(1, 13)]
    rows: List[Dict[str, Any]] = []
    with _DATA_LOCK:
        store = _ensure_store()
        for m in months:
            rec = store["months"].get(m)
            if rec:
                roll = _calc_month_rollup(rec)
                rows.append({
                    "month": m,
                    "income_planned": roll["income"]["planned"],
                    "income_actual": roll["income"]["actual"],
                    "expense_planned": roll["expenses_total"]["planned"],
                    "expense_actual": roll["expenses_total"]["actual"],
                    "net_planned": roll["net_savings"]["planned"],
                    "net_actual": roll["net_savings"]["actual"],
                })
            else:
                rows.append({
                    "month": m,
                    "income_planned": 0.0,
                    "income_actual": None,
                    "expense_planned": 0.0,
                    "expense_actual": None,
                    "net_planned": 0.0,
                    "net_actual": None,
                })
    return {"year": year, "months": rows}

def get_series(from_month: str, to_month: str) -> Dict[str, Any]:
    rng = _month_iter(from_month, to_month)
    series: List[Dict[str, Any]] = []
    cumulative_actual = 0.0
    with _DATA_LOCK:
        store = _ensure_store()
        for m in rng:
            rec = store["months"].get(m)
            if rec:
                roll = _calc_month_rollup(rec)
                net_p = roll["net_savings"]["planned"]
                net_a = roll["net_savings"]["actual"]
                if net_a is not None:
                    cumulative_actual += net_a
                series.append({
                    "month": m,
                    "net_planned": net_p,
                    "net_actual": net_a,
                    "cumulative_actual": round(cumulative_actual, 2)
                })
            else:
                series.append({
                    "month": m,
                    "net_planned": 0.0,
                    "net_actual": None,
                    "cumulative_actual": round(cumulative_actual, 2)
                })
    return {"from": rng[0], "to": rng[-1], "series": series}

def list_all_categories() -> List[str]:
    cats = set()
    with _DATA_LOCK:
        store = _ensure_store()
        for rec in store["months"].values():
            for e in rec.get("expenses", []):
                cats.add(str(e.get("category", "")).strip())
    return sorted([c for c in cats if c], key=lambda x: x.lower())