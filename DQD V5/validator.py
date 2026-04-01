"""
Config-driven validation engine.

Key goals:
- Run ONLY enabled checks (no "disabled but still runs" issue)
- Overall score out of 100 (sum of enabled check points must equal 100)
- Robust dtype coercion for required fields before validation
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml


def _as_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _as_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _safe_str_series(s: pd.Series) -> pd.Series:
    # preserve NaNs
    return s.astype("string")


def _points_from_pass_rate(points_possible: float, pass_rate: float) -> float:
    if points_possible <= 0:
        return 0.0
    pr = float(pass_rate) if pass_rate is not None else 0.0
    pr = max(0.0, min(100.0, pr))
    if pr > 95.0:
        return float(int(points_possible))          # full points
    elif pr > 90.0:
        return float(int(points_possible * 0.6))    # 60% of points
    else:
        return 0.0                                   # no points


# Check types whose failed_records are identifier strings (not row indices).
# Used by the errors-excel builder to handle them correctly.
IDENTIFIER_RECORD_CHECKS = {
    "missing_claims_from_prev_month",
    "total_paid_non_decreasing_vs_prev",
    "risk_policy_match",
}


@dataclass
class CheckContext:
    current_df: pd.DataFrame
    prev_df: Optional[pd.DataFrame]
    risk_df: Optional[pd.DataFrame]
    settings: Dict[str, Any]


class DataQualityValidator:
    def __init__(self, config_or_dict: Any):
        if isinstance(config_or_dict, str):
            with open(config_or_dict, "r", encoding="utf-8") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = config_or_dict

    def validate(
        self,
        current_df: pd.DataFrame,
        column_mappings: Dict[str, str],
        quarter_id: Optional[str] = None,
        risk_df: Optional[pd.DataFrame] = None,
        prev_df: Optional[pd.DataFrame] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        settings = settings or {}

        # Map columns to logical names
        df_current = current_df.rename(columns={v: k for k, v in column_mappings.items()})
        df_prev = None
        if prev_df is not None:
            df_prev = prev_df.rename(columns={v: k for k, v in column_mappings.items()})

        # Generic collision fix: if two logical names share the same physical column,
        # pandas only renames it to one. Find any missing logical names and copy from
        # whichever logical name ended up with that physical column's data.
        from collections import defaultdict
        phys_to_logical = defaultdict(list)
        for logical, physical in column_mappings.items():
            phys_to_logical[physical].append(logical)

        for physical, logical_names in phys_to_logical.items():
            if len(logical_names) < 2:
                continue
            for df in [df_current, df_prev]:
                if df is None:
                    continue
                present = [l for l in logical_names if l in df.columns]
                missing = [l for l in logical_names if l not in df.columns]
                if present and missing:
                    for m in missing:
                        df[m] = df[present[0]]

        # Coerce dtypes for fields we know about
        df_current = self._coerce_required_types(df_current)
        if df_prev is not None:
            df_prev = self._coerce_required_types(df_prev)

        ctx = CheckContext(current_df=df_current, prev_df=df_prev, risk_df=risk_df, settings=settings)

        enabled_checks = self._get_enabled_checks()
        results_by_category: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        for check in enabled_checks:
            category = check.get("category", "checks")
            subcategory = check.get("subcategory", "general")
            results_by_category.setdefault(category, {}).setdefault(subcategory, [])

            r = self._run_one_check(check, ctx)
            results_by_category[category][subcategory].append(r)

        scores = self._calculate_scores(results_by_category)

        final = {
            "quarter_id": quarter_id or datetime.now().strftime("%Y_Q%m"),
            "timestamp": datetime.now().isoformat(),
            "total_records": int(len(df_current)),
            "overall_score": float(scores["overall_score"]),
            "status": self._get_status(scores["overall_score"]),
            "category_scores": scores["category_scores"],
            "detailed_results": results_by_category,
            "summary": self._create_summary(results_by_category),
        }

        # Dashboard helper for optional risk message
        final["flags"] = {
            "risk_match_ok": bool(scores.get("risk_match_ok", False)),
        }

        return final

    def _get_enabled_checks(self) -> List[Dict[str, Any]]:
        checks = self.config.get("checks", [])
        return [c for c in checks if c.get("enabled", True)]

    def _coerce_required_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        date_cols = [
            "accident_date",
            "uw_date",
            "policy_start_date",
            "policy_end_date",
            "bdx_month",
        ]
        for c in date_cols:
            if c in df.columns:
                df[c] = _as_datetime_series(df[c])

        num_cols = ["total_incurred", "total_paid", "total_os"]
        for c in num_cols:
            if c in df.columns:
                df[c] = _as_numeric_series(df[c])

        if "claim_status" in df.columns:
            df["claim_status"] = _safe_str_series(df["claim_status"])

        if "policy_number" in df.columns:
            df["policy_number"] = _safe_str_series(df["policy_number"])
        if "claim_number" in df.columns:
            df["claim_number"] = _safe_str_series(df["claim_number"])

        return df

    def _run_one_check(self, check: Dict[str, Any], ctx: CheckContext) -> Dict[str, Any]:
        check_id = str(check.get("id", ""))
        name = str(check.get("name", check_id))
        points_possible = float(check.get("points", 0))
        check_type = str(check.get("type", ""))

        base = {
            "check_id": check_id,
            "check_name": name,
            "check_type": check_type,
            "points_possible": points_possible,
            # FIX: always pre-populate these so dashboard never shows blank/0 by default
            "priority": check.get("priority", "medium"),
            "pass_rate": 0.0,
            "passed": 0,
            "failed": 0,
            "total": 0,
            "failed_records": [],
            "failed_records_are_identifiers": check_type in IDENTIFIER_RECORD_CHECKS,
        }

        try:
            if check_type == "not_null":
                field = check["field"]
                pass_rate, passed, failed, total, failed_idx = self._check_not_null(ctx.current_df, field)
            elif check_type == "not_null_not_year_1900":
                field = check["field"]
                pass_rate, passed, failed, total, failed_idx = self._check_not_null_not_year_1900(ctx.current_df, field)
            elif check_type == "non_negative":
                field = check["field"]
                pass_rate, passed, failed, total, failed_idx = self._check_non_negative(ctx.current_df, field)
            elif check_type == "unique":
                field = check["field"]
                pass_rate, passed, failed, total, failed_idx = self._check_unique(ctx.current_df, field)
            elif check_type == "valid_date":
                field = check["field"]
                pass_rate, passed, failed, total, failed_idx = self._check_valid_date(ctx.current_df, field)
            elif check_type == "closed_os_zero":
                closed_vals = ctx.settings.get("closed_status_values", ["Closed"])
                pass_rate, passed, failed, total, failed_idx = self._check_closed_os_zero(
                    ctx.current_df, "claim_status", "total_os", closed_vals
                )
            elif check_type == "loss_within_policy":
                pass_rate, passed, failed, total, failed_idx = self._check_loss_within_policy(
                    ctx.current_df, "accident_date", "policy_start_date", "policy_end_date"
                )
            elif check_type == "incurred_equals_paid_plus_os":
                tolerance = float(check.get("tolerance", 0))
                pass_rate, passed, failed, total, failed_idx = self._check_incurred_equals_paid_plus_os(
                    ctx.current_df, "total_incurred", "total_paid", "total_os", tolerance
                )
            elif check_type == "missing_claims_from_prev_month":
                if ctx.prev_df is None:
                    base["points_earned"] = float(points_possible)
                    base["pass_rate"] = 100.0
                    base["skipped"] = True
                    return base
                pass_rate, passed, failed, total, failed_idx = self._check_missing_claims(
                    ctx.current_df, ctx.prev_df, "claim_number"
                )
            elif check_type == "total_paid_non_decreasing_vs_prev":
                if ctx.prev_df is None:
                    base["points_earned"] = float(points_possible)
                    base["pass_rate"] = 100.0
                    base["skipped"] = True
                    return base
                pass_rate, passed, failed, total, failed_idx = self._check_total_paid_non_decreasing(
                    ctx.current_df, ctx.prev_df, "claim_number", "total_paid"
                )
            elif check_type == "risk_policy_match":
                if ctx.risk_df is None:
                    base["points_earned"] = float(points_possible)
                    base["pass_rate"] = 100.0
                    base["skipped"] = True
                    return base
                # FIX: use correct column name — logical name is 'risk_policy_number'
                risk_policy_col = check.get("risk_policy_col", "risk_policy_number")
                pass_rate, passed, failed, total, failed_idx = self._check_risk_policy_match(
                    ctx.current_df, ctx.risk_df, "policy_number", risk_policy_col
                )
            else:
                base["points_earned"] = 0.0
                base["error"] = f"Unknown check type: {check_type}"
                return base

            points_earned = _points_from_pass_rate(points_possible, pass_rate)

            # FIX: always populate all result keys explicitly — never rely on default 0
            base.update({
                "pass_rate": float(pass_rate),
                "passed": int(passed),
                "failed": int(failed),      # this is now always correctly set
                "total": int(total),
                "points_earned": float(points_earned),
                "failed_records": failed_idx if isinstance(failed_idx, list) else list(failed_idx),
                "field": check.get("field", ""),
            })

        except Exception as e:
            base["points_earned"] = 0.0
            base["error"] = str(e)

        return base

    # ---- individual check implementations ----

    def _check_not_null(self, df, field):
        if field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        total = int(len(df))
        failed_mask = df[field].isna() | (df[field].astype(str).str.strip() == "")
        failed = int(failed_mask.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, df.index[failed_mask].tolist()

    def _check_not_null_not_year_1900(self, df, field):
        if field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        total = int(len(df))
        s = _as_datetime_series(df[field])
        failed_mask = s.isna() | (s.dt.year == 1900)
        failed = int(failed_mask.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, df.index[failed_mask].tolist()

    def _check_non_negative(self, df, field):
        if field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        total = int(len(df))
        s = _as_numeric_series(df[field])
        failed_mask = s.isna() | (s < 0)
        failed = int(failed_mask.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, df.index[failed_mask].tolist()

    def _check_unique(self, df, field):
        if field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        total = int(len(df))
        dupes = df[field].duplicated(keep=False)
        failed = int(dupes.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, df.index[dupes].tolist()

    def _check_valid_date(self, df, field):
        if field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        total = int(len(df))
        s = _as_datetime_series(df[field])
        failed_mask = s.isna()
        failed = int(failed_mask.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, df.index[failed_mask].tolist()

    def _check_closed_os_zero(self, df, status_field, os_field, closed_values):
        if status_field not in df.columns or os_field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        closed_vals_str = [str(v).strip() for v in closed_values]
        closed_mask = df[status_field].astype(str).str.strip().isin(closed_vals_str)
        scope = df[closed_mask]
        total = int(len(scope))
        if total == 0:
            return 100.0, 0, 0, 0, []
        os_s = _as_numeric_series(scope[os_field])
        failed_mask = os_s.isna() | (os_s != 0)
        failed = int(failed_mask.sum())
        passed = total - failed
        pass_rate = (passed / total * 100) if total else 0.0
        return pass_rate, passed, failed, total, scope.index[failed_mask].tolist()

    def _check_loss_within_policy(self, df, loss_field, start_field, end_field):
        if loss_field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        loss = _as_datetime_series(df[loss_field])
        usable = loss.notna()
        scope = df[usable].copy()
        total = int(len(scope))
        if total == 0:
            return 0.0, 0, 0, 0, []
        loss_s = loss[scope.index]
        if start_field not in df.columns:
            return 0.0, 0, 0, total, []
        start = _as_datetime_series(df[start_field])
        start_s = start[scope.index]
        if end_field not in df.columns:
            end_s = start_s + pd.DateOffset(years=1)
            end_s = pd.Series(end_s, index=scope.index)
        else:
            end = _as_datetime_series(df[end_field])
            end_s = end[scope.index]
        ok = (loss_s >= start_s) & (loss_s <= end_s)
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        return pass_rate, passed, failed, total, scope.index[~ok].tolist()

    def _check_incurred_equals_paid_plus_os(self, df, incurred_field, paid_field, os_field, tolerance):
        for f in [incurred_field, paid_field, os_field]:
            if f not in df.columns:
                return 0.0, 0, 0, int(len(df)), []
        inc = _as_numeric_series(df[incurred_field])
        paid = _as_numeric_series(df[paid_field])
        os_ = _as_numeric_series(df[os_field])
        usable = inc.notna() & paid.notna() & os_.notna()
        scope = df[usable].copy()
        total = int(len(scope))
        if total == 0:
            return 0.0, 0, 0, 0, []
        inc_s = _as_numeric_series(scope[incurred_field])
        paid_s = _as_numeric_series(scope[paid_field])
        os_s = _as_numeric_series(scope[os_field])
        diff = (inc_s - (paid_s + os_s)).abs()
        ok = diff <= float(tolerance)
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        return pass_rate, passed, failed, total, scope.index[~ok].tolist()

    def _check_missing_claims(self, current_df, prev_df, claim_field):
        if claim_field not in current_df.columns or claim_field not in prev_df.columns:
            return 0.0, 0, 0, 0, []
        prev_claims = prev_df[claim_field].dropna().astype("string").unique().tolist()
        prev_set = set(prev_claims)
        if not prev_set:
            return 100.0, 0, 0, 0, []
        current_set = set(current_df[claim_field].dropna().astype("string").unique().tolist())
        missing = sorted(prev_set - current_set)
        total = len(prev_set)
        failed = len(missing)
        passed = total - failed
        pass_rate = (passed / total) * 100 if total else 0.0
        # failed_records are claim number strings (identifiers), not row indices
        return pass_rate, passed, failed, total, missing[:100]

    def _check_total_paid_non_decreasing(self, current_df, prev_df, claim_field, paid_field):
        if claim_field not in current_df.columns or claim_field not in prev_df.columns:
            return 0.0, 0, 0, 0, []
        if paid_field not in current_df.columns or paid_field not in prev_df.columns:
            return 0.0, 0, 0, 0, []
        cur = current_df[[claim_field, paid_field]].copy()
        prv = prev_df[[claim_field, paid_field]].copy()
        cur[paid_field] = _as_numeric_series(cur[paid_field])
        prv[paid_field] = _as_numeric_series(prv[paid_field])
        cur_claim = cur[claim_field].astype("string")
        prv_claim = prv[claim_field].astype("string")
        cur = cur.assign(_claim=cur_claim)
        prv = prv.assign(_claim=prv_claim)
        cur_agg = cur.groupby("_claim", dropna=True)[paid_field].sum(min_count=1)
        prv_agg = prv.groupby("_claim", dropna=True)[paid_field].sum(min_count=1)
        total = int(cur_agg.shape[0])
        if total == 0:
            return 0.0, 0, 0, 0, []
        merged = pd.DataFrame({"cur_paid": cur_agg}).join(prv_agg.rename("prev_paid"), how="left")
        merged["prev_paid"] = merged["prev_paid"].fillna(-np.inf)
        ok = merged["cur_paid"] >= merged["prev_paid"]
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        # failed_records are claim number strings (identifiers), not row indices
        failed_claims = merged.index[~ok].astype(str).tolist()
        return pass_rate, passed, failed, total, failed_claims[:100]

    def _check_risk_policy_match(self, current_df, risk_df, policy_field, risk_policy_col):
        """
        Check that every unique policy number in the claims data exists in the risk table.
        failed_records = list of policy number strings that are absent from the risk table.
        """
        if policy_field not in current_df.columns:
            return 0.0, 0, 0, 0, []
        # FIX: risk_df has already been renamed to logical names upstream (if passed through
        # column_mappings). The risk policy column is mapped as 'risk_policy_number'.
        # Support both the logical name and the raw column name gracefully.
        actual_risk_col = risk_policy_col
        if actual_risk_col not in risk_df.columns:
            # Try common fallbacks
            for fallback in ["risk_policy_number", "policy_number", "PolicyNumber", "Policy_Number"]:
                if fallback in risk_df.columns:
                    actual_risk_col = fallback
                    break
            else:
                # Column not found at all — return 0 so it fails visibly
                return 0.0, 0, int(len(current_df)), int(len(current_df)), []

        cur_policies = current_df[policy_field].dropna().astype("string").unique().tolist()
        cur_set = set(cur_policies)
        if not cur_set:
            return 100.0, 0, 0, 0, []
        risk_set = set(risk_df[actual_risk_col].dropna().astype("string").unique().tolist())
        missing = sorted(cur_set - risk_set)
        total = len(cur_set)
        failed = len(missing)
        passed = total - failed
        pass_rate = (passed / total) * 100 if total else 0.0
        # failed_records are policy number strings (identifiers), not row indices
        return pass_rate, passed, failed, total, missing[:100]

    # ---- scoring/summary ----

    def _calculate_scores(self, results_by_category):
        category_scores = {}
        overall_possible = 0.0
        overall_earned = 0.0
        risk_match_ok = False

        for category, subcats in results_by_category.items():
            cat_possible = 0.0
            cat_earned = 0.0
            for _subcat, checks in subcats.items():
                for c in checks:
                    cat_possible += float(c.get("points_possible", 0))
                    cat_earned += float(c.get("points_earned", 0))
                    if c.get("check_type") == "risk_policy_match":
                        risk_match_ok = (float(c.get("pass_rate", 0)) == 100.0)
            pct = (cat_earned / cat_possible * 100.0) if cat_possible > 0 else 0.0
            category_scores[category] = {
                "points_earned": float(cat_earned),
                "points_possible": float(cat_possible),
                "percentage": float(pct),
            }
            overall_possible += cat_possible
            overall_earned += cat_earned

        return {
            "overall_score": float(overall_earned),
            "overall_possible": float(overall_possible),
            "category_scores": category_scores,
            "risk_match_ok": risk_match_ok,
        }

    def _get_status(self, score: float) -> str:
        if score >= 95:
            return "EXCELLENT"
        if score >= 85:
            return "GOOD"
        if score >= 75:
            return "FAIR"
        if score >= 65:
            return "POOR"
        return "CRITICAL"

    def _create_summary(self, results_by_category):
        total_checks = 0
        passed = 0
        warnings = 0
        failed = 0

        for _cat, subcats in results_by_category.items():
            for _sub, checks in subcats.items():
                for c in checks:
                    total_checks += 1
                    pe = float(c.get("points_earned", 0))
                    pp = float(c.get("points_possible", 0))
                    if pp <= 0:
                        continue
                    if pe == pp:
                        passed += 1
                    elif pe == 0:
                        failed += 1
                    else:
                        warnings += 1

        return {
            "total_checks": total_checks,
            "passed": passed,
            "warnings": warnings,
            "failed": failed,
            "critical_issues_count": 0,
            "critical_issues": [],
        }