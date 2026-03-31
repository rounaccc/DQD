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
    return float(points_possible) * (pr / 100.0)


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
                status_field = check.get("status_field", "claim_status")
                os_field = check.get("os_field", "total_os")
                closed_values = ctx.settings.get("closed_status_values") or check.get("closed_status_values") or ["Closed"]
                pass_rate, passed, failed, total, failed_idx = self._check_closed_os_zero(
                    ctx.current_df, status_field, os_field, closed_values
                )
            elif check_type == "loss_within_policy":
                loss_field = check.get("loss_field", "accident_date")
                start_field = check.get("start_field", "policy_start_date")
                end_field = check.get("end_field", "policy_end_date")
                pass_rate, passed, failed, total, failed_idx = self._check_loss_within_policy(
                    ctx.current_df, loss_field, start_field, end_field
                )
            elif check_type == "incurred_equals_paid_plus_os":
                incurred_field = check.get("incurred_field", "total_incurred")
                paid_field = check.get("paid_field", "total_paid")
                os_field = check.get("os_field", "total_os")
                tolerance = float(check.get("tolerance", 0))
                pass_rate, passed, failed, total, failed_idx = self._check_incurred_equals_paid_plus_os(
                    ctx.current_df, incurred_field, paid_field, os_field, tolerance
                )
            elif check_type == "missing_claims_from_prev_month":
                claim_field = check.get("claim_field", "claim_number")
                if ctx.prev_df is None:
                    pass_rate, passed, failed, total, failed_idx = 0.0, 0, 0, 0, []
                else:
                    pass_rate, passed, failed, total, failed_idx = self._check_missing_claims(ctx.current_df, ctx.prev_df, claim_field)
            elif check_type == "total_paid_non_decreasing_vs_prev":
                claim_field = check.get("claim_field", "claim_number")
                paid_field = check.get("paid_field", "total_paid")
                if ctx.prev_df is None:
                    pass_rate, passed, failed, total, failed_idx = 0.0, 0, 0, 0, []
                else:
                    pass_rate, passed, failed, total, failed_idx = self._check_total_paid_non_decreasing(
                        ctx.current_df, ctx.prev_df, claim_field, paid_field
                    )
            elif check_type == "risk_policy_match":
                # Policy number of each claim must exist in risk policy table column
                policy_field = check.get("policy_field", "policy_number")
                risk_policy_col = check.get("risk_policy_col", "policy_number")
                if ctx.risk_df is None:
                    pass_rate, passed, failed, total, failed_idx = 0.0, 0, 0, 0, []
                else:
                    pass_rate, passed, failed, total, failed_idx = self._check_risk_policy_match(
                        ctx.current_df, ctx.risk_df, policy_field, risk_policy_col
                    )
            else:
                raise ValueError(f"Unknown check type: {check_type}")

            points_earned = _points_from_pass_rate(points_possible, pass_rate)

            return {
                **base,
                "pass_rate": float(pass_rate),
                "passed": int(passed),
                "failed": int(failed),
                "total": int(total),
                "points_earned": float(points_earned),
                "failed_records": failed_idx[:100],
            }

        except Exception as e:
            return {
                **base,
                "pass_rate": 0.0,
                "passed": 0,
                "failed": 0,
                "total": 0,
                "points_earned": 0.0,
                "failed_records": [],
                "error": str(e),
            }

    # ---- checks ----

    def _check_not_null(self, df: pd.DataFrame, field: str) -> Tuple[float, int, int, int, List[int]]:
        total = int(len(df))
        if total == 0 or field not in df.columns:
            return 0.0, 0, 0, total, []
        ok = df[field].notna()
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = df.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_not_null_not_year_1900(self, df: pd.DataFrame, field: str) -> Tuple[float, int, int, int, List[int]]:
        total = int(len(df))
        if total == 0 or field not in df.columns:
            return 0.0, 0, 0, total, []
        dt = _as_datetime_series(df[field])
        ok = dt.notna() & (dt.dt.year != 1900)
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = df.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_non_negative(self, df: pd.DataFrame, field: str) -> Tuple[float, int, int, int, List[int]]:
        total = int(len(df))
        if total == 0 or field not in df.columns:
            return 0.0, 0, 0, total, []
        s = _as_numeric_series(df[field])
        ok = s.isna() | (s >= 0)
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = df.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_unique(self, df: pd.DataFrame, field: str) -> Tuple[float, int, int, int, List[int]]:
        total = int(len(df))
        if total == 0 or field not in df.columns:
            return 0.0, 0, 0, total, []
        dup = df.duplicated(subset=[field], keep=False)
        failed = int(dup.sum())
        passed = total - failed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = df.index[dup].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_valid_date(self, df: pd.DataFrame, field: str) -> Tuple[float, int, int, int, List[int]]:
        total = int(len(df))
        if total == 0 or field not in df.columns:
            return 0.0, 0, 0, total, []
        dt = _as_datetime_series(df[field])
        ok = dt.notna() | df[field].isna()
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = df.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_closed_os_zero(
        self,
        df: pd.DataFrame,
        status_field: str,
        os_field: str,
        closed_values: List[str],
    ) -> Tuple[float, int, int, int, List[int]]:
        if status_field not in df.columns or os_field not in df.columns:
            return 0.0, 0, 0, int(len(df)), []
        statuses = _safe_str_series(df[status_field]).str.strip().str.lower()
        closed_set = {str(v).strip().lower() for v in closed_values if v is not None}
        is_closed = statuses.isin(closed_set)
        scope = df[is_closed].copy()
        total = int(len(scope))
        if total == 0:
            # No closed claims: treat as pass
            return 100.0, 0, 0, 0, []
        os_vals = _as_numeric_series(scope[os_field]).fillna(0)
        ok = os_vals == 0
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = scope.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_loss_within_policy(
        self,
        df: pd.DataFrame,
        loss_field: str,
        start_field: str,
        end_field: str,
    ) -> Tuple[float, int, int, int, List[int]]:
        # loss_field and start_field are required; end_field is optional (inferred if missing)
        for f in [loss_field, start_field]:
            if f not in df.columns:
                return 0.0, 0, 0, int(len(df)), []

        loss = _as_datetime_series(df[loss_field])
        start = _as_datetime_series(df[start_field])

        # If end_field is missing or not in columns, infer as start + 1 year
        if end_field not in df.columns:
            end = start + pd.DateOffset(years=1)
            end = pd.Series(end, index=df.index)
            inferred_end = True
        else:
            end = _as_datetime_series(df[end_field])
            # For rows where end is null, fall back to start + 1 year
            inferred_mask = end.isna() & start.notna()
            if inferred_mask.any():
                end = end.copy()
                end[inferred_mask] = start[inferred_mask] + pd.DateOffset(years=1)
            inferred_end = False

        usable = loss.notna() & start.notna() & end.notna()
        scope = df[usable].copy()
        total = int(len(scope))
        if total == 0:
            return 0.0, 0, 0, 0, []
        loss_s = _as_datetime_series(scope[loss_field])
        start_s = _as_datetime_series(scope[start_field])
        if inferred_end:
            end_s = start_s + pd.DateOffset(years=1)
            end_s = pd.Series(end_s, index=scope.index)
        else:
            end_s = end[scope.index]
        ok = (loss_s >= start_s) & (loss_s <= end_s)
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_idx = scope.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_incurred_equals_paid_plus_os(
        self,
        df: pd.DataFrame,
        incurred_field: str,
        paid_field: str,
        os_field: str,
        tolerance: float,
    ) -> Tuple[float, int, int, int, List[int]]:
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
        failed_idx = scope.index[~ok].tolist()
        return pass_rate, passed, failed, total, failed_idx

    def _check_missing_claims(
        self, current_df: pd.DataFrame, prev_df: pd.DataFrame, claim_field: str
    ) -> Tuple[float, int, int, int, List[int]]:
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
        # Return missing claim numbers as failed_records (not row idx)
        return pass_rate, passed, failed, total, missing[:100]

    def _check_total_paid_non_decreasing(
        self,
        current_df: pd.DataFrame,
        prev_df: pd.DataFrame,
        claim_field: str,
        paid_field: str,
    ) -> Tuple[float, int, int, int, List[int]]:
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

        # Aggregate paid by claim number
        cur_agg = cur.groupby("_claim", dropna=True)[paid_field].sum(min_count=1)
        prv_agg = prv.groupby("_claim", dropna=True)[paid_field].sum(min_count=1)

        # For claims not in prev month, treat as pass (per your requirement)
        total = int(cur_agg.shape[0])
        if total == 0:
            return 0.0, 0, 0, 0, []

        merged = pd.DataFrame({"cur_paid": cur_agg}).join(prv_agg.rename("prev_paid"), how="left")
        merged["prev_paid"] = merged["prev_paid"].fillna(-np.inf)
        ok = merged["cur_paid"] >= merged["prev_paid"]
        passed = int(ok.sum())
        failed = total - passed
        pass_rate = (passed / total) * 100 if total else 0.0
        failed_claims = merged.index[~ok].astype(str).tolist()
        return pass_rate, passed, failed, total, failed_claims[:100]

    def _check_risk_policy_match(
        self,
        current_df: pd.DataFrame,
        risk_df: pd.DataFrame,
        policy_field: str,
        risk_policy_col: str,
    ) -> Tuple[float, int, int, int, List[int]]:
        if policy_field not in current_df.columns:
            return 0.0, 0, 0, 0, []
        if risk_policy_col not in risk_df.columns:
            return 0.0, 0, 0, 0, []
        cur_policies = current_df[policy_field].dropna().astype("string").unique().tolist()
        cur_set = set(cur_policies)
        if not cur_set:
            return 100.0, 0, 0, 0, []
        risk_set = set(risk_df[risk_policy_col].dropna().astype("string").unique().tolist())
        missing = sorted(cur_set - risk_set)
        total = len(cur_set)
        failed = len(missing)
        passed = total - failed
        pass_rate = (passed / total) * 100 if total else 0.0
        return pass_rate, passed, failed, total, missing[:100]

    # ---- scoring/summary ----

    def _calculate_scores(self, results_by_category: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> Dict[str, Any]:
        category_scores: Dict[str, Dict[str, float]] = {}
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

        # We want overall to read out of 100 when points sum to 100.
        overall_score = overall_earned

        return {
            "overall_score": float(overall_score),
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

    def _create_summary(self, results_by_category: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> Dict[str, Any]:
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