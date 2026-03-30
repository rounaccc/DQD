# ============================================================================
# FILE: validator.py
# Final validation engine with 15 checks (100 points total)
# ============================================================================

import pandas as pd
import numpy as np
from datetime import datetime

class DataQualityValidator:
    """Validation engine with 15 checks"""
    
    def __init__(self, enabled_checks=None, check_points=None):
        """
        Initialize validator
        
        Args:
            enabled_checks: dict of {check_id: True/False}
            check_points: dict of {check_id: points}
        """
        self.enabled_checks = enabled_checks or {}
        self.check_points = check_points or self._get_default_points()
        self.results = {}
        
    def _get_default_points(self):
        """Default equal distribution of 100 points across 15 checks"""
        # 100 / 15 = 6.67, so use: 13 checks with 7 points, 2 checks with 6.5 (round to 7 and 6)
        return {
            '1.1': 7, '1.2': 7, '1.3': 7, '1.4': 7, '1.5': 7,  # Not Null: 35
            '2.1': 7, '2.2': 7, '2.3': 7,                      # Non-Negative: 21
            '3.1': 7, '3.2': 7, '3.3': 7,                      # Logical: 21
            '4.1': 7,                                           # Uniqueness: 7
            '5.1': 8, '5.2': 8,                                # Historical: 16
            '6.1': 0  # Risk check - optional, not in 100
        }
    
    def validate(self, df, column_mappings, quarter_id, prev_df=None, risk_df=None, closed_values=None):
        """
        Run validation checks
        
        Args:
            df: Current month data
            column_mappings: Dict mapping logical names to actual columns
            quarter_id: e.g. '2024_Q3'
            prev_df: Previous month data (for historical checks)
            risk_df: Risk table data (optional)
            closed_values: List of values meaning "closed" (e.g. ['Closed', 'CLOSED'])
        """
        print("🔍 Starting Validation...")
        
        # Convert data types
        df = self._convert_data_types(df, column_mappings)
        
        # Prepare data with logical column names
        df_mapped = self._map_columns(df, column_mappings)
        
        if prev_df is not None:
            prev_df = self._convert_data_types(prev_df, column_mappings)
            prev_df_mapped = self._map_columns(prev_df, column_mappings)
        else:
            prev_df_mapped = None
        
        # Run all checks
        all_results = {}
        
        # Check 1.1: Policy Number Not Null
        if self._is_enabled('1.1'):
            all_results['1.1'] = self._check_not_null(df_mapped, 'policy_number', '1.1', 'Policy Number Complete')
        
        # Check 1.2: Claim Number Not Null
        if self._is_enabled('1.2'):
            all_results['1.2'] = self._check_not_null(df_mapped, 'claim_number', '1.2', 'Claim Number Complete')
        
        # Check 1.3: Accident Date Not Null
        if self._is_enabled('1.3'):
            all_results['1.3'] = self._check_not_null(df_mapped, 'accident_date', '1.3', 'Accident Date Complete')
        
        # Check 1.4: Status Not Null
        if self._is_enabled('1.4'):
            all_results['1.4'] = self._check_not_null(df_mapped, 'status', '1.4', 'Status of Claim Complete')
        
        # Check 1.5: UW Date Not Null and Not 1900
        if self._is_enabled('1.5'):
            all_results['1.5'] = self._check_uw_date(df_mapped, '1.5', 'UW Date Valid')
        
        # Check 2.1: Total Incurred Non-Negative
        if self._is_enabled('2.1'):
            all_results['2.1'] = self._check_non_negative(df_mapped, 'total_incurred', '2.1', 'Total Incurred Non-Negative')
        
        # Check 2.2: Total Paid Non-Negative
        if self._is_enabled('2.2'):
            all_results['2.2'] = self._check_non_negative(df_mapped, 'total_paid', '2.2', 'Total Paid Non-Negative')
        
        # Check 2.3: Total Reserve Non-Negative
        if self._is_enabled('2.3'):
            all_results['2.3'] = self._check_non_negative(df_mapped, 'total_reserve', '2.3', 'Total Reserve Non-Negative')
        
        # Check 3.1: Closed Claims Have Zero Reserve
        if self._is_enabled('3.1'):
            all_results['3.1'] = self._check_closed_zero_reserve(df_mapped, closed_values or ['Closed'], '3.1', 'Closed Claims Zero Reserve')
        
        # Check 3.2: Date of Loss Within Policy Period
        if self._is_enabled('3.2'):
            all_results['3.2'] = self._check_loss_in_policy_period(df_mapped, '3.2', 'Loss Date Within Policy Period')
        
        # Check 3.3: Total Incurred Formula
        if self._is_enabled('3.3'):
            all_results['3.3'] = self._check_incurred_formula(df_mapped, '3.3', 'Total Incurred = Reserve + Paid')
        
        # Check 4.1: Unique Claim Numbers
        if self._is_enabled('4.1'):
            all_results['4.1'] = self._check_unique_claims(df_mapped, '4.1', 'Claim Numbers Unique')
        
        # Check 5.1: Missing Claims from Previous Month
        if self._is_enabled('5.1'):
            if prev_df_mapped is not None:
                all_results['5.1'] = self._check_missing_claims(df_mapped, prev_df_mapped, '5.1', 'No Missing Claims')
            else:
                all_results['5.1'] = self._warning_result('5.1', 'No Missing Claims', 'Previous month data not available')
        
        # Check 5.2: Total Paid Increasing
        if self._is_enabled('5.2'):
            if prev_df_mapped is not None:
                all_results['5.2'] = self._check_paid_increasing(df_mapped, prev_df_mapped, '5.2', 'Total Paid Non-Decreasing')
            else:
                all_results['5.2'] = self._warning_result('5.2', 'Total Paid Non-Decreasing', 'Previous month data not available')
        
        # Check 6.1: Risk Table Reconciliation (Optional)
        if self._is_enabled('6.1') and risk_df is not None:
            risk_df_mapped = self._map_columns(risk_df, column_mappings)
            all_results['6.1'] = self._check_risk_reconciliation(df_mapped, risk_df_mapped, '6.1', 'Risk Table Reconciliation')
        
        # Calculate scores
        scores = self._calculate_scores(all_results)
        
        # Compile results
        final_results = {
            'quarter_id': quarter_id,
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'overall_score': scores['overall_score'],
            'status': self._get_status(scores['overall_score']),
            'detailed_results': all_results,
            'summary': self._create_summary(all_results, scores)
        }
        
        print(f"✅ Validation Complete! Score: {scores['overall_score']:.0f}/100")
        
        return final_results
    
    def _is_enabled(self, check_id):
        """Check if a check is enabled"""
        return self.enabled_checks.get(check_id, True)
    
    def _convert_data_types(self, df, column_mappings):
        """Convert data types of required columns"""
        df = df.copy()
        
        # Date columns
        date_cols = ['accident_date', 'uw_date', 'policy_start_date', 'policy_end_date']
        for logical_col in date_cols:
            if logical_col in column_mappings:
                actual_col = column_mappings[logical_col]
                if actual_col in df.columns:
                    try:
                        df[actual_col] = pd.to_datetime(df[actual_col], errors='coerce')
                    except:
                        pass
        
        # Numeric columns
        numeric_cols = ['total_incurred', 'total_paid', 'total_reserve']
        for logical_col in numeric_cols:
            if logical_col in column_mappings:
                actual_col = column_mappings[logical_col]
                if actual_col in df.columns:
                    try:
                        df[actual_col] = pd.to_numeric(df[actual_col], errors='coerce')
                    except:
                        pass
        
        return df
    
    def _map_columns(self, df, column_mappings):
        """Rename columns to logical names"""
        rename_dict = {v: k for k, v in column_mappings.items() if v in df.columns}
        return df.rename(columns=rename_dict)
    
    # Individual check functions
    
    def _check_not_null(self, df, field, check_id, check_name):
        """Check if field is not null"""
        total = len(df)
        not_null = df[field].notna().sum()
        pass_rate = (not_null / total * 100) if total > 0 else 0
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': not_null,
            'failed': total - not_null,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_uw_date(self, df, check_id, check_name):
        """Check UW date is not null and not 1900"""
        total = len(df)
        
        # Not null
        not_null = df['uw_date'].notna()
        
        # Not 1900
        not_1900 = df['uw_date'].dt.year != 1900
        
        valid = not_null & not_1900
        passed = valid.sum()
        pass_rate = (passed / total * 100) if total > 0 else 0
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_non_negative(self, df, field, check_id, check_name):
        """Check if field is non-negative"""
        df_check = df.dropna(subset=[field])
        total = len(df_check)
        
        if total == 0:
            return self._empty_result(check_id, check_name)
        
        non_negative = (df_check[field] >= 0).sum()
        pass_rate = (non_negative / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': non_negative,
            'failed': total - non_negative,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_closed_zero_reserve(self, df, closed_values, check_id, check_name):
        """Check closed claims have zero reserve"""
        # Filter to closed claims
        closed_mask = df['status'].isin(closed_values)
        df_closed = df[closed_mask]
        
        total = len(df_closed)
        
        if total == 0:
            return self._empty_result(check_id, check_name, note='No closed claims found')
        
        zero_reserve = (df_closed['total_reserve'] == 0).sum()
        pass_rate = (zero_reserve / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': zero_reserve,
            'failed': total - zero_reserve,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_loss_in_policy_period(self, df, check_id, check_name):
        """Check loss date is between policy start and end"""
        df_check = df.dropna(subset=['accident_date', 'policy_start_date', 'policy_end_date'])
        total = len(df_check)
        
        if total == 0:
            return self._empty_result(check_id, check_name)
        
        in_period = (
            (df_check['accident_date'] >= df_check['policy_start_date']) &
            (df_check['accident_date'] <= df_check['policy_end_date'])
        ).sum()
        
        pass_rate = (in_period / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': in_period,
            'failed': total - in_period,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_incurred_formula(self, df, check_id, check_name):
        """Check Total Incurred = Total Reserve + Total Paid"""
        df_check = df.dropna(subset=['total_incurred', 'total_reserve', 'total_paid'])
        total = len(df_check)
        
        if total == 0:
            return self._empty_result(check_id, check_name)
        
        calculated = df_check['total_reserve'] + df_check['total_paid']
        diff = abs(df_check['total_incurred'] - calculated)
        
        # Allow tolerance of $1
        valid = (diff <= 1).sum()
        pass_rate = (valid / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': valid,
            'failed': total - valid,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate >= 98 else 0
        }
    
    def _check_unique_claims(self, df, check_id, check_name):
        """Check claim numbers are unique"""
        total = len(df)
        duplicates = df['claim_number'].duplicated().sum()
        unique = total - duplicates
        pass_rate = (unique / total * 100) if total > 0 else 0
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': unique,
            'failed': duplicates,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0
        }
    
    def _check_missing_claims(self, df_current, df_prev, check_id, check_name):
        """Check all claims from previous month exist in current"""
        prev_claims = set(df_prev['claim_number'].dropna())
        current_claims = set(df_current['claim_number'].dropna())
        
        missing = prev_claims - current_claims
        total_prev = len(prev_claims)
        
        if total_prev == 0:
            return self._empty_result(check_id, check_name, note='No previous claims to compare')
        
        present = total_prev - len(missing)
        pass_rate = (present / total_prev * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': present,
            'failed': len(missing),
            'total': total_prev,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate == 100 else 0,
            'missing_claims': list(missing)[:100]
        }
    
    def _check_paid_increasing(self, df_current, df_prev, check_id, check_name):
        """Check total paid is non-decreasing for same claims"""
        # Merge on claim number
        merged = df_current.merge(
            df_prev[['claim_number', 'total_paid']],
            on='claim_number',
            how='inner',
            suffixes=('_current', '_prev')
        )
        
        total = len(merged)
        
        if total == 0:
            return self._empty_result(check_id, check_name, note='No common claims to compare')
        
        non_decreasing = (merged['total_paid_current'] >= merged['total_paid_prev']).sum()
        pass_rate = (non_decreasing / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': non_decreasing,
            'failed': total - non_decreasing,
            'total': total,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id] if pass_rate >= 98 else 0
        }
    
    def _check_risk_reconciliation(self, df_claims, df_risk, check_id, check_name):
        """Check claim policy numbers exist in risk table"""
        claim_policies = set(df_claims['policy_number'].dropna())
        risk_policies = set(df_risk['policy_number'].dropna())
        
        matched = claim_policies & risk_policies
        total = len(claim_policies)
        
        if total == 0:
            return self._empty_result(check_id, check_name)
        
        pass_rate = (len(matched) / total * 100)
        
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': pass_rate,
            'passed': len(matched),
            'failed': total - len(matched),
            'total': total,
            'points_possible': self.check_points.get(check_id, 0),
            'points_earned': self.check_points.get(check_id, 0) if pass_rate == 100 else 0,
            'is_risk_check': True,
            'matches_risk': pass_rate == 100
        }
    
    def _empty_result(self, check_id, check_name, note='No data to validate'):
        """Return result for empty/skipped check"""
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': 100.0,
            'passed': 0,
            'failed': 0,
            'total': 0,
            'points_possible': self.check_points[check_id],
            'points_earned': self.check_points[check_id],
            'note': note
        }
    
    def _warning_result(self, check_id, check_name, warning):
        """Return warning result"""
        return {
            'check_id': check_id,
            'check_name': check_name,
            'pass_rate': 0.0,
            'passed': 0,
            'failed': 0,
            'total': 0,
            'points_possible': self.check_points[check_id],
            'points_earned': 0,
            'warning': warning
        }
    
    def _calculate_scores(self, all_results):
        """Calculate overall score"""
        total_possible = sum(r['points_possible'] for r in all_results.values() if not r.get('is_risk_check', False))
        total_earned = sum(r['points_earned'] for r in all_results.values() if not r.get('is_risk_check', False))
        
        # Overall score out of 100
        overall_score = (total_earned / total_possible * 100) if total_possible > 0 else 0
        
        return {
            'overall_score': overall_score,
            'total_possible': total_possible,
            'total_earned': total_earned
        }
    
    def _get_status(self, score):
        """Get status based on score"""
        if score >= 95:
            return 'EXCELLENT'
        elif score >= 85:
            return 'GOOD'
        elif score >= 75:
            return 'FAIR'
        elif score >= 65:
            return 'POOR'
        else:
            return 'CRITICAL'
    
    def _create_summary(self, all_results, scores):
        """Create summary"""
        total_checks = len([r for r in all_results.values() if not r.get('is_risk_check', False)])
        passed = sum(1 for r in all_results.values() if not r.get('is_risk_check', False) and r['points_earned'] == r['points_possible'])
        failed = sum(1 for r in all_results.values() if not r.get('is_risk_check', False) and r['points_earned'] == 0)
        warnings = total_checks - passed - failed
        
        return {
            'total_checks': total_checks,
            'passed': passed,
            'warnings': warnings,
            'failed': failed
        }