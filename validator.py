# ============================================================================
# FILE: validator.py
# Simplified validation engine with 10 checks only
# ============================================================================

import pandas as pd
import yaml
from datetime import datetime
import numpy as np

class DataQualityValidator:
    """Simplified validation engine for 10 checks"""
    
    def __init__(self, config_or_dict):
        """Initialize validator with configuration"""
        if isinstance(config_or_dict, str):
            # Load from file
            with open(config_or_dict, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            # Use dict directly
            self.config = config_or_dict
        
        self.results = {}
        self.scores = {}
        
    def validate(self, df, column_mappings, quarter_id=None, risk_df=None):
        """
        Run all validation checks on dataframe
        
        Args:
            df: pandas DataFrame with claims data
            column_mappings: dict mapping logical names to actual column names
            quarter_id: string like '2024_Q3'
            risk_df: optional pandas DataFrame with risk data
            
        Returns:
            dict with validation results and scores
        """
        print("🔍 Starting Data Quality Validation...")
        print(f"   Records to validate: {len(df):,}")
        
        # Rename columns to logical names
        df_mapped = df.rename(columns={v: k for k, v in column_mappings.items()})
        
        # Run all checks
        all_results = self._run_all_checks(df_mapped, risk_df, column_mappings)
        
        # Calculate scores
        scores = self._calculate_scores(all_results)
        
        # Compile results
        final_results = {
            'quarter_id': quarter_id or datetime.now().strftime('%Y_Q%m'),
            'timestamp': datetime.now().isoformat(),
            'total_records': len(df),
            'overall_score': scores['overall_score'],
            'status': self._get_status(scores['overall_score']),
            'category_scores': scores['category_scores'],
            'detailed_results': all_results,
            'summary': self._create_summary(all_results, scores)
        }
        
        self.results = final_results
        self.scores = scores
        
        print(f"\n✅ Validation Complete!")
        print(f"   Overall Score: {scores['overall_score']:.1f}/100")
        print(f"   Status: {final_results['status']}")
        
        return final_results
    
    def _run_all_checks(self, df, risk_df, column_mappings):
        """Run all 10 checks"""
        all_results = {
            'completeness': {'critical_fields': []},
            'consistency': {'financial': []},
            'reasonableness': {'value_ranges': []},
            'data_quality': {'duplicates': [], 'format': []},
            'temporal_logic': {'date_sequence': []}
        }
        
        # CHECK 1.1: Policy Number Complete
        result = self._check_not_null(df, 'policy_number')
        result.update({
            'check_id': '1.1',
            'check_name': 'Policy Number Complete',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': 3 if result['pass_rate'] == 100 else 0
        })
        all_results['completeness']['critical_fields'].append(result)
        
        # CHECK 1.2: Claim Number Complete
        result = self._check_not_null(df, 'claim_number')
        result.update({
            'check_id': '1.2',
            'check_name': 'Claim Number Complete',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': 3 if result['pass_rate'] == 100 else 0
        })
        all_results['completeness']['critical_fields'].append(result)
        
        # CHECK 2.1: Gross Incurred Formula
        result = self._check_gross_formula(df)
        result.update({
            'check_id': '2.1',
            'check_name': 'Gross Incurred Formula',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': self._score_by_threshold(result['pass_rate'], {98: 3, 95: 2, 90: 1})
        })
        all_results['consistency']['financial'].append(result)
        
        # CHECK 2.3: Net <= Gross
        result = self._check_net_gross(df)
        result.update({
            'check_id': '2.3',
            'check_name': 'Net <= Gross Relationship',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': self._score_by_threshold(result['pass_rate'], {99: 3, 97: 2, 95: 1})
        })
        all_results['consistency']['financial'].append(result)
        
        # CHECK 3.1: Non-Negative Values
        result = self._check_non_negative(df)
        result.update({
            'check_id': '3.1',
            'check_name': 'Non-Negative Financial Values',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': self._score_by_threshold(result['pass_rate'], {99.5: 3, 99: 2, 98: 1})
        })
        all_results['reasonableness']['value_ranges'].append(result)
        
        # CHECK 3.2: Accident Year Range
        result = self._check_accident_year_range(df)
        result.update({
            'check_id': '3.2',
            'check_name': 'Accident Year Reasonable Range',
            'priority': 'high',
            'points_possible': 2,
            'points_earned': self._score_by_threshold(result['pass_rate'], {99.9: 2, 99: 1})
        })
        all_results['reasonableness']['value_ranges'].append(result)
        
        # CHECK 4.1: Unique Claim Numbers
        result = self._check_duplicates(df)
        result.update({
            'check_id': '4.1',
            'check_name': 'Unique Claim Numbers',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': self._score_by_threshold(result['pass_rate'], {100: 3, 99: 2, 97: 1})
        })
        all_results['data_quality']['duplicates'].append(result)
        
        # CHECK 4.3: Valid Date Formats
        result = self._check_date_formats(df)
        result.update({
            'check_id': '4.3',
            'check_name': 'Valid Date Formats',
            'priority': 'high',
            'points_possible': 2,
            'points_earned': self._score_by_threshold(result['pass_rate'], {100: 2, 99: 1})
        })
        all_results['data_quality']['format'].append(result)
        
        # CHECK 5.2: Accident Before Notification
        result = self._check_date_sequence(df)
        result.update({
            'check_id': '5.2',
            'check_name': 'Accident Before Notification',
            'priority': 'critical',
            'points_possible': 3,
            'points_earned': self._score_by_threshold(result['pass_rate'], {98: 3, 95: 2, 92: 1})
        })
        all_results['temporal_logic']['date_sequence'].append(result)
        
        # CHECK 5.6: Accident Year Matches Date
        result = self._check_ay_matches_date(df)
        result.update({
            'check_id': '5.6',
            'check_name': 'Accident Year Matches Date',
            'priority': 'high',
            'points_possible': 2,
            'points_earned': self._score_by_threshold(result['pass_rate'], {99.9: 2, 99: 1})
        })
        all_results['temporal_logic']['date_sequence'].append(result)
        
        # CHECK 11.1: GWP Reconciliation (if risk data provided)
        if risk_df is not None:
            result = self._check_gwp_reconciliation(df, risk_df, column_mappings)
            result.update({
                'check_id': '11.1',
                'check_name': 'Total Incurred vs GWP Reconciliation',
                'priority': 'high',
                'points_possible': 3,
                'points_earned': self._score_by_threshold(result['pass_rate'], {95: 3, 90: 2, 85: 1})
            })
            # Add to consistency
            all_results['consistency']['financial'].append(result)
        
        return all_results
    
    # Individual check functions
    
    def _check_not_null(self, df, field):
        """Check if field is not null"""
        total = len(df)
        not_null_count = df[field].notna().sum()
        pass_rate = (not_null_count / total) * 100
        failed_records = df[df[field].isna()].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': not_null_count,
            'failed': total - not_null_count,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_gross_formula(self, df):
        """Check Gross_Incurred = Gross_Paid + Gross_Estimate"""
        df_check = df.dropna(subset=['gross_incurred', 'gross_paid', 'gross_estimate'])
        
        if len(df_check) == 0:
            return {'pass_rate': 0, 'passed': 0, 'failed': 0, 'total': 0, 'failed_records': []}
        
        diff = abs(df_check['gross_incurred'] - (df_check['gross_paid'] + df_check['gross_estimate']))
        passed_mask = diff <= 1
        
        total = len(df_check)
        passed = passed_mask.sum()
        pass_rate = (passed / total) * 100
        failed_records = df_check[~passed_mask].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_net_gross(self, df):
        """Check Net <= Gross"""
        df_check = df.dropna(subset=['net_paid', 'gross_paid', 'net_incurred', 'gross_incurred'])
        
        if len(df_check) == 0:
            return {'pass_rate': 0, 'passed': 0, 'failed': 0, 'total': 0, 'failed_records': []}
        
        mask = (df_check['net_paid'] <= df_check['gross_paid']) & \
               (df_check['net_incurred'] <= df_check['gross_incurred'])
        
        total = len(df_check)
        passed = mask.sum()
        pass_rate = (passed / total) * 100
        failed_records = df_check[~mask].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_non_negative(self, df):
        """Check all financial fields are non-negative"""
        financial_cols = ['gross_paid', 'gross_incurred', 'net_paid', 'net_incurred']
        
        all_passed = pd.Series([True] * len(df))
        
        for col in financial_cols:
            if col in df.columns:
                all_passed &= (df[col] >= 0) | (df[col].isna())
        
        total = len(df)
        passed = all_passed.sum()
        pass_rate = (passed / total) * 100
        failed_records = df[~all_passed].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_accident_year_range(self, df):
        """Check accident year is within reasonable range"""
        current_year = datetime.now().year
        min_year = current_year - 15
        max_year = current_year
        
        mask = (df['accident_year'] >= min_year) & (df['accident_year'] <= max_year)
        
        total = len(df)
        passed = mask.sum()
        pass_rate = (passed / total) * 100
        failed_records = df[~mask].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_duplicates(self, df):
        """Check for duplicate claim numbers"""
        duplicates = df.duplicated(subset=['claim_number'], keep=False)
        
        total = len(df)
        duplicate_count = duplicates.sum()
        pass_rate = ((total - duplicate_count) / total) * 100
        failed_records = df[duplicates].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': total - duplicate_count,
            'failed': duplicate_count,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_date_formats(self, df):
        """Check if dates are valid"""
        date_cols = ['accident_date', 'notification_date']
        
        total = len(df)
        all_valid = pd.Series([True] * total)
        
        for col in date_cols:
            if col in df.columns:
                try:
                    pd.to_datetime(df[col], errors='coerce')
                    valid = pd.to_datetime(df[col], errors='coerce').notna()
                    all_valid &= valid | df[col].isna()
                except:
                    all_valid &= False
        
        passed = all_valid.sum()
        pass_rate = (passed / total) * 100
        failed_records = df[~all_valid].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_date_sequence(self, df):
        """Check accident date <= notification date"""
        df_check = df.dropna(subset=['accident_date', 'notification_date'])
        
        if len(df_check) == 0:
            return {'pass_rate': 0, 'passed': 0, 'failed': 0, 'total': 0, 'failed_records': []}
        
        acc_dates = pd.to_datetime(df_check['accident_date'])
        notif_dates = pd.to_datetime(df_check['notification_date'])
        
        mask = acc_dates <= notif_dates
        
        total = len(df_check)
        passed = mask.sum()
        pass_rate = (passed / total) * 100
        failed_records = df_check[~mask].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_ay_matches_date(self, df):
        """Check accident year matches year from accident date"""
        df_check = df.dropna(subset=['accident_year', 'accident_date'])
        
        if len(df_check) == 0:
            return {'pass_rate': 0, 'passed': 0, 'failed': 0, 'total': 0, 'failed_records': []}
        
        acc_dates = pd.to_datetime(df_check['accident_date'])
        mask = df_check['accident_year'] == acc_dates.dt.year
        
        total = len(df_check)
        passed = mask.sum()
        pass_rate = (passed / total) * 100
        failed_records = df_check[~mask].index.tolist()
        
        return {
            'pass_rate': pass_rate,
            'passed': passed,
            'failed': total - passed,
            'total': total,
            'failed_records': failed_records[:100]
        }
    
    def _check_gwp_reconciliation(self, claims_df, risk_df, column_mappings):
        """Check 11: Total Incurred vs GWP from Risk DB"""
        try:
            # Get column names
            gwp_col = column_mappings.get('gwp')
            risk_policy_col = column_mappings.get('risk_policy_number')
            
            # Aggregate claims by policy
            claims_agg = claims_df.groupby('policy_number')['gross_incurred'].sum().reset_index()
            claims_agg.columns = ['policy_number', 'total_incurred']
            
            # Prepare risk data
            risk_df_mapped = risk_df.rename(columns={
                risk_policy_col: 'policy_number',
                gwp_col: 'gwp'
            })
            
            # Merge
            merged = claims_agg.merge(risk_df_mapped[['policy_number', 'gwp']], 
                                     on='policy_number', 
                                     how='inner')
            
            # Check if total incurred is reasonable compared to GWP
            # Typically loss ratio should be between 0-150%
            merged['loss_ratio'] = merged['total_incurred'] / merged['gwp']
            mask = (merged['loss_ratio'] >= 0) & (merged['loss_ratio'] <= 1.5)
            
            total = len(merged)
            passed = mask.sum()
            pass_rate = (passed / total) * 100 if total > 0 else 0
            failed_records = merged[~mask].index.tolist()
            
            return {
                'pass_rate': pass_rate,
                'passed': passed,
                'failed': total - passed,
                'total': total,
                'failed_records': failed_records[:100]
            }
        
        except Exception as e:
            return {
                'pass_rate': 0,
                'passed': 0,
                'failed': 0,
                'total': 0,
                'failed_records': [],
                'error': str(e)
            }
    
    def _score_by_threshold(self, pass_rate, thresholds):
        """Calculate points based on pass rate and thresholds"""
        sorted_thresholds = sorted(thresholds.keys(), reverse=True)
        
        for threshold in sorted_thresholds:
            if pass_rate >= threshold:
                return thresholds[threshold]
        
        return 0
    
    def _calculate_scores(self, all_results):
        """Calculate category and overall scores"""
        category_scores = {}
        
        # Define max points per category
        category_max_points = {
            'completeness': 6,      # 2 checks × 3 points
            'consistency': 6,       # 2 checks × 3 points
            'reasonableness': 5,    # 1×3 + 1×2
            'data_quality': 5,      # 1×3 + 1×2
            'temporal_logic': 5     # 1×3 + 1×2
        }
        
        for category, subcategories in all_results.items():
            total_possible = 0
            total_earned = 0
            
            for subcategory, checks in subcategories.items():
                for check in checks:
                    total_possible += check['points_possible']
                    total_earned += check['points_earned']
            
            category_percentage = (total_earned / total_possible * 100) if total_possible > 0 else 0
            
            category_scores[category] = {
                'points_earned': total_earned,
                'points_possible': total_possible,
                'percentage': category_percentage
            }
        
        # Overall score
        overall_score = sum(cat['points_earned'] for cat in category_scores.values())
        
        return {
            'overall_score': overall_score,
            'category_scores': category_scores
        }
    
    def _get_status(self, score):
        """Get status label based on score"""
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
        """Create summary of validation results"""
        total_checks = 0
        passed_checks = 0
        failed_checks = 0
        warning_checks = 0
        critical_issues = []
        
        for category, subcategories in all_results.items():
            for subcategory, checks in subcategories.items():
                for check in checks:
                    total_checks += 1
                    
                    if check['points_earned'] == check['points_possible']:
                        passed_checks += 1
                    elif check['points_earned'] > 0:
                        warning_checks += 1
                    else:
                        failed_checks += 1
                        
                        if check.get('priority') == 'critical':
                            critical_issues.append({
                                'check_id': check['check_id'],
                                'check_name': check['check_name'],
                                'failed_count': check.get('failed', 0)
                            })
        
        return {
            'total_checks': total_checks,
            'passed': passed_checks,
            'warnings': warning_checks,
            'failed': failed_checks,
            'critical_issues_count': len(critical_issues),
            'critical_issues': critical_issues
        }