import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
import numpy as np
import os
import argparse
import logging
import sys

# ==============================================================================
# Script: model_oh_nitrogen.py
#
# Description:
#   A two-stage modeling pipeline focused on facilities in a specific state.
#
#   Stage 1: Identifies the top N facilities by calculated average daily
#   nitrogen load. Load is calculated by joining nitrogen concentration data
#   from DMR reports with flow data from both DMR reports and ICIS permits.
#
#   Stage 2: For each top facility identified, it trains, evaluates, and
#   visualizes a separate XGBoost time-series model to forecast future
#   monthly nitrogen concentration.
#
# Usage:
#   This script is controlled via command-line arguments and is intended
#   to be executed by a wrapper script (e.g., model_oh_nitrogen_bash.sh).
#
# Dependencies:
#   - pandas, xgboost, scikit-learn, matplotlib, numpy, pyarrow
# ==============================================================================

def setup_logging(log_path: str):
    """Configures logging to write to a file and to the console."""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_path, mode='w'), logging.StreamHandler(sys.stdout)]
    )

def get_top_facilities_by_load(dmr_path, permits_path, state, n_param, f_param, n):
    """Identifies the top N facilities by average daily nitrogen load."""
    logging.info(f"--- 1. Identifying Top {n} Facilities in {state} by Nitrogen Load ---")
    
    dmr_cols = ['EXTERNAL_PERMIT_NMBR', 'PARAMETER_DESC', 'DMR_VALUE_STANDARD_UNITS', 'state']
    permit_cols = ['EXTERNAL_PERMIT_NMBR', 'ACTUAL_AVERAGE_FLOW_NMBR']
    state_filter = [('state', '==', state)]

    logging.info(f"Loading DMR data for state '{state}'...")
    df_dmr = pd.read_parquet(dmr_path, engine='pyarrow', columns=dmr_cols, filters=state_filter)
    
    df_nitrogen = df_dmr[df_dmr['PARAMETER_DESC'] == n_param].copy()
    df_nitrogen.rename(columns={'DMR_VALUE_STANDARD_UNITS': 'Nitrogen_mg_L'}, inplace=True)
    
    df_flow = df_dmr[df_dmr['PARAMETER_DESC'] == f_param].copy()
    df_flow.rename(columns={'DMR_VALUE_STANDARD_UNITS': 'Flow_MGD'}, inplace=True)

    state_permit_ids = df_dmr['EXTERNAL_PERMIT_NMBR'].unique()
    del df_dmr
    
    logging.info("Loading permit data for flow imputation...")
    df_permits_all = pd.read_parquet(permits_path, engine='pyarrow', columns=permit_cols)
    df_permits_state = df_permits_all[df_permits_all['EXTERNAL_PERMIT_NMBR'].isin(state_permit_ids)].copy()
    del df_permits_all
    
    logging.info("Merging data and calculating load...")
    df_merged = pd.merge(df_nitrogen[['EXTERNAL_PERMIT_NMBR', 'Nitrogen_mg_L']],
                         df_flow[['EXTERNAL_PERMIT_NMBR', 'Flow_MGD']],
                         on='EXTERNAL_PERMIT_NMBR', how='left')
    df_final = pd.merge(df_merged, df_permits_state, on='EXTERNAL_PERMIT_NMBR', how='left')
    
    df_final['Flow_MGD'] = df_final['Flow_MGD'].fillna(df_final['ACTUAL_AVERAGE_FLOW_NMBR'])
    df_final['Nitrogen_mg_L'] = pd.to_numeric(df_final['Nitrogen_mg_L'], errors='coerce')
    df_final['Flow_MGD'] = pd.to_numeric(df_final['Flow_MGD'], errors='coerce')
    df_final.dropna(subset=['Nitrogen_mg_L', 'Flow_MGD'], inplace=True)
    
    df_final['Nitrogen_Load_kg_day'] = df_final['Nitrogen_mg_L'] * df_final['Flow_MGD'] * 3.78541
    
    top_facilities_by_load = (
        df_final.groupby('EXTERNAL_PERMIT_NMBR')['Nitrogen_Load_kg_day']
        .mean()
        .nlargest(n)
    )
    
    logging.info("\n--- Top Facilities Identified by Load ---")
    for facility, avg_load in top_facilities_by_load.items():
        logging.info(f"  - {facility} (Avg Load: {avg_load:.2f} kg/day)")
    
    return top_facilities_by_load.index.tolist()

def create_features(df, target_col='target'):
    """Creates a rich set of time-series features from a datetime index."""
    df = df.copy()
    df['month'] = df.index.month
    df['year'] = df.index.year
    df['quarter'] = df.index.quarter
    df['lag_1'] = df[target_col].shift(1)
    df['lag_3'] = df[target_col].shift(3)
    df['lag_12'] = df[target_col].shift(12)
    df['rolling_mean_3'] = df[target_col].rolling(window=3).mean()
    df['rolling_std_3'] = df[target_col].rolling(window=3).std()
    df['rolling_mean_12'] = df[target_col].rolling(window=12).mean()
    return df

def main():
    """Main function to orchestrate the full modeling process."""
    parser = argparse.ArgumentParser(description="Multi-facility nitrogen forecasting pipeline.")
    parser.add_argument('--dmr-data-path', required=True, help="Path to the DMR Parquet dataset.")
    parser.add_argument('--permits-data-path', required=True, help="Path to the ICIS permits Parquet dataset.")
    parser.add_argument('--output-dir', required=True, help="Parent directory for run results.")
    parser.add_argument('--run-name', required=True, help="Unique name for this run's subdirectory.")
    parser.add_argument('--parameter-desc', required=True, help="The PARAMETER_DESC for the target pollutant.")
    args = parser.parse_args()

    run_output_dir = os.path.join(args.output_dir, args.run_name)
    os.makedirs(run_output_dir, exist_ok=True)
    setup_logging(os.path.join(run_output_dir, 'pipeline.log'))

    logging.info(f"STARTING RUN: {args.run_name}")
    logging.info(f"Output saved to: {run_output_dir}")

    TARGET_STATE = 'OH'
    FLOW_PARAM_DESC = 'Flow, in conduit or thru treatment plant'
    TOP_N_FACILITIES = 5

    top_facilities = get_top_facilities_by_load(
        dmr_path=args.dmr_data_path, permits_path=args.permits_data_path,
        state=TARGET_STATE, n_param=args.parameter_desc,
        f_param=FLOW_PARAM_DESC, n=TOP_N_FACILITIES
    )

    logging.info("\n--- 2. Loading State Data for Concentration Modeling ---")
    df_state_all = pd.read_parquet(args.dmr_data_path, engine='pyarrow', filters=[('state', '==', TARGET_STATE)])
    df_target_param = df_state_all[df_state_all['PARAMETER_DESC'] == args.parameter_desc].copy()
    df_target_param['MONITORING_PERIOD_END_DATE'] = pd.to_datetime(df_target_param['MONITORING_PERIOD_END_DATE'])
    del df_state_all

    logging.info(f"\n--- 3. Starting Modeling for {len(top_facilities)} Facilities ---")
    for facility_id in top_facilities:
        logging.info(f"\n--- Processing Facility: {facility_id} ---")

        facility_df = df_target_param[df_target_param['EXTERNAL_PERMIT_NMBR'] == facility_id].copy()
        facility_df.set_index('MONITORING_PERIOD_END_DATE', inplace=True)
        facility_df['target'] = pd.to_numeric(facility_df['DMR_VALUE_STANDARD_UNITS'], errors='coerce')
        facility_df.dropna(subset=['target'], inplace=True)
        
        monthly_df = facility_df[['target']].resample('MS').mean()
        monthly_df.dropna(inplace=True)

        if len(monthly_df) < 24:
            logging.warning(f"Skipping {facility_id} due to insufficient data ({len(monthly_df)} monthly points).")
            continue
            
        monthly_df_featured = create_features(monthly_df, target_col='target')
        monthly_df_featured.dropna(inplace=True)
        
        if len(monthly_df_featured) < 12:
            logging.warning(f"Skipping {facility_id} due to insufficient data after feature creation ({len(monthly_df_featured)} points).")
            continue

        FEATURES = [
            'month', 'year', 'quarter', 'lag_1', 'lag_3', 'lag_12',
            'rolling_mean_3', 'rolling_std_3', 'rolling_mean_12'
        ]
        TARGET = 'target'
        X, y = monthly_df_featured[FEATURES], monthly_df_featured[TARGET]

        test_size = max(1, int(len(y) * 0.2))
        X_train, X_test = X[:-test_size], X[-test_size:]
        y_train, y_test = y[:-test_size], y[-test_size:]
        logging.info(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
        
        reg = xgb.XGBRegressor(n_estimators=1000, early_stopping_rounds=50, objective='reg:squarederror', eval_metric='rmse')
        reg.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_test, y_test)], verbose=False)
        
        y_pred = reg.predict(X_test)
        mae, rmse = mean_absolute_error(y_test, y_pred), np.sqrt(mean_squared_error(y_test, y_pred))
        logging.info(f"Facility {facility_id} - MAE: {mae:.4f}, RMSE: {rmse:.4f}")
        
        plt.figure(figsize=(15, 7))
        plt.plot(y_test.index, y_test, 'go--', label=f'Actual: {args.parameter_desc}')
        plt.plot(y_test.index, y_pred, 'x--', color='orange', label=f'Predicted (XGBoost)\nMAE: {mae:.2f}, RMSE: {rmse:.2f}')
        plt.title(f'Ohio Facility {facility_id} - Monthly Prediction', fontsize=16)
        plt.ylabel(f'{args.parameter_desc} (mg/L)', fontsize=12)
        plt.grid(True), plt.legend()
        plt.savefig(os.path.join(run_output_dir, f'prediction_{facility_id}.png')), plt.close()

        plt.figure(figsize=(10, 6))
        pd.Series(reg.feature_importances_, index=X.columns).sort_values().plot(kind='barh')
        plt.title(f'Feature Importance for {facility_id}'), plt.xlabel('Importance')
        plt.tight_layout()
        plt.savefig(os.path.join(run_output_dir, f'feature_importance_{facility_id}.png')), plt.close()

    logging.info("\nOhio load-based forecasting pipeline finished successfully.")

if __name__ == "__main__":
    main()