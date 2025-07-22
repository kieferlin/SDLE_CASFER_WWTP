import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib
# Use a non-interactive backend for Matplotlib, crucial for running on servers
# without a graphical user interface (GUI).
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import argparse
import logging
import sys
import gc # Import garbage collector for manual memory management
from sklearn.metrics import mean_absolute_error, mean_squared_error

# ==============================================================================
# Script: model_nitrogen.py
#
# Description:
#   A comprehensive machine learning pipeline to forecast water quality
#   parameter levels using an XGBoost model. The script is driven by command-
#   line arguments, making it flexible for experimentation.
#
#   Pipeline Steps:
#   1. Loads a specific subset of data from a partitioned Parquet dataset
#      using efficient filters (predicate pushdown).
#   2. Preprocesses the data by handling duplicates and resampling to a
#      consistent daily time series.
#   3. Engineers a rich set of features (lags, rolling stats, time-based).
#   4. Splits the data chronologically into training and testing sets.
#   5. Trains an XGBoost regression model with early stopping.
#   6. Evaluates the model and saves performance metrics, prediction plots,
#      and feature importance charts to a unique, named output directory.
#
# Usage:
#   This script is intended to be called from a batch script (like the
#   accompanying .sh file). Direct usage:
#   python3 model_nitrogen.py --data-path <path_to_parquet> \
#                             --output-dir <base_results_dir> \
#                             --run-name <experiment_name> \
#                             --parameter-desc "Parameter Name"
#
# Dependencies:
#   - pandas, numpy, xgboost, matplotlib, scikit-learn, pyarrow
# ==============================================================================


def setup_logging(output_dir: str):
    """
    Configures logging to write to both a file and the console.
    This is crucial for capturing all output in automated or background jobs.

    Args:
        output_dir: The directory where the 'pipeline.log' file will be saved.
    """
    log_file = os.path.join(output_dir, 'pipeline.log')
    # Get the root logger and remove any existing handlers to avoid duplicates.
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    # Configure logging to capture all messages from INFO level and above.
    root.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Add a handler to write logs to the specified file.
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Add a handler to stream logs to the console (stdout).
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

def main():
    """
    Main function to orchestrate the entire forecasting pipeline, from data
    loading and preparation to model training, evaluation, and result saving.
    """
    # === Argument Parsing ===
    # Set up a parser to accept command-line arguments, making the script flexible.
    parser = argparse.ArgumentParser(description="Nitrogen Level Forecasting Pipeline using XGBoost.")
    parser.add_argument('--data-path', type=str, required=True, help="The root path to the partitioned Parquet dataset.")
    parser.add_argument('--output-dir', type=str, required=True, help="The base directory where all experiment results will be saved.")
    parser.add_argument('--run-name', type=str, required=True, help="A unique name for this experiment run, used to create a subdirectory for outputs.")
    parser.add_argument('--parameter-desc', type=str, required=True, help="The exact 'PARAMETER_DESC' string to filter and model.")
    args = parser.parse_args()

    # Create a unique output directory for this specific run to keep results organized.
    output_dir = os.path.join(args.output_dir, args.run_name)
    os.makedirs(output_dir, exist_ok=True)

    # === Setup Logging ===
    # All subsequent print statements will be managed by the logging framework.
    setup_logging(output_dir)
    logging.info(f"STARTING RUN: {args.run_name}")
    logging.info(f"TARGETING PARAMETER: '{args.parameter_desc}'")
    logging.info(f"Output will be saved to: {output_dir}")

    # === Step 1: Load Data with Efficient Filtering ===
    logging.info("\n--- 1. Loading and Preparing Data ---")
    # Define filters to apply during the read operation. This is called "predicate
    # pushdown" and is extremely efficient as it avoids loading unnecessary data
    # from the Parquet files into memory.
    unit_filter = ('DMR_UNIT_DESC', '=', 'mg/L')
    specific_param_filter = ('PARAMETER_DESC', '=', args.parameter_desc)

    combined_filters = [unit_filter, specific_param_filter]
    logging.info(f"Loading partitioned data from '{args.data_path}' with filters: {combined_filters}")
    try:
        # Use pandas with the 'pyarrow' engine to read the partitioned dataset.
        df = pd.read_parquet(args.data_path, filters=combined_filters, engine='pyarrow')
        logging.info(f"Successfully loaded FILTERED dataset with {len(df)} rows for '{args.parameter_desc}'.")
    except Exception as e:
        logging.error(f"Failed to load Parquet dataset. Check path and filters. Error: {e}", exc_info=True)
        sys.exit(1) # Exit if data cannot be loaded.

    # Check if the filtering resulted in an empty DataFrame.
    if df.empty:
        logging.warning(f"The filters resulted in an empty dataframe for parameter '{args.parameter_desc}'. No data to model. Exiting.")
        return

    # --- Data Cleaning and Type Conversion ---
    nitrogen_df = df
    nitrogen_df['date'] = pd.to_datetime(nitrogen_df['MONITORING_PERIOD_END_DATE'], errors='coerce')
    nitrogen_df['value'] = pd.to_numeric(nitrogen_df['DMR_VALUE_NMBR'], errors='coerce', downcast='float')
    nitrogen_df.dropna(subset=['date', 'value'], inplace=True) # Drop rows where essential values are missing
    nitrogen_df.set_index('date', inplace=True)
    nitrogen_df.sort_index(inplace=True)
    irregular_series = nitrogen_df['value']
    logging.info(f"Created clean series with {len(irregular_series)} irregular data points.")
    # Free up memory by deleting the original large DataFrames.
    del df, nitrogen_df
    gc.collect()

    # === Step 2a: Aggregate Duplicates & Resample to a Regular Time Index ===
    logging.info("\n--- 2a. Aggregating Duplicates and Resampling to Daily Frequency ---")
    # It's common to have multiple readings for the same parameter on the same day.
    # We aggregate these by taking the mean to get a single value per day.
    daily_series_agg = irregular_series.resample('D').mean()
    logging.info(f"Resampled to daily frequency. Series now has {len(daily_series_agg)} points, including NaNs for missing days.")
    # Fill in the gaps (NaNs) created by resampling. `ffill` carries the last valid
    # observation forward, and `bfill` fills any remaining NaNs at the start.
    series = daily_series_agg.ffill().bfill()
    logging.info(f"Forward/backward filled missing days. Final series has {len(series)} daily data points.")
    del irregular_series, daily_series_agg
    gc.collect()

    # === Step 2b: Feature Engineering ===
    logging.info("\n--- 2b. Engineering Time-Series Features ---")
    # Create a new DataFrame to hold the target variable and all engineered features.
    features_df = pd.DataFrame(index=series.index)
    features_df['target'] = series

    # Lag features: Use past values of the series as features.
    for i in range(1, 8):
        features_df[f'lag_{i}'] = series.shift(i)

    # Rolling window features: Capture trends and volatility over different time horizons.
    features_df['rolling_mean_7'] = series.shift(1).rolling(window=7).mean()
    features_df['rolling_std_7'] = series.shift(1).rolling(window=7).std()
    features_df['rolling_mean_3'] = series.shift(1).rolling(window=3).mean()
    features_df['rolling_mean_30'] = series.shift(1).rolling(window=30).mean()

    # Difference feature: Captures the change from the previous day.
    features_df['lag_1_diff'] = series.shift(1).diff()

    # Time-based features: Help the model capture seasonality and cyclical patterns.
    features_df['day_of_week'] = series.index.dayofweek
    features_df['week_of_year'] = series.index.isocalendar().week.astype(int)
    features_df['month'] = series.index.month
    features_df['month_day_interaction'] = (features_df['month'].astype(str) + "_" + features_df['day_of_week'].astype(str)).astype('category').cat.codes

    # Drop rows with NaN values created by the shifting and rolling operations.
    features_df = features_df.dropna()
    logging.info(f"Created feature set with shape: {features_df.shape}")
    del series
    gc.collect()

    # === Step 3: Splitting the Data ===
    logging.info("\n--- 3. Splitting Data into Training and Testing Sets ---")
    # For time series, the split must be chronological to simulate a real forecasting scenario.
    y = features_df['target']
    X = features_df.drop('target', axis=1)
    split_index = int(len(X) * 0.8)
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]
    logging.info(f"Train set size: {len(X_train)}, Test set size: {len(X_test)}")

    # === Step 4: Training the XGBoost Model ===
    logging.info("\n--- 4. Training Model ---")
    model_xgb = xgb.XGBRegressor(
        n_estimators=1000,          # Set a high number of trees; early stopping will find the optimal number.
        learning_rate=0.05,
        early_stopping_rounds=10,   # Stop training if the validation score doesn't improve for 10 consecutive rounds.
        tree_method='hist'          # Use a fast histogram-based algorithm.
    )
    # Train the model on the training data and use the test set for early stopping.
    model_xgb.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False # Suppress per-iteration output.
    )
    logging.info("Model training complete.")

    # === Step 5: Evaluating Model Performance ===
    logging.info("\n--- 5. Evaluating Model ---")
    predictions_xgb = model_xgb.predict(X_test)
    mae = mean_absolute_error(y_test, predictions_xgb)
    rmse = np.sqrt(mean_squared_error(y_test, predictions_xgb))
    logging.info(f"Model Performance on Test Set for '{args.parameter_desc}':")
    logging.info(f"  Mean Absolute Error (MAE): {mae:.4f}")
    logging.info(f"  Root Mean Squared Error (RMSE): {rmse:.4f}")

    # --- Plot 1: Predictions vs. Actuals ---
    plt.figure(figsize=(15, 7))
    plt.plot(y_test.index, y_test, color='green', label=f'Actual: {args.parameter_desc}')
    plt.plot(y_test.index, predictions_xgb, color='orange', linestyle='--', label=f'Predicted (XGBoost)\nMAE: {mae:.2f}, RMSE: {rmse:.2f}')
    plt.title(f'Prediction for {args.parameter_desc} - Run: {args.run_name}')
    plt.ylabel('Nitrogen (mg/L)')
    plt.legend()
    plt.grid(True)
    prediction_path = os.path.join(output_dir, 'prediction_vs_actual.png')
    plt.savefig(prediction_path)
    plt.close() # Close the plot to free up memory.
    logging.info(f"Saved prediction plot to '{prediction_path}'")

    # --- Plot 2: Feature Importance ---
    fig, ax = plt.subplots(figsize=(10, 8))
    xgb.plot_importance(model_xgb, ax=ax, height=0.9)
    ax.set_title(f'Feature Importance for {args.parameter_desc} - Run: {args.run_name}')
    fig.tight_layout() # Adjust layout to prevent labels from overlapping.
    importance_path = os.path.join(output_dir, 'feature_importance.png')
    plt.savefig(importance_path)
    plt.close()
    logging.info(f"Saved feature importance plot to '{importance_path}'")

    # --- Save data for offline analysis ---
    logging.info("Saving test data, predictions, and features for deeper analysis...")
    analysis_df = X_test.copy()
    analysis_df['y_test'] = y_test
    analysis_df['prediction'] = predictions_xgb
    analysis_output_path = os.path.join(output_dir, 'analysis_data.parquet')
    analysis_df.to_parquet(analysis_output_path)
    logging.info(f"Saved analysis data to '{analysis_output_path}'")

    logging.info(f"\nPipeline for run '{args.run_name}' finished successfully.")


if __name__ == '__main__':
    main()