import pandas as pd
import os

# Constants
INPUT_FILE = "/opt/airflow/data/raw/transactions.csv"
OUTPUT_DIR = "/opt/airflow/data/processed"
OUTPUT_FILE = "processed_transactions.csv"

def process_data():
    """Reads raw data, applies transformations, and saves processed data."""
    print("Starting data processing...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} rows.")

    # Transformation 1: Filter only high-value transactions (> 50,000)
    high_value_txns = df[df['amount'] > 50000].copy()
    
    # Transformation 2: Add a new column 'is_suspicious'
    # Suspicious if type is TRANSFER and amount > 80,000
    high_value_txns['is_suspicious'] = high_value_txns.apply(
        lambda row: 1 if (row['type'] == 'TRANSFER' and row['amount'] > 80000) else 0, axis=1
    )

    # Transformation 3: Select specific columns for output
    final_df = high_value_txns[['step', 'type', 'amount', 'nameOrig', 'nameDest', 'isFraud', 'is_suspicious']]

    # Save to processed folder
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    
    final_df.to_csv(output_path, index=False)
    print(f"Processed data saved to {output_path} with {len(final_df)} rows.")

if __name__ == "__main__":
    process_data()
