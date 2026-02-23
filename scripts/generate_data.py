import pandas as pd
import random
import os

# Constants
OUTPUT_DIR = "/opt/airflow/data/raw"
OUTPUT_FILE = "transactions.csv"
NUM_ROWS = 1000

def generate_mock_data():
    """Generates a mock dataset resembling the PaySim financial dataset."""
    print(f"Generating {NUM_ROWS} mock transactions...")
    
    data = []
    types = ['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN']
    
    for _ in range(NUM_ROWS):
        step = 1
        txn_type = random.choice(types)
        amount = round(random.uniform(10.0, 100000.0), 2)
        nameOrig = f"C{random.randint(1000000, 9999999)}"
        oldbalanceOrg = round(random.uniform(0.0, 50000.0), 2)
        newbalanceOrig = round(oldbalanceOrg - amount if txn_type in ['PAYMENT', 'TRANSFER', 'CASH_OUT'] else oldbalanceOrg + amount, 2)
        nameDest = f"M{random.randint(1000000, 9999999)}"
        oldbalanceDest = round(random.uniform(0.0, 50000.0), 2)
        newbalanceDest = round(oldbalanceDest + amount if txn_type in ['CASH_IN'] else oldbalanceDest - amount, 2)
        isFraud = 1 if amount > 90000 and txn_type == 'TRANSFER' else 0
        isFlaggedFraud = 0
        
        data.append([step, txn_type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud])

    columns = ['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig', 'nameDest', 'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud']
    df = pd.DataFrame(data, columns=columns)
    
    # Ensure directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(output_path, index=False)
    print(f"Mock data saved to {output_path}")

if __name__ == "__main__":
    generate_mock_data()
