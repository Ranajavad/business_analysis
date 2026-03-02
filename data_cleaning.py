import pandas as pd
from sqlalchemy import create_engine
import pathlib
import time

# 1. Automatic Path Discovery
current_dir = pathlib.Path(__file__).parent.resolve()
file_path = current_dir / 'duolingo.csv' # Make sure this filename is exact!

# 2. Database Connection
engine = create_engine('postgresql://postgres:1905@localhost:5432/duolingo')

print(f"Checking for file at: {file_path}")

if not file_path.exists():
    print(f"Error: {file_path.name} not found in this folder!")
else:
    print("File found. Starting the 13M row cleaning and upload...")
    start_time = time.time()

    # 3. Load and Stream in Chunks
    chunk_size = 100000 
    df_iter = pd.read_csv(file_path, chunksize=chunk_size)

    for i, chunk in enumerate(df_iter):
        
        # --- [CLEANING STARTS HERE] ---
        # 1. Convert weird numbers to real Dates
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s')
        
        # 2. Add a 'day_of_week' column for Business Analysis
        chunk['day_of_week'] = chunk['timestamp'].dt.day_name()

        # 3. Clean 'apple/apple<n>' -> 'apple'
        chunk['lexeme_string'] = chunk['lexeme_string'].str.split('/').str[0]

        # 4. Remove unnecessary columns (saves massive space in SQL)
        if 'lexeme_id' in chunk.columns:
            chunk = chunk.drop(columns=['lexeme_id'])
            
        # 5. Fill missing values
        chunk[['session_correct', 'session_seen']] = chunk[['session_correct', 'session_seen']].fillna(0)
        # --- [CLEANING ENDS HERE] ---
        
        # 4. Upload Step
        mode = 'replace' if i == 0 else 'append'
        chunk.to_sql('duolingo', engine, index=False, if_exists=mode)
        
        # Progress tracker
        if (i + 1) % 5 == 0: # Prints every 500,000 rows
            rows_processed = (i + 1) * chunk_size
            print(f"✅ Processed and Uploaded ~{rows_processed:,} rows...")

    end_time = time.time()
    total_min = round((end_time - start_time) / 60, 2)
    print(f"🏁 Finished! 13M rows uploaded and cleaned in {total_min} minutes.")