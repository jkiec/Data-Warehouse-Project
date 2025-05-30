import pandas as pd
import subprocess
import zipfile
from sqlalchemy import create_engine


# Function to read CSV with multiple encodings
def read_csv_with_multiple_encodings(file_path, encodings=['utf-8', 'latin1', 'ISO-8859-1', 'cp1252']):
    """Read a CSV file with multiple encodings until one works."""
    for encoding in encodings:
        try:
            print(f"Trying to read with {encoding} encoding...")
            df = pd.read_csv(file_path, encoding=encoding)
            print(f"Successfully read with {encoding} encoding")
            return df
        except UnicodeDecodeError:
            print(f"Failed with {encoding} encoding")
            continue

    # If we get here, all encodings failed
    raise ValueError(f"Failed to read {file_path} with any of the encodings: {encodings}")


# PART 1: Download dataset from Kaggle
print("Downloading dataset from Kaggle...")
try:
    # Run kaggle command to download the dataset
    subprocess.run([
        'kaggle', 'datasets', 'download',
        '-d', 'bhavikjikadara/global-electronics-retailers'
    ], check=True)

    print("Unzipping dataset...")
    with zipfile.ZipFile('global-electronics-retailers.zip', 'r') as zip_ref:
        zip_ref.extractall('.')
    print("Dataset downloaded and extracted successfully!")

except subprocess.CalledProcessError as e:
    print(f"Error during download: {e}")
    exit(1)
except zipfile.BadZipFile:
    print("Error: The downloaded file is not a valid zip file")
    exit(1)
except Exception as e:
    print(f"Unexpected error: {e}")
    exit(1)


# PART 2: Load data into PostgreSQL
print("\nLoading data to PostgreSQL...")

# PostgreSQL connection settings
db_user = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'
db_name = 'global_electronics_retailers'

# Create database connection string
conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create SQLAlchemy engine
print("\nConnecting to PostgreSQL...")
engine = create_engine(conn_string)

# List of CSV files to process
csv_files = ['Sales.csv', 'Customers.csv', 'Products.csv', 'Stores.csv']

try:
    # Process each CSV file
    for csv_file in csv_files:
        print(f"\n--- Processing {csv_file} ---")

        # Convert filename to table name - remove .csv extension
        table_name = csv_file.split('.')[0].lower()

        # Read the CSV file into a pandas DataFrame
        print(f"Reading {csv_file}...")
        try:
            # Use the custom function to handle encoding issues
            df = read_csv_with_multiple_encodings(csv_file)

            # Display information about the data
            print(f"\nData preview for {csv_file}:")
            print(df.head(3))  # Show only 3 rows for brevity

            print(f"\nColumn names: {', '.join(df.columns)}")
            print(f"Number of rows: {len(df)}")

            # Load DataFrame to PostgreSQL
            print(f"Loading data to PostgreSQL table '{table_name}'...")
            df.to_sql(table_name, engine, schema='data_source', if_exists='replace', index=False)

            # Verify row count
            row_count = engine.execute(f"SELECT COUNT(*) FROM data_source.{table_name}").scalar()

            # Verify row count
            print(f"Verified {row_count} rows in the '{table_name}' table.")

        except FileNotFoundError:
            print(f"Warning: File {csv_file} not found. Skipping.")
            continue
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
            continue

    print("\nSUCCESS: All available data files have been loaded to PostgreSQL!")

except Exception as e:
    print(f"Error: {e}")
