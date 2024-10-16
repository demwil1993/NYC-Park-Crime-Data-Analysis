import psycopg2
import pandas as pd
import numpy as np
import os
from glob import glob
from Config_crime import config


class CrimeDataETL(object):
    # constructor to initialize object
    def __init__(self):
        self.crime = None
        self.crime_transform = None

    def extract(self):
        try:
            # Get the absolute paths of all Excel files 
            all_excel_files = glob("C:/Users/aspar/OneDrive/Project_files/nyc_crime/nyc_park_stats/*.xlsx")

            if not all_excel_files:
                raise ValueError("No Excel files found in the specified directory.")
            
            print(f"Found {len(all_excel_files)} files: {all_excel_files}")
        
            # Read all Excel files at once, skip the first 3 rows, remove the last row, and remove the last column
            dfs = []
            for excel_file in all_excel_files:
                try:
                    # Extract year and quarter from file name
                    file_name = os.path.basename(excel_file)
                    year = int(file_name.split('-')[-1].split('.')[0])
                    quarter = file_name.split('-')[-2]
                    quarter_map = {'q1': 'qtr1', 'q2': 'qtr2', 'q3': 'qtr3', 'q4': 'qtr4'}
                    qtr = quarter_map.get(quarter.lower(), 'unknown')

                    df = pd.read_excel(excel_file, skiprows=3)
                    df = df.iloc[:-1, :-1]  # Remove the last row and last column
                    # Make the first letter of each column lower case
                    df.columns = [col.lower() for col in df.columns]
                    # Add year and quarter columns
                    df['year'] = year
                    df['quarter'] = qtr
                    dfs.append(df)
                except Exception as e:
                    print(f"Error reading {excel_file}: {e}")

            if not dfs:
                raise ValueError("No valid dataframes extracted from excel files")
            
            # Concatenate all DataFrames
            self.crime = pd.concat(dfs, ignore_index=True)
            print("Data extraction successful.\n")
        except Exception as e:
            print(f"Error during extraction: {e}")

    def transform(self):
        try:
            # Copy dataframes
            self.crime_transform = self.crime.copy()
            
            # Rename some columns
            self.crime_transform.rename(columns={'size (acres)':'acres', 'felony assault':'felony_assault', 'grand larceny':'grand_larceny',
                                                 'grand larceny of motor vehicle': 'grand_larceny_motor_vehicle'}, inplace=True)
                                                 
            # Strip white spaces from values in all columns
            for col in self.crime_transform.columns:
                if self.crime_transform[col].dtype == 'object':
                    self.crime_transform[col] = self.crime_transform[col].str.strip()
                   
            # For categorical columns, change all the values in the column to title form
            for col in self.crime_transform.columns:
                if self.crime_transform[col].dtype == 'object':
                    self.crime_transform[col] = self.crime_transform[col].str.title()
                    
            # Remove decimal places from values in the murder and burglary column
            int_lst = ['murder', 'burglary']
            for col in int_lst:
                self.crime_transform[col] = self.crime_transform[col].astype('int64')
                
            print("Data transformation successful.\n")
        except Exception as e:
            print(f"Error during transformation: {e}")

    def load(self):
        try:
            # Create a new instance of the Database
            database = Database()

            print("Creating tables in the database...")
            try:
                database.create_tables()
                print("\tTables created successfully.\n")
            except Exception as e:
                print(f"Error creating tables: {e}")
                return # Terminate if table creation fails

            print("Inserting data into the staging table...")
            try:
                database.insert_data_staging(self.crime_transform)
                print("\tData inserted into staging table successfully.\n")
            except Exception as e:
                print(f"Error inserting data into staging table: {e}")
                return # Terminate if data insertion into staging table fails

            print("Inserting data into fact and dimension tables...")
            try:
                database.insert_data_facts_dims()
                print("\tData inserted into fact and dimension tables successfully.\n")
            except Exception as e:
                print(f"Error inserting data into fact and dimension tables: {e}")
                return # Terminate if data insertion into fact and dimension tables fails

            print("Correcting null values in the database...")
            try:
                database.correct_nulls_database()
                print("\tNull values corrected successfully.\n")
            except Exception as e:
                print(f"Error correcting null values: {e}")
                return # Terminate if null value correction fails

            print("Counting rows in the tables...\n")
            try:
                database.count_rows()
                print("Row counts completed successfully.\n")
            except Exception as e:
                print(f"Error counting rows in tables: {e}")
                return # Terminate if row count fails

            print("\nData loaded successfully into PostgreSQL.")
        except Exception as e:
            print(f"Error during loading: {e}")
            
class Database:
    def __init__(self, **params):
        if params:
            self.conn = psycopg2.connect(**params)
        else:
            # Read connection parameters from the configuration file
            db_params = config()
            self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def create_tables(self):
        # Define SQL queries to create necessary tables if they don't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS staging.crime (
            park   VARCHAR,
            borough   VARCHAR,
            acres    FLOAT,
            category   VARCHAR,
            murder  INT,
            rape   INT,
            robbery    INT,
            felony_assault INT,
            burglary   INT,
            grand_larceny INT,
            grand_larceny_motor_vehicle    INT,
            year   VARCHAR,
            quarter VARCHAR
        );

        -- create fact table --
        CREATE TABLE IF NOT EXISTS core.fact_crime (
            crime_id   SERIAL PRIMARY KEY,
            year_id    INT,
            qtr_id  INT,
            park_id INT,
            borough_id    INT,
            acres FLOAT,
            category_id   INT,
            murder INT,
            rape  INT,
            robbery    INT,
            felony_assault INT,
            burglary  INT,
            grand_larceny  INT,
            grand_larceny_motor_vehicle   INT
        );

        -- create dimension tables --
        CREATE TABLE IF NOT EXISTS core.dim_year (
            year_id    SERIAL PRIMARY KEY,
            year   VARCHAR
        );

        CREATE TABLE IF NOT EXISTS core.dim_quarter (
            qtr_id  SERIAL PRIMARY KEY,
            quarter VARCHAR
        );

        CREATE TABLE IF NOT EXISTS core.dim_park (
            park_id  SERIAL PRIMARY KEY,
            park   VARCHAR
        );

        CREATE TABLE IF NOT EXISTS core.dim_borough (
            borough_id SERIAL PRIMARY KEY,
            borough    VARCHAR
        );

        CREATE TABLE IF NOT EXISTS core.dim_category (
            category_id    SERIAL PRIMARY KEY,
            category   VARCHAR
        );   
        """

        # Execute the create table query
        self.cur.execute(create_table_query)
        self.conn.commit()
        
    def insert_data_staging(self, survey_df):
        # Use cursor to execute INSERT statement
        for _, row in survey_df.iterrows():
            insert_survey_query = """
            INSERT INTO staging.crime (park, borough, acres, category, murder, rape, robbery, felony_assault, burglary, grand_larceny, grand_larceny_motor_vehicle, year, quarter)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            data_tuple = (row['park'],
                          row['borough'],
                          row['acres'],
                          row['category'],
                          row['murder'],
                          row['rape'],
                          row['robbery'],
                          row['felony_assault'],
                          row['burglary'],
                          row['grand_larceny'],
                          row['grand_larceny_motor_vehicle'],
                          row['year'],
                          row['quarter'])
            self.cur.execute(insert_survey_query, data_tuple)

        self.conn.commit()
        
    def insert_data_facts_dims (self):
        populate_dim_query = """
        -- Step 1: Populate Dimension Tables
        INSERT INTO core.dim_year (year)
        SELECT DISTINCT year FROM staging.crime
        ON CONFLICT DO NOTHING;

        INSERT INTO core.dim_quarter (quarter)
        SELECT DISTINCT quarter FROM staging.crime
        ON CONFLICT DO NOTHING;

        INSERT INTO core.dim_park (park)
        SELECT DISTINCT park FROM staging.crime
        ON CONFLICT DO NOTHING;

        INSERT INTO core.dim_borough (borough)
        SELECT DISTINCT borough FROM staging.crime
        ON CONFLICT DO NOTHING;

        INSERT INTO core.dim_category (category)
        SELECT DISTINCT category FROM staging.crime
        ON CONFLICT DO NOTHING;

        -- Step 2: Create Relationships --
        ALTER TABLE core.fact_crime
        ADD CONSTRAINT fk_year FOREIGN KEY (year_id) REFERENCES core.dim_year (year_id),
        ADD CONSTRAINT fk_qtr FOREIGN KEY (qtr_id) REFERENCES core.dim_quarter (qtr_id),
        ADD CONSTRAINT fk_park FOREIGN KEY (park_id) REFERENCES core.dim_park (park_id),
        ADD CONSTRAINT fk_borough FOREIGN KEY (borough_id) REFERENCES core.dim_borough (borough_id),
        ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES core.dim_category (category_id);

        -- Step 3: Insert data into fact table --
        INSERT INTO core.fact_crime (year_id, qtr_id, park_id, borough_id, acres, category_id, murder, rape, robbery, felony_assault, burglary, grand_larceny, grand_larceny_motor_vehicle)
        SELECT y.year_id, q.qtr_id, p.park_id, b.borough_id, c.acres, cy.category_id, c.murder, c.rape, c.robbery, c.felony_assault, c.burglary, c.grand_larceny, c.grand_larceny_motor_vehicle
        FROM staging.crime c
        JOIN core.dim_year y ON c.year = y.year
        JOIN core.dim_quarter q ON c.quarter = q.quarter
        JOIN core.dim_park p ON c.park = p.park
        JOIN core.dim_borough b ON c.borough = b.borough
        JOIN core.dim_category cy ON c.category = cy.category;
        """
        self.cur.execute(populate_dim_query)
        self.conn.commit()
        
    def correct_nulls_database(self):
        create_update_queries = """
        UPDATE core.dim_year
        SET year = NULL
        WHERE year = 'NaN';

        UPDATE core.dim_quarter
        SET quarter = NULL
        WHERE quarter = 'NaN';
        
        UPDATE core.dim_park
        SET park = NULL
        WHERE park = 'NaN';
        
        UPDATE core.dim_borough
        SET borough = NULL
        WHERE borough = 'NaN';
        
        UPDATE core.dim_category
        SET category = NULL
        WHERE category = 'NaN';
        """

        self.cur.execute(create_update_queries)
        self.conn.commit()
        
    def count_rows(self):
        # SQL queries to count rows in each table
        count_stage_crime_query = "SELECT COUNT(*) FROM staging.crime;"
        count_core_crime_query = "SELECT COUNT(*) FROM core.fact_crime;"
        count_dim_year_query = "SELECT COUNT(*) FROM core.dim_year;"
        count_dim_quarter_query = "SELECT COUNT(*) FROM core.dim_quarter;"
        count_dim_park_query = "SELECT COUNT(*) FROM core.dim_park;"
        count_dim_borough_query = "SELECT COUNT(*) FROM core.dim_borough;"
        count_dim_category_query = "SELECT COUNT(*) FROM core.dim_category;"
        
        # Execute queries to get row counts
        self.cur.execute(count_stage_crime_query)
        stage_crime_count = self.cur.fetchone()[0]
        
        self.cur.execute(count_core_crime_query)
        fact_crime_count = self.cur.fetchone()[0]
        
        self.cur.execute(count_dim_year_query)
        dim_year_count = self.cur.fetchone()[0]

        self.cur.execute(count_dim_quarter_query)
        dim_qtr_count = self.cur.fetchone()[0]
        
        self.cur.execute(count_dim_park_query)
        dim_park_count = self.cur.fetchone()[0]
        
        self.cur.execute(count_dim_borough_query)
        dim_borough_count = self.cur.fetchone()[0]
        
        self.cur.execute(count_dim_category_query)
        dim_category_count = self.cur.fetchone()[0]

        # Print out row counts
        print(f"Staging Crime Table: {stage_crime_count} rows")
        print(f"Fact Crime Table: {fact_crime_count} rows")
        print(f"Dimension Year Table: {dim_year_count} rows")
        print(f"Dimension Quarter Table: {dim_qtr_count} rows")
        print(f"Dimension Park Table: {dim_park_count} rows")
        print(f"Dimension Borough Table: {dim_borough_count} rows")
        print(f"Dimension Category Table: {dim_category_count} rows")

    def __del__ (self):
        # Close cursor and connection when object is deleted
        self.cur.close()
        self.conn.close()
        print('\nETL Process Done, database object deleted.')
        
# Execute ETL process
if __name__ == "__main__":
    etl_process = CrimeDataETL()
    etl_process.extract()
    etl_process.transform()
    etl_process.load()
