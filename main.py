import os
import csv
from pyspark.sql import SparkSession
import xml.etree.ElementTree as ET

class DataCleaner:
    def __init__(self, debug= False):
        self.debug = debug
        if self.debug: print("\t ---- DEBUG MODE ON ----")
        self.cleaning_folder = "cleaned_files"
        os.makedirs(self.cleaning_folder, exist_ok=True)

    def remove_lines_from_csv(self, input_path, output_path, lines_to_remove=4):
        with open(input_path, "r") as input_file, open(output_path, "w") as output_file:
            for _ in range(lines_to_remove):
                next(input_file)
            output_file.write(input_file.read())
        if self.debug: print(f"Removed first 4 lines from {input_path} and saved to {output_path}")

    def detect_encoding(self, file_path):
        import chardet
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding']

    def resolve_encoding(self, input_path, output_path, encoding_from, encoding_to):
        with open(input_path, "r", encoding=encoding_from) as input_file, open(output_path, "w", encoding=encoding_to) as output_file:
            output_file.write("index")
            output_file.write(input_file.read())
        if self.debug: print(f"Encoding of {input_path} changed from {encoding_from} to {encoding_to}")

    def xml_to_csv(self, xml_file_path, csv_file_path):
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            header = [field.attrib['name'] for field in root.find('.//record').iter('field')]
            header[0] = "Country Name"   ######## .replaced "Country or Area" to "Country Name" in header
            csv_writer.writerow(header)

            for record in root.findall('.//record'):
                row = [field.text for field in record.iter('field')]
                csv_writer.writerow(row)
        if self.debug: print(f"converted {xml_file_path} to {csv_file_path}")

    def convert_db_to_csv(self, filename, tablename):
        import sqlite3
        import csv
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM "+tablename)
        data = cursor.fetchall()

        csv_file_path = 'cleaned_files/cleaned_'+tablename+"_db.csv"
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([desc[0].replace("_"," ") for desc in cursor.description])
            csvwriter.writerows(data)

        conn.close()
        if self.debug: print(f"converted {tablename} of {filename} to {csv_file_path}")

    def spark_read_csv(self, file_path): return spark.read.csv(file_path, header=True, inferSchema=True)
    def spark_read_json(self, file_path): return spark.read.json(file_path)

    def merge_dfs (self, *dfs):
        combined_df = dfs[1]
        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns = True).distinct()
        if debug: print("combined ", dfs, "dataframes")
        return combined_df
    
    def showall(self):
        try:
            print("\telectricity_access_percent_df.show()")
            electricity_access_percent_df.show()

            print("\tgdp_data_df.show()")
            gdp_data_df.show()

            print("\tmystery_df.show()")
            mystery_df.show()

            print("\tpopulation_data_csv_df.show()")
            population_data_csv_df.show()

            print("\tpopulation_data_db_df.show()")
            population_data_db_df.show()

            print("\tpopulation_data_json_df.show()")
            population_data_json_df.show()

            print("\tpopulation_data_xml_df.show()")
            population_data_xml_df.show()

            print("\tprojects_data_xml_df.show()")
            projects_data_xml_df.show()

            print("\trural_population_percent_df.show()")
            rural_population_percent_df.show()

            print("\tpopulation_combined_df.show()")
            population_combined_df.show()
        except: pass
    
    def load(self, output_file):
        import sqlite3
        from pyspark.sql.types import StringType, IntegerType, DoubleType

        projects_data_df = projects_data_xml_df

        # Function to create a table based on the schema of a DataFrame
        def create_table(cursor, table_name, df):
            type_mapping = {
                StringType(): 'TEXT',
                IntegerType(): 'INTEGER',
                DoubleType(): 'REAL'
            }

            columns_and_types = [(col.name, type_mapping.get(col.dataType, 'TEXT')) for col in df.schema]
            columns_definition = ', '.join([f'"{col}" {data_type}' for col, data_type in columns_and_types])
            create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})'
            
            if debug: print(f'Generated query: {create_table_query}')  # Print the query for debugging purposes
            
            cursor.execute(create_table_query)


        # Function to insert data into the table
        def insert_data(cursor, table_name, df):
            insert_data_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(df.columns))})'
            if debug: print(f'Generated query: {insert_data_query}')  # Print the query for debugging purposes
            for row in df.collect():
                cursor.execute(insert_data_query, tuple(row))

        # Connect to SQLite and create tables
        connection = sqlite3.connect(output_file)
        cursor = connection.cursor()

        create_table(cursor, 'population_data', population_combined_df)
        create_table(cursor, 'gdp_data', gdp_data_df)
        create_table(cursor, 'electricity_access_percent', electricity_access_percent_df)
        create_table(cursor, 'rural_population_percent', rural_population_percent_df)
        create_table(cursor, 'mystery', mystery_df)
        create_table(cursor, 'projects_data', projects_data_df)

        # Insert data into tables
        insert_data(cursor, 'population_data', population_combined_df)
        insert_data(cursor, 'gdp_data', gdp_data_df)
        insert_data(cursor, 'electricity_access_percent', electricity_access_percent_df)
        insert_data(cursor, 'rural_population_percent', rural_population_percent_df)
        insert_data(cursor, 'mystery', mystery_df)
        insert_data(cursor, 'projects_data', projects_data_df)

        # Commit the changes and close the connection
        connection.commit()
        connection.close()

# Main
if __name__ == "__main__":

    debug = True                    ##### YOU CAN SWITCH DEBUGGER HERE #####

    cleaner = DataCleaner(debug = debug)

    if debug: print("\t---- EXTRACTION INITIATED ----\t")
    # CSV (4 line removal)
    csv_files = ["electricity_access_percent.csv", "gdp_data.csv", "population_data.csv", 'rural_population_percent.csv']
    for csv_file in csv_files:
        input_path = csv_file
        output_path = os.path.join(cleaner.cleaning_folder, f"cleaned_{csv_file}")
        cleaner.remove_lines_from_csv(input_path, output_path)

    # CSV (Encoding issue)
    mystery_encoding = cleaner.detect_encoding("mystery.csv")
    cleaner.resolve_encoding("mystery.csv", os.path.join(cleaner.cleaning_folder, "cleaned_mystery.csv"), mystery_encoding, "utf-8")

    # Starting Spark
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    # Read CSV files
    electricity_access_percent_df = cleaner.spark_read_csv('cleaned_files/cleaned_electricity_access_percent.csv')
    gdp_data_df = cleaner.spark_read_csv('cleaned_files/cleaned_gdp_data.csv')
    rural_population_percent_df = cleaner.spark_read_csv('cleaned_files/cleaned_rural_population_percent.csv')

    if debug: cleaner.detect_encoding("mystery.csv")
    mystery_df = cleaner.spark_read_csv('cleaned_files/cleaned_mystery.csv')
    if debug: cleaner.detect_encoding('cleaned_files/cleaned_mystery.csv')

    population_data_csv_df = cleaner.spark_read_csv('cleaned_files/cleaned_population_data.csv')

    # Convert DB to CSV
    cleaner.convert_db_to_csv(filename = 'population_data.db', tablename = "population_data")
    population_data_db_df = cleaner.spark_read_csv('cleaned_files/cleaned_population_data_db.csv')

    # Read JSON file
    population_data_json_df = cleaner.spark_read_json('population_data.json')

    # Read XML file
    cleaner.xml_to_csv("population_data.xml", "cleaned_files/cleaned_population_data_xml.csv")
    population_data_xml_df = cleaner.spark_read_csv('cleaned_files/cleaned_population_data_xml.csv')

    # Read other CSV files
    projects_data_xml_df = cleaner.spark_read_csv('projects_data.csv')

    if debug: print("\t---- EXTRACT COMPLETED \t PROCEDING WITH TRANSFORMATION ----\t")

    population_combined_df = cleaner.merge_dfs(population_data_csv_df, population_data_db_df, population_data_json_df, population_data_xml_df)

    if debug: print("\t---- TRANSFORMATION COMPLETED \t PROCEDING WITH LOADING ----\t")
    cleaner.load(output_file="combined_data_db.sqlite")

    # END SPARK SESSION
    if debug: 
        print("\t---- LOADING FINISHED ----\t")
        cleaner.showall()
    
    spark.stop()