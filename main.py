import os
import csv
from pyspark.sql import SparkSession
import xml.etree.ElementTree as ET

class DataCleaner:
    def __init__(self, debug= False):
        self.debug = debug
        if self.debug: print("\tDEBUG MODE ON")
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
            output_file.write(input_file.read())
        if self.debug: print(f"Encoding of {input_path} changed from {encoding_from} to {encoding_to}")

    def xml_to_csv(self, xml_file_path, csv_file_path):
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        with open(csv_file_path, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            header = [field.attrib['name'] for field in root.find('.//record').iter('field')]
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
            csvwriter.writerow([desc[0] for desc in cursor.description])
            csvwriter.writerows(data)

        conn.close()
        if self.debug: print(f"converted {tablename} of {filename} to {csv_file_path}")

    def spark_read_csv(self, file_path): return spark.read.csv(file_path, header=True, inferSchema=True)
    def spark_read_json(self, file_path): return spark.read.json(file_path)

# Main
if __name__ == "__main__":

    debug = True                    ##### YOU CAN SWITCH DEBUGGER HERE #####
    
    cleaner = DataCleaner(debug = debug)

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