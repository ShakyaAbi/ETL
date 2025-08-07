import os
import sys
import zipfile
import json

def extract_zip_file(zip_filename, extract_to_dir):
    """Extract zip contents to the specified directory and remove zip"""
    if not zipfile.is_zipfile(zip_filename):
        raise Exception(f"File is not a valid ZIP: {zip_filename}")
    
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(extract_to_dir)
    os.remove(zip_filename)
    print(f"Extracted files to {extract_to_dir} and removed the zip.")

def fix_json_for_pyspark(json_file_path):
    """Convert single-dict JSON file to newline-delimited JSON (NDJSON)"""
    with open(json_file_path, 'r') as infile:
        data_dict = json.load(infile)

    ndjson_file_path = os.path.join(os.path.dirname(json_file_path), "fixed_da.json")
    with open(ndjson_file_path, 'w') as outfile:
        for key, value in data_dict.items():
            json.dump({key: value}, outfile)
            outfile.write('\n')

    os.remove(json_file_path)
    print(f"Converted {json_file_path} to {ndjson_file_path} in NDJSON format.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract.py <output_directory>")
        sys.exit(1)

    EXTRACT_PATH = sys.argv[1]
    LOCAL_ZIP = "/home/abishakya/Python/output/dataset.zip"

    try:
        extract_zip_file(LOCAL_ZIP, EXTRACT_PATH)
        fix_json_for_pyspark(os.path.join(EXTRACT_PATH, "data.json"))
        print("ZIP extraction and JSON conversion completed successfully.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
