import json
from main import convert_data_pwn_rws_2  # Import the conversion function from main.py

# Define batch size
BATCH_SIZE = 1000  # Adjust based on your system's memory and performance

def process_large_json(input_file, output_file):
    try:
        # Load the large JSON file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Ensure the input data is a list
        if 'data' not in data or not isinstance(data['data'], list):
            raise ValueError("Input JSON must contain a 'data' field with a list of records.")

        records = data['data']  # Extract the list of records
        harmonized_data = []  # Store converted data

        # Process data in batches
        total_records = len(records)
        print(f"Total records: {total_records}")
        for i in range(0, total_records, BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]  # Extract a batch of records
            print(f"Processing batch {i // BATCH_SIZE + 1} ({len(batch)} records)")

            # Harmonize the batch using the RWS-2 conversion function
            converted_batch = convert_data_pwn_rws_2(batch)

            # Append the converted batch to the harmonized data
            harmonized_data.extend(converted_batch)

        # Save the harmonized data to a new JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(harmonized_data, f, indent=4)

        print(f"Harmonization complete. Results saved to {output_file}")

    except Exception as e:
        print(f"Error during harmonization: {str(e)}")

# Example usage
if __name__ == '__main__':
    input_file = 'rws_2024-2025.json'  # Path to your large JSON file
    output_file = 'harmonized_rws_2_data.json'  # Path to save the harmonized JSON file

    process_large_json(input_file, output_file)