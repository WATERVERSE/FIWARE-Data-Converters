import requests
import json
import csv
url = 'https://dataconnector-waterverse.opsi.lecce.it/api/v1/pwn/rws/v2'
headers = {
    "Content-Type": "application/json"
}
query = {
    'days': 365
}
response = requests.get(url, headers=headers, params=query)

# Print the response
if response.ok:
    dataset = response.json()
    print(f"dataset: {dataset}")
    
else:
    print("Error:", response.json())


# Save to a JSON file
with open("knmi_2024-2025.json", "w") as file:
    json.dump(dataset, file)
print("JSON file saved successfully.")