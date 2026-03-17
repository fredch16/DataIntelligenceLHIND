import requests
import json

# 1. Configuration
BASE_URL = "https://lh-proxy.onrender.com"
PASSWORD = "DataIntelligence2026"  # Replace with your actual password
HEADERS = {"password": PASSWORD}

# 2. Make the request (Frankfurt Airport 'FRA')
response = requests.get(f"{BASE_URL}/v1/references/airports?limit=93&offset=543&lang=EN", headers=HEADERS)
# response = requests.get(f"{BASE_URL}/v1/references/airports?limit=1&offset=1543&lang=EN", headers=HEADERS)
# response = requests.get(f"{BASE_URL}/v1/references/airports?limit=42&offset=1500&lang=EN", headers=HEADERS)

# 3. Print the result
if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Failed! Status: {response.status_code}")
    print(response.text)