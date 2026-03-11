import requests
import json

# 1. Configuration
BASE_URL = "https://lh-proxy.onrender.com"
PASSWORD = "DataIntelligence2026"  # Replace with your actual password
HEADERS = {"password": PASSWORD}

# 2. Make the request (Frankfurt Airport 'FRA')
response = requests.get(f"{BASE_URL}/v1/references/cities?limit=5&lang=EN", headers=HEADERS)

# 3. Print the result
if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Failed! Status: {response.status_code}")
    print(response.text)