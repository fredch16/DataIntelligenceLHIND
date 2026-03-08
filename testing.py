import requests
import json
import yaml
import os
from pathlib import Path

# Load configuration from YAML file
config_path = Path(__file__).parent / "config.yaml"
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# 1. Setup the connection details
BASE_URL = "https://lh-proxy.onrender.com"
HEADERS = {"password": config["password"]}

# 2. Pick an endpoint (Let's look at Frankfurt Airport 'FRA')
endpoint = "/v1/references/airports/FRA"

# 3. Make the request
response = requests.get(f"{BASE_URL}{endpoint}", headers=HEADERS)

# 4. Check if it worked and save the data to a file
output_dir = Path(__file__).parent / "outputs"
output_dir.mkdir(exist_ok=True)

script_name = Path(__file__).stem  # Get script name without extension
output_file = output_dir / f"{script_name}_output.json"

if response.status_code == 200:
    data = response.json()
    output_text = json.dumps(data, indent=2)
    print(output_text)
    
    # Save to file
    with open(output_file, 'w') as f:
        f.write(output_text)
    print(f"\nOutput saved to: {output_file}")
else:
    error_text = f"Error! Status Code: {response.status_code}\n{response.text}"
    print(error_text)
    
    # Save error to file
    with open(output_file, 'w') as f:
        f.write(error_text)
    print(f"\nError saved to: {output_file}")