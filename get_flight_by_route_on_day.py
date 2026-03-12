import requests # to get the api stuff
import json #formatting
import yaml #password
from pathlib import Path

config_path = Path(__file__).parent / "config.yaml"
with open(config_path, 'r') as f:
	config = yaml.safe_load(f)

HEADERS = {"password": config["password"]}

departure_airport = "LHR"
arrival_airport = "STR"
date = "2026-03-12"

routeForOutput = f"{departure_airport}-{arrival_airport}"

# Set endpoint
endpoint = f"/v1/operations/flightstatus/route/{departure_airport}/{arrival_airport}/{date}"

# Make the request
response = requests.get(f"{config["base_url"]}{endpoint}", headers = HEADERS)

output_dir = Path(__file__).parent / "outputs"
output_dir.mkdir(exist_ok=True)

# get script name
output_file = output_dir / f"get_flight_on_route{routeForOutput}_{date}_output.json"

if response.status_code == 200:
	data = response.json()
	output_text = json.dumps(data, indent = 2)

	with open(output_file, 'w') as f:
		f.write(output_text)
	print(f"\nSuccess: Output saved to : {output_file}")
else:
	error_text = f"Error! Status Code: {response.status_code}\n{response.text}"
	print(error_text)
