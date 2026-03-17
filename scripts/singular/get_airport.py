import requests

# Constants
BASE_URL = "https://api.lufthansa.com"
ACCESS_TOKEN = ""  # Replace with your actual token

# Define the endpoint and parameters
endpoint = "/v1/references/airports?limit=1&offset=1543&lang=EN"
url = f"{BASE_URL}{endpoint}"

# Setup query parameters
# params = {
# 	"limit": 1,
# 	"offset": 1543,
# 	"lang": "EN"
# }

# Setup the headers with your token
headers = {
	"Authorization": f"Bearer {ACCESS_TOKEN}",
	"Accept": "application/json"
}

try:
	print(headers)
	response = requests.get(url, headers=headers)
	# response = requests.get(url, headers=headers, params=params)
	
	# Check if the request was successful
	response.raise_for_status()
	
	data = response.json()
	print("Success! Data received:")
	print(data)

except requests.exceptions.HTTPError as err:
	print(f"HTTP error occurred: {err}")
	print(f"Response Body: {response.text}")
except Exception as err:
	print(f"An error occurred: {err}")