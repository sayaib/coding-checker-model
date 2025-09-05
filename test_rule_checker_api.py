import requests
import json

# Test the updated rule_checker_api
url = "http://localhost:8000/rule_checker_api"

# Test payload with the new structure
payload = {
    "folder_name": "Coding Checker_Rule26NG_250703-1756818977690",
    "input_list": ["test1", "test2", "test3"]
}

try:
    response = requests.post(url, json=payload)
    print("Status Code:", response.status_code)
    print("Response:")
    print(json.dumps(response.json(), indent=2))
except requests.exceptions.ConnectionError:
    print("Error: Could not connect to the API. Make sure the server is running on localhost:8000")
except Exception as e:
    print(f"Error: {e}")