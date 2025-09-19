import requests
import json

def test_download_task_csv_api():
    """
    Test the download Task CSV API endpoint
    """
    # API endpoint
    url = "http://localhost:8000/download_task_csv"
    
    # Test payload - using one of the existing folders from input_files
    payload = {
        "folder_name": "Coding Checker_Rule26"
    }
    
    # Headers
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        print("Testing download Task CSV API...")
        print(f"URL: {url}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        # Make the POST request
        response = requests.post(url, json=payload, headers=headers)
        
        print(f"\nResponse Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        # Parse response
        if response.headers.get('content-type', '').startswith('application/json'):
            response_data = response.json()
            print(f"Response JSON: {json.dumps(response_data, indent=2)}")
        else:
            print(f"Response Text: {response.text}")
        
        # Check if successful
        if response.status_code == 200:
            if 'status' in response_data and response_data['status'] == 'success':
                print("\n✅ API test PASSED - File downloaded successfully!")
                print(f"Downloaded file: {response_data.get('file_name')}")
                print(f"File path: {response_data.get('file_path')}")
            else:
                print(f"\n❌ API test FAILED - Error in response: {response_data.get('error', 'Unknown error')}")
        else:
            print(f"\n❌ API test FAILED - HTTP {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("\n❌ Connection Error - Make sure the FastAPI server is running on localhost:8000")
    except Exception as e:
        print(f"\n❌ Test failed with exception: {str(e)}")

if __name__ == "__main__":
    test_download_task_csv_api()