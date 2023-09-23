import json

import requests

# Replace with your own access token and repository information
access_token = "<ACCESS_TOKEN>"
repo_owner = "Missouri-BMI"
repo_name = "ShowMeVax"
project_id = 1

# Send a GET request to retrieve the project columns
response = requests.get(
    f"https://api.github.com/repos/{repo_owner}/{repo_name}",
    headers={
        "Authorization": f"Token {access_token}"
    }
)

# Check the response status code
if response.status_code == 200:
    # Parse the response data
    columns = response.json()
    with open('test.json', 'a') as tj:
        json.dump(columns, tj, indent=4)
    for column in columns:
        column_id = column["id"]
        column_name = column["name"]
        print(f"Column ID: {column_id}, Column Name: {column_name}")
else:
    print(response)
    print("Failed to retrieve project columns")
