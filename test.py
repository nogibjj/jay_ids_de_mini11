import requests
from dotenv import load_dotenv
import os

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
server_h = os.getenv("SERVER_HOSTNAME")

url = f"https://{server_h}/api/2.0/clusters/list"
headers = {"Authorization": f"Bearer {access_token}"}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print("Authorization successful!")
except requests.exceptions.RequestException as e:
    print(f"Authorization failed: {e}")
