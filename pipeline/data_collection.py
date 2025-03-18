import requests
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from utils.secret_manager import access_secret

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
PROJECT_ID = "apollo-432603"
OCTOPARSE_SECRET_NAME = "tadesse_octoparse_API_token"
OCTOPARSE_API_URL = "https://openapi.octoparse.com/data/notexported"

# Fetch data from Octoparse API
def fetch_data_from_octoparse(task_id, size):
    try:
        # Retrieve the Octoparse API token using the access_secret function
        octoparse_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImdXS3hWc0F4SHhaMDlDcWh6NkNJb3ciLCJ0eXAiOiJhdCtqd3QifQ.eyJuYmYiOjE3NDIyNzM2MzksImV4cCI6MTc0MjM2MDAzOSwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS5vY3RvcGFyc2UuY29tIiwiY2xpZW50X2lkIjoiT3BlbkFwaSIsInN1YiI6IjFhNzQ3NDQ4LTlkYjEtNDE0ZC05YTYzLTg2NDg5YjhlMjdlMCIsImF1dGhfdGltZSI6MTc0MjI3MzYzOSwiaWRwIjoibG9jYWwiLCJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6IjFhNzQ3NDQ4LTlkYjEtNDE0ZC05YTYzLTg2NDg5YjhlMjdlMCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWUiOiJiczQzbmQxdSIsInByZWZlcnJlZF91c2VybmFtZSI6ImJzNDNuZDF1IiwidW5pcXVlX25hbWUiOiJiczQzbmQxdSIsInJlZ2lzdGVyX2RhdGUiOiIyMDIzLTA2LTA3VDE4OjI3OjQ2KzAwOjAwIiwibGFzdF9sb2dpbl9kYXRlIjoiMjAyNS0wMy0wNVQxNjo0NzowMCswMDowMCIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL2VtYWlsYWRkcmVzcyI6IndpbGxAdmFsZW50aW5lLWFkdmlzb3JzLmNvbSIsImVtYWlsIjoid2lsbEB2YWxlbnRpbmUtYWR2aXNvcnMuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInNjb3BlIjpbIm9wZW5pZCIsInByb2ZpbGUiLCJvZmZsaW5lX2FjY2VzcyJdLCJhbXIiOlsicHdkIl19.kwz3lqkULk0ed_nbENji8xAI4J151fSCeCBCrm6yjYDdgTnMJHpgfi3GdOwIiCJWBLUI0VuEFobSqpXWNND6axQC38BfOXaZcNome3ADE4qJBpl4q6WZRNpMgs60srDgnhrS-IPyyEo3bzHitCgt_4K_6a-YeSp_c89D2eW6JsGU1XJ-4uaJqqjwUFBy5-Ue6PR7HpINmp7aAEsLkGGfEnxFFm5KICGed7W-cVjgTK2yB7LIDeBZTjB8_bh5cysA09daTsHXntLvRHAVVuRQS0TQxUWRG2-JFGxAL0HlAvjACKgyjGqo78Wz8VOPbLKdOZG3wP0Fsy19PsVErGdB-Q"
        headers = {"Authorization": f"Bearer {octoparse_token}"}

        # Construct the URL with query parameters
        params = {
            "taskId": task_id,
            "size": size,
        }
        url = OCTOPARSE_API_URL

       # Print the full URL for debugging
        print(f"Fetching data from: {url} with params: {params}")

        # Set up retry mechanism
        retry_strategy = Retry(
            total=3,  # Retry 3 times
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)

        logging.info(f"Fetching data from Octoparse API: {url}")
        response = session.get(url, headers=headers, params=params, timeout=10)  # Add timeout to prevent hanging
        response.raise_for_status()  # Raise an error for bad status codes

        # Parse the response
        response_data = response.json()
        data = response_data.get("data", {}).get("data", [])  # Extract the data array
        request_id = response_data.get("requestId", "")  # Extract the requestId

        logging.info(f"Fetched {len(data)} records from Octoparse with requestId: {request_id}")
        return data, request_id  # Return both the data and requestId
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Octoparse: {e}")
        return [], ""  # Return empty data and requestId
    except ValueError as e:
        logging.error(f"Error with Octoparse API token: {e}")
        return [], ""  # Return empty data and requestId