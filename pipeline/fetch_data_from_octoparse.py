import requests
import logging
import json
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from utils.secret_manager import access_secret

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
PROJECT_ID = "apollo-432603"
OCTOPARSE_SECRET_NAME = "tadesse_octoparse_API_token"
OCTOPARSE_API_URL = "https://openapi.octoparse.com/data/notexported"

def fetch_data_from_octoparse(task_id, size):
    """
    Fetches data from the Octoparse API.

    Args:
        task_id (str): The task ID for which to fetch data.
        size (int): The number of records to fetch. Defaults to 2.

    Returns:
        tuple: A tuple containing the fetched data (list) and request ID (str).
              Returns empty data and request ID in case of errors.
    """
    try:
        # Retrieve the secret
        secret_value = access_secret(PROJECT_ID, OCTOPARSE_SECRET_NAME)

        # Extract the token
        if '"access_token":' in secret_value:
            # If the secret contains "access_token":, extract the token
            octoparse_token = secret_value.split('"access_token":')[1].strip().strip('"')
        else:
            # Otherwise, assume the secret is the token itself
            octoparse_token = secret_value.strip('"')

        # Construct the Authorization header
        headers = {"Authorization": f"Bearer {octoparse_token}"}

        # Construct the URL with query parameters
        params = {"taskId": task_id, "size": size}
        url = OCTOPARSE_API_URL
        logging.info(f"Fetching data from: {url} with params: {params}")

        # Set up retry mechanism
        retry_strategy = Retry(
            total=3,  # Retry 3 times
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[500, 502, 503, 504],  # Retry on these status codes
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)

        # Make API request
        logging.info(f"Fetching data from Octoparse API: {url}")
        response = session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        # Parse the response
        response_data = response.json()
        data = response_data.get("data", {}).get("data", [])
        request_id = response_data.get("requestId", "")

        logging.info(f"Fetched {len(data)} records from Octoparse with requestId: {request_id}")
        return data, request_id

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {e}")
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error occurred: {e}")
    except requests.exceptions.Timeout as e:
        logging.error(f"Request timed out: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")
    except ValueError as e:
        logging.error(f"Error with Octoparse API token: {e}")
    return [], ""