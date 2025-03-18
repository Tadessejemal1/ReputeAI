from google.cloud import secretmanager
from google.api_core.exceptions import NotFound

# Set your project ID
PROJECT_ID = "455738376818"

def access_secret(project_id, secret_name):
    """Access a specific secret's value."""
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": secret_path})
        return response.payload.data.decode("UTF-8")
    except NotFound:
        raise NotFound(f"Secret {secret_name} not found or has no versions in project {project_id}.")

def list_and_access_secrets():
    """Access multiple specific secrets."""
    secrets_to_access = [
        "tadesse_airtable_table_publication_dictionary_press_outlets",
        "tadesse_airtable_base_publication_dictionary"
    ]
    
    for secret_name in secrets_to_access:
        try:
            api_key = access_secret(PROJECT_ID, secret_name)
            print(f"API Key for {secret_name}: {api_key}")
        except NotFound as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error accessing secret {secret_name}: {e}")

if __name__ == "__main__":
    list_and_access_secrets()