import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date
from fuzzywuzzy import process

MIN_BODY_LENGTH = 50
PROJECT_ID = "apollo-432603"
AIRTABLE_SECRET_NAME = "tadesse_airtable_API"
AIRTABLE_BASE_KEY = "appVi25LV7lINvOSC"
AIRTABLE_TABLE_NAME = "Imported table"

def analyze_relevance(headline, body):
    """
    Task 7: Relevance Filter
    Analyze the article's relevance based on the provided headline and body.
    """
    try:
        # Get API key using the existing access_secret function
        gemini_api_key = access_secret(PROJECT_ID, GEMINI_SECRET_NAME)
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')  # Use the correct model name

        # Load the Task 7 prompt
        with open("prompts/task7_relevance_prompt_example_client.txt", "r") as file:
            prompt_template = file.read()

        # Format the prompt with the headline and body
        prompt = prompt_template.replace("$Headline", headline).replace("$Body", body[:500])  # First 500 words

        # Generate the response
        response = model.generate_content(prompt)
        relevance_score = response.text.strip()

        # Extract the integer score from the response
        score_match = re.search(r"\d+", relevance_score)
        if score_match:
            relevance_score = int(score_match.group(0))
            return relevance_score
        else:
            logging.error(f"Invalid relevance score format: {relevance_score}")
            return None

    except NotFound as e:
        logging.error(f"Missing Gemini API key: {e}")
        return None
    except ValueError as e:
        logging.error(f"Invalid relevance score format: {e}")
        return None
    except Exception as e:
        logging.error(f"Relevance analysis failed: {e}")
        return None