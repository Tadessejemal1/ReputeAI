import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable
from utils.secret_manager import access_secret
from pipeline.sentiment_analysis import analyze_relevance, analyze_sentiment
from utils.date_utils import process_date
from fuzzywuzzy import process

def analyze_sentiment(headline, body):
    """
    Task 8: Article Sentiment Analysis
    Analyze the sentiment of the article based on the provided headline and body.
    """
    try:
        # Get API key using the existing access_secret function
        gemini_api_key = access_secret(PROJECT_ID, GEMINI_SECRET_NAME)
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')

        # Load the Task 8 prompt
        with open("prompts/task8_openai_prompt_example_client.txt", "r") as file:
            prompt_template = file.read()

        # Format the prompt with the headline and body
        prompt = prompt_template.replace("$Headline", headline).replace("$Body", body[:750])  # First 750 words

        # Generate the response
        response = model.generate_content(prompt)
        sentiment_result = response.text.strip()

        # Use regex to extract the JSON object from the response
        json_match = re.search(r"```json\s*({.*?})\s*```", sentiment_result, re.DOTALL) or re.search(r"({.*})", sentiment_result, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            try:
                sentiment_data = json.loads(json_str)
                return sentiment_data
            except json.JSONDecodeError:
                logging.error(f"Invalid JSON format in extracted response: {json_str}")
                return None
        else:
            logging.error(f"No JSON object found in response: {sentiment_result}")
            return None

    except NotFound as e:
        logging.error(f"Missing Gemini API key: {e}")
        return None
    except ValueError as e:
        logging.error(f"Invalid sentiment score format: {e}")
        return None
    except Exception as e:
        logging.error(f"Sentiment analysis failed: {e}")
        return None