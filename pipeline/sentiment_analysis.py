import google.generativeai as genai
import logging
import json
import re
from utils.secret_manager import access_secret
from google.api_core.exceptions import NotFound

# Constants
PROJECT_ID = "apollo-432603"
GEMINI_SECRET_NAME = "tadesse_google_gemini_API"

def preprocess_text(text):
    """Clean and normalize the input text."""
    if not text:
        logging.warning("Input text is empty.")
        return None
    # Normalize whitespace and remove leading/trailing spaces
    text = " ".join(text.split()).strip()
    return text

def analyze_relevance(headline, body):
    """
    Perform Task 7: Relevance Filter.
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

def analyze_sentiment(headline, body):
    """
    Perform Task 8: Article Sentiment Analysis.
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

def analyze_article(headline, body):
    """
    Perform both Task 7 and Task 8 analysis on the article.
    """
    # Step 1: Perform Task 7 (Relevance Filter)
    relevance_score = analyze_relevance(headline, body)
    if relevance_score is None or relevance_score == 0:
        logging.info(f"Article not relevant. Skipping sentiment analysis. Headline: {headline}")
        return None

    # Step 2: Perform Task 8 (Sentiment Analysis)
    sentiment_data = analyze_sentiment(headline, body)
    return sentiment_data