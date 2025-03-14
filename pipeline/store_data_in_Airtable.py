from airtable import Airtable
import json
from utils.secret_manager import access_secret
import logging
import time
from utils.date_utils import process_date
from task_pipeline.task0_check_null_value import pre_check_null_values
from task_pipeline.task1_autho_delete import auto_delete_short_body_text
from task_pipeline.task2_publication_normalization import prep_normalize_publication_titles
from task_pipeline.task3_catalog_unfamiliar import prep_catalog_unfamiliar_publications
from task_pipeline.task4_quality_score import prep_assign_quality_scores
from task_pipeline.task5_Duplicate_check import prep_identify_duplicate_syndicate
from task_pipeline.task7_Ai_analysis import prep_article_relevance_check
from task_pipeline.task8_Deep_AI_analysis import analyze_sentiment
# Constants
PROJECT_ID = "apollo-432603"
AIRTABLE_SECRET_NAME = "tadesse_airtable_API"
AIRTABLE_BASE_KEY = "appVi25LV7lINvOSC"
AIRTABLE_TABLE_NAME = "Imported table"

# Rate limit settings
MAX_REQUESTS_PER_SECOND = 5  # Airtable's rate limit
BATCH_SIZE = 5  # Number of records to process per batch
RETRY_DELAY = 1  # Initial delay for retries (in seconds)
MAX_RETRIES = 3  # Maximum number of retries for failed requests


def store_data_in_airtable(data):
    """
    Store data in Airtable with throttling, batching, and retries.
    Calculate and store the Sentiment Index using the placeholder formula.
    """
    try:
        # Retrieve the Airtable API key
        airtable_api_key = access_secret(PROJECT_ID, AIRTABLE_SECRET_NAME)
        airtable = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_TABLE_NAME, api_key=airtable_api_key)

        # Process records through tasks
        context = {
            "catalog": [
                "BBC", "CNN", "New York Times", "The Wall Street Journal", "Reuters",
                "WIRED", "NPR", "CBS News", "eTeknix", "The Guardian",
                "The Washington Post", "Bloomberg", "Forbes", "Financial Times",
                "TechCrunch", "Engadget", "Vox", "The Verge", "Business Insider",
                "Ars Technica", "Mashable", "Gizmodo", "CNET", "ZDNet"
            ], 
            "catalog_additions": [], 
            "duplicate_pairs": [], 
            "records": data
        }

        # Process records in batches
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            success = False
            retries = 0

            while not success and retries < MAX_RETRIES:
                try:
                    # Insert each record in the batch
                    for record in batch:
                        try:
                            # Task 1: Auto Delete Short Body Text
                            record["Auto_Delete_Short_Body_Text"] = auto_delete_short_body_text(record, airtable)
                            if record["Auto_Delete_Short_Body_Text"] == 0:
                                continue  # Skip further processing for records with short body text

                            # Task 2: Publication Title Normalization
                            normalized_name, score = prep_normalize_publication_titles(record.get("Publication", ""))
                            record["Publication"] = normalized_name  # Update the publication name
                            record["Prep_Normalize_Publication_Titles"] = score  # Store the score

                            # Other tasks
                            record["Pre_Check_Null_Values"] = pre_check_null_values(record)
                            record["Prep_Catalog_Unfamiliar_Publications"] = prep_catalog_unfamiliar_publications(record, context)
                            record["Prep_Assign_Quality_Scores"] = prep_assign_quality_scores(record)
                            record["Prep_Identify_Duplicate_Syndicate"] = score = prep_identify_duplicate_syndicate(record, context["records"])
                            record["Prep_Article_Relevance_Check"] = prep_article_relevance_check(record)

                            # Process the date string to extract only the date part
                            date_str = record.get("Date", "")
                            date_only = process_date(date_str)  # Use the updated process_date function
                            if not date_only:
                                logging.warning(f"Skipping record due to invalid date format: {date_str}")
                                continue

                            # Calculate the length of words in Headline and Body
                            headline = record.get("Headline", "")
                            body = record.get("Body", "") or record.get("Content", "")
                            headline_length = len(headline.split())
                            body_length = len(body.split())

                            # Sentiment analysis fields
                            sentiment_data = record.get("Sentiment Data", {})
                            primary_character = sentiment_data.get("Primary_Character", "")
                            overall_sentiment = sentiment_data.get("Overall_Sentiment", "")
                            overall_description = sentiment_data.get("Overall_Description", "")
                            opt_pes_score = sentiment_data.get("Opt_Pes_Score", "")
                            opt_pes_desc = sentiment_data.get("Opt_Pes_Desc", "")
                            relevance_score = record.get("Relevance Score", "")
                            request_id = record.get("requestId", "")

                            # Convert numeric fields to floats
                            try:
                                overall_sentiment = float(overall_sentiment) if overall_sentiment else None
                            except ValueError:
                                logging.warning(f"Invalid Overall_Sentiment value: {overall_sentiment}")
                                overall_sentiment = None

                            try:
                                opt_pes_score = float(opt_pes_score) if opt_pes_score else None
                            except ValueError:
                                logging.warning(f"Invalid Opt_Pes_Score value: {opt_pes_score}")
                                opt_pes_score = None

                            try:
                                relevance_score = int(relevance_score) if relevance_score else None
                            except ValueError:
                                logging.warning(f"Invalid Relevance Score value: {relevance_score}")
                                relevance_score = None

                            # Calculate Sentiment Index using the placeholder formula
                            if overall_sentiment is not None:
                                sentiment_index = (overall_sentiment / 1) * 100
                            else:
                                sentiment_index = None
                                logging.warning("Overall_Sentiment is missing. Cannot calculate Sentiment Index.")

                            # Handle both field naming conventions
                            search_field = record.get("Search_link") or record.get("Search", "")
                            story_link_field = record.get("Link_to_story") or record.get("Story_Link", "")

                            # Prepare the Airtable record
                            airtable_record = {
                                "Search": search_field,
                                "Headline": headline,
                                "Headline - Length - Words": headline_length,
                                "Company": record.get("Company", ""),
                                "Story_Link": story_link_field,
                                "Date": date_only,
                                "Publication": record.get("Publication", ""),
                                "Body": body,
                                "Body - Length - Words": body_length,
                                "Author": record.get("Author", ""),
                                "Primary_Character": primary_character,
                                "Overall_Sentiment": overall_sentiment,
                                "Overall_Description": overall_description,
                                "Opt_Pes_Score": opt_pes_score,
                                "Opt_Pes_Desc": opt_pes_desc,
                                "Prep_Score": relevance_score,
                                "Record ID": request_id,
                                "Sentiment_Index": sentiment_index,  # Add Sentiment Index
                                "Prep_Normalize_Publication_Titles": record.get("Prep_Normalize_Publication_Titles", 0),
                                "Pre_Check_Null_Values": record.get("Pre_Check_Null_Values", 0),
                                "Prep_Catalog_Unfamiliar_Publications": record.get("Prep_Catalog_Unfamiliar_Publications", 0),
                                "Prep_Assign_Quality_Scores": record.get("Prep_Assign_Quality_Scores", 0),
                                "Prep_Identify_Duplicate_Syndicate": record.get("Prep_Identify_Duplicate_Syndicate", 0),
                                "Prep_Article_Relevance_Check": record.get("Prep_Article_Relevance_Check", 0),
                                "AI - Analysis - Results": json.dumps(record.get("Sentiment Data", {})),
                            }

                            # Insert the record into Airtable
                            airtable.insert(airtable_record)
                            logging.info(f"Successfully inserted record into Airtable with RequestId: {request_id}")

                        except Exception as e:
                            logging.error(f"Error inserting record into Airtable: {e}")

                    # Throttle to respect rate limits
                    time.sleep(1 / MAX_REQUESTS_PER_SECOND)
                    success = True

                except Exception as e:
                    retries += 1
                    if retries == MAX_RETRIES:
                        logging.error(f"Failed to store batch after {MAX_RETRIES} retries: {e}")
                    else:
                        logging.warning(f"Retry {retries}/{MAX_RETRIES} after {RETRY_DELAY} seconds: {e}")
                        time.sleep(RETRY_DELAY * (2 ** (retries - 1)))  # Exponential backoff

    except Exception as e:
        logging.error(f"Error connecting to Airtable: {e}")