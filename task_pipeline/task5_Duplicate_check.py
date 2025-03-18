import logging
from datetime import datetime
from utils.date_utils import process_date

from airtable import Airtable

def prep_identify_duplicate_syndicate(record, all_data):
    """
    Task 5: Identify Duplicate/Syndicate Content
    Detects duplicate or syndicated content based on headlines within a ±14-day window.
    Applies scoring rules and tiebreakers.
    """
    headline = record.get("Headline", "")
    publication = record.get("Publication", "")
    date_str = record.get("Date", "")
    logging.info(f"Processing record: {headline} - ({publication}) - {date_str}")
    
    if not headline or not date_str or not publication:
        logging.warning("Missing required fields. Skipping record.")
        return 0  # Skip if required fields are missing

    date_only = process_date(date_str)
    if not date_only:
        logging.warning("Failed to process date. Skipping record.")
        return 0  # Skip if date processing fails

    try:
        record_date = datetime.strptime(date_only, "%Y-%m-%d")
    except ValueError:
        logging.error(f"Invalid processed date format: {date_only}")
        return 0

    # Initialize the score
    score = 1  # Default score for originals
    logging.info(f"Initial score: {score}")

    # Normalize headline for comparison
    normalized_headline = headline.strip().lower()

    # Check for duplicates or syndicates within ±14 days
    for existing_record in all_data:
        if not isinstance(existing_record, dict):
            logging.warning(f"Invalid record format: {existing_record}. Skipping.")
            continue

        existing_headline = existing_record.get("Headline", "")
        existing_publication = existing_record.get("Publication", "")
        existing_date_str = existing_record.get("Date", "")

        # Log existing record details
        logging.info(f"Existing record: Headline = {existing_headline}, Publication = {existing_publication}, Date = {existing_date_str}")

        # Skip if the existing record is missing required fields
        if not existing_headline or not existing_date_str:
            logging.warning("Existing record missing required fields. Skipping comparison.")
            continue

        # Process the existing record's date
        existing_date_only = process_date(existing_date_str)
        if not existing_date_only:
            logging.warning("Failed to process existing record date. Skipping comparison.")
            continue  # Skip if date processing fails

        try:
            existing_record_date = datetime.strptime(existing_date_only, "%Y-%m-%d")
            date_diff = abs((record_date - existing_record_date).days)
            logging.info(f"Date difference with existing record: {date_diff} days")

            # Normalize existing headline for comparison
            normalized_existing_headline = existing_headline.strip().lower()

            # Check if the existing record is within ±14 days
            if date_diff <= 14:
                logging.info(f"Existing record within ±14 days: {existing_headline} - ({existing_publication}) - {existing_date_str}")
                if normalized_headline == normalized_existing_headline:
                    logging.info(f"Headline match found: {headline}")
                    if existing_publication == publication:
                        # Duplicate case (same headline + same publication)
                        logging.info(f"Duplicate found: Same publication ({publication})")
                        # Apply tiebreakers
                        if existing_record.get("AI - Analysis - Results"):
                            # Tiebreaker 1: Already analyzed article gets scored 1
                            logging.info("Tiebreaker 1 applied: Existing record already analyzed.")
                            score = 0 
                            logging.info(f"Assigning score 0 to current record.")
                            break
                        elif existing_record_date < record_date:
                            # Tiebreaker 2: Older article gets score 1
                            logging.info("Tiebreaker 2 applied: Existing record is older.")
                            score = 0  # Current record gets score 0
                            logging.info(f"Assigning score 0 to current record.")
                            break
                        else:
                            # Tiebreaker 3: First-processed article wins (if same date)
                            logging.info("Tiebreaker 3 applied: Existing record is first-processed.")
                            score = 0  # Current record gets score 0
                            logging.info(f"Assigning score 0 to current record.")
                            break
                    else:
                        # Syndicate case (same headline + different publication)
                        logging.info(f"Syndicate found: Different publication ({existing_publication} vs {publication})")
                        score = 2
                        logging.info(f"Assigning score 2 to current record.")
                else:
                    logging.info(f"No headline match: {existing_headline} vs {headline}")
            else:
                logging.info(f"Existing record outside ±14 days: {date_diff} days")
        except ValueError:
            logging.error(f"Invalid processed date format in existing record: {existing_date_only}")

    # Update the current record with the calculated score
    record["Prep_Identify_Duplicate_Syndicate"] = score
    logging.info(f"Final score for current record: {score}")
    return score