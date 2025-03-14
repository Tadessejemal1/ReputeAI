import logging
from datetime import datetime
from utils.date_utils import process_date

from airtable import Airtable

def prep_identify_duplicate_syndicate(record, all_records):
    """
    Task 5: Identify Duplicate/Syndicate Content
    Detects duplicate or syndicated content based on headlines within a ±14-day window.
    Applies scoring rules and tiebreakers.
    """
    headline = record.get("Headline", "")
    publication = record.get("Publication", "")
    date_str = record.get("Date", "")
    logging.info(f"Processing record: {headline}- ({publication}) - {date_str}")
    
    if not headline or not date_str or not publication:
        return 0

    date_only = process_date(date_str)
    if not date_only:
        return 0

    try:
        record_date = datetime.strptime(date_only, "%Y-%m-%d")
    except ValueError:
        logging.error(f"Invalid processed date format: {date_only}")
        return 0

    # Initialize the score
    score = 1  # Default score for originals

    # Check for duplicates or syndicates within ±14 days
    for existing_record in all_records:
        existing_headline = existing_record.get("Headline", "")
        existing_publication = existing_record.get("Publication", "")
        existing_date_str = existing_record.get("Date", "")

        # Skip if the existing record is missing required fields
        if not existing_headline or not existing_date_str:
            continue

        # Process the existing record's date
        existing_date_only = process_date(existing_date_str)
        if not existing_date_only:
            continue  # Skip if date processing fails

        try:
            existing_record_date = datetime.strptime(existing_date_only, "%Y-%m-%d")
            date_diff = abs((record_date - existing_record_date).days)

            # Check if the existing record is within ±14 days
            if date_diff <= 14:
                if existing_headline == headline:
                    if existing_publication == publication:
                        # Duplicate case (same headline + same publication)
                        # Apply tiebreakers
                        if existing_record.get("AI - Analysis - Results"):
                            # Tiebreaker 1: Already analyzed article gets scored 1
                            existing_record["Prep_Identify_Duplicate_Syndicate"] = 1
                            score = 0
                            logging.info(f"Tiebreaker 1 applied. Assigning score 0 to current record.")
                            break
                        elif existing_record_date < record_date:
                            # Tiebreaker 2: Older article gets score 1
                            existing_record["Prep_Identify_Duplicate_Syndicate"] = 1
                            score = 0
                            logging.info(f"Tiebreaker 2 applied. Assigning score 0 to current record.")
                            break
                        else:
                            # Tiebreaker 3: First-processed article wins (if same date)
                            existing_record["Prep_Identify_Duplicate_Syndicate"] = 1
                            score = 0
                            logging.info(f"Tiebreaker 3 applied. Assigning score 0 to current record.")
                            break
                    else:
                        # Syndicate case (same headline + different publication)
                        score = 2
                        logging.info(f"Syndicate found. Assigning score 2 to current record.")
        except ValueError:
            logging.error(f"Invalid processed date format in existing record: {existing_date_only}")

    # Update the record with the calculated score
    record["Prep_Identify_Duplicate_Syndicate"] = score
    return score