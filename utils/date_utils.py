from datetime import datetime
import logging

def process_date(date_str):
    """
    Process a date string to extract only the date part (YYYY-MM-DD).
    Handles both date-only and date-time strings.
    """
    if not date_str:
        return None

    try:
        # Try parsing with date-time format
        date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            # Try parsing with date-only format
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            logging.error(f"Invalid date format: {date_str}")
            return None

    # Extract only the date part
    date_only = date_obj.strftime("%Y-%m-%d")
    return date_only