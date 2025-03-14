import logging
import re
from datetime import datetime, timedelta
from airtable import Airtable

def auto_delete_short_body_text(record, airtable):
    """
    Task 1: Auto Delete Short Body Text
    Deletes records with short body text.
    """
    body = record.get("Body", "") or record.get("Content", "")
    if len(body.split()) < 50:  
        record_id = record.get("Record ID")
        if record_id:
            airtable.delete(record_id)
            logging.info(f"Record with short body text deleted: {record_id}")
        return 0 
    return 1