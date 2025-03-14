import logging

def pre_check_null_values(record):
    """
    Task 1: Pre-Check Null Values
    Validates records for required fields and logs missing fields.
    """
    required_fields = [
        ("Search_link", "Search"),
        "Date",
        "Publication",
        "Author",
        "Headline",
        "Body",
        ("Link_to_story", "Story_Link"),
    ]
    
    missing_fields = []
    for field in required_fields:
        if isinstance(field, tuple):
            # Check for alternative field names
            if not any(record.get(f) for f in field):
                missing_fields.append(f"({', '.join(field)})")
        else:
            # Check for single field name
            if not record.get(field):
                missing_fields.append(field)
    
    if missing_fields:
        logging.warning(f"Missing required fields: {missing_fields}")
        return 0
    else:
        return 1