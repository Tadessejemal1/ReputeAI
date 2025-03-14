import logging
from airtable import Airtable
from utils.secret_manager import access_secret
import pandas as pd
from datetime import datetime

# Constants
PROJECT_ID = "apollo-432603"
AIRTABLE_SECRET_NAME = "tadesse_airtable_API"
AIRTABLE_BASE_KEY = "appVi25LV7lINvOSC"
AIRTABLE_TABLE_NAME = "Imported table"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def fetch_index_data_from_airtable():
    """
    Fetch calculated sentiment indices from Airtable.
    """
    try:
        # Retrieve the Airtable API key
        airtable_api_key = access_secret(PROJECT_ID, AIRTABLE_SECRET_NAME)
        airtable = Airtable(AIRTABLE_BASE_KEY, AIRTABLE_TABLE_NAME, api_key=airtable_api_key)

        # Fetch all records from Airtable
        records = airtable.get_all()
        logging.info(f"Fetched {len(records)} records from Airtable.")

        # Convert records to a pandas DataFrame for easier manipulation
        data = []
        for record in records:
            fields = record.get("fields", {})  # Access the "fields" dictionary in each record
            data.append({
                "Date": fields.get("Date", ""),
                "Topic": fields.get("Topic", ""),
                "Subtopic": fields.get("Subtopic", ""),
                "Overall_Sentiment": fields.get("Overall_Sentiment", None),
                "Sentiment_Index": fields.get("Sentiment_Index", None),
                "Current_Sentiment_Score": fields.get("Current_Sentiment_Score", None),
                "Baseline_Sentiment_Score": fields.get("Baseline_Sentiment_Score", None),
                "Percent_Change": fields.get("Percent_Change", None),
                "Key_Drivers": fields.get("Key_Drivers", "")
            })

        df = pd.DataFrame(data)
        return df

    except Exception as e:
        logging.error(f"Error fetching data from Airtable: {e}")
        return pd.DataFrame()

def filter_data_by_topic(df, topic_column, topic_value):
    """
    Filter data by a dynamic topic column and value.
    
    Args:
        df (pd.DataFrame): The DataFrame to filter.
        topic_column (str): The column to filter by (e.g., "Company", "Publication").
        topic_value (str): The value to filter for (e.g., "Trump", "The New York Times").
    
    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if topic_column in df.columns:
        return df[df[topic_column].str.contains(topic_value, case=False, na=False)]
    else:
        logging.warning(f"Column '{topic_column}' not found in DataFrame.")
        return pd.DataFrame()

def filter_data_by_subtopic(df, subtopic_column, subtopic_value):
    """
    Filter data by a dynamic subtopic column and value.
    
    Args:
        df (pd.DataFrame): The DataFrame to filter.
        subtopic_column (str): The column to filter by (e.g., "Company", "Publication").
        subtopic_value (str): The value to filter for (e.g., "Trump", "The New York Times").
    
    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if subtopic_column in df.columns:
        return df[df[subtopic_column].str.contains(subtopic_value, case=False, na=False)]
    else:
        logging.warning(f"Column '{subtopic_column}' not found in DataFrame.")
        return pd.DataFrame()

def track_index_values_over_time(df):
    """
    Track index values (e.g., Overall_Sentiment, Opt_Pes_Score) over time.
    """
    try:
        if "Date" in df.columns and "Overall_Sentiment" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values(by="Date")
            return df.groupby("Date")["Overall_Sentiment"].mean().reset_index()
        else:
            logging.warning("Required columns 'Date' or 'Overall_Sentiment' not found in DataFrame.")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error tracking index values over time: {e}")
        return pd.DataFrame()

def check_data_quality(df):
    """
    Perform data quality checks.
    """
    try:
        # Check for missing values
        missing_values = df.isnull().sum()
        logging.info(f"Missing values:\n{missing_values}")

        # Check for duplicates
        duplicates = df.duplicated().sum()
        logging.info(f"Number of duplicate records: {duplicates}")

        # Check for invalid dates
        if "Date" in df.columns:
            invalid_dates = df[~pd.to_datetime(df["Date"], errors="coerce").notna()]
            logging.info(f"Invalid dates:\n{invalid_dates}")

        # Check for required columns
        required_columns = ["Date", "Topic", "Sentiment_Index"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(f"Missing required columns: {missing_columns}")
            return False

        return True
    except Exception as e:
        logging.error(f"Error performing data quality checks: {e}")
        return False

def prepare_data_for_visualization(df):
    """
    Organize data for visualization.
    """
    try:
        # Aggregate data by Company and calculate mean sentiment
        if "Company" in df.columns and "Overall_Sentiment" in df.columns:
            company_sentiment = df.groupby("Company")["Overall_Sentiment"].mean().reset_index()
            logging.info(f"Aggregated data for visualization:\n{company_sentiment}")
            return company_sentiment
        else:
            logging.warning("Required columns 'Company' or 'Overall_Sentiment' not found in DataFrame.")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error preparing data for visualization: {e}")
        return pd.DataFrame()
    
def calculate_sentiment_index(df):
    """
    Calculate the Sentiment Index using the formula: (Overall_Sentiment / 1) * 100

    Args:
        df (pd.DataFrame): DataFrame containing the Overall_Sentiment column.

    Returns:
        pd.DataFrame: DataFrame with added Sentiment Index column.
    """
    try:
        # Check if the 'Overall_Sentiment' column exists
        if "Overall_Sentiment" not in df.columns:
            logging.error("Column 'Overall_Sentiment' not found in DataFrame.")
            return df

        # Calculate Sentiment Index using the formula: (Overall_Sentiment / 1) * 100
        df["Sentiment Index"] = (df["Overall_Sentiment"] / 1) * 100
        logging.info("Sentiment Index calculated successfully.")
        return df
    except Exception as e:
        logging.error(f"Error calculating Sentiment Index: {e}")
        return df
    
def calculate_friday_to_friday_comparison(df):
    """
    Calculate Friday-to-Friday comparison for sentiment indices.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the data.
    
    Returns:
        pd.DataFrame: DataFrame with Friday-to-Friday comparisons.
    """
    try:
        if "Date" in df.columns and "Sentiment_Index" in df.columns:
            # Convert Date column to datetime
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values(by="Date")

            # Filter records to include only Fridays
            df["DayOfWeek"] = df["Date"].dt.day_name()
            fridays_df = df[df["DayOfWeek"] == "Friday"]

            # Calculate week-over-week change
            fridays_df["Previous Friday Sentiment Index"] = fridays_df["Sentiment_Index"].shift(1)
            fridays_df["Week-over-Week Change"] = (
                (fridays_df["Sentiment_Index"] - fridays_df["Previous Friday Sentiment Index"]) / 
                fridays_df["Previous Friday Sentiment Index"]
            ) * 100

            return fridays_df
        else:
            logging.warning("Required columns 'Date' or 'Sentiment Index' not found in DataFrame.")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error calculating Friday-to-Friday comparison: {e}")
        return pd.DataFrame()
