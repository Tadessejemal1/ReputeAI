import logging
import json
import argparse
from utils.config_manager import load_config, get_topics, get_subtopics, get_disruptors
from pipeline.data_collection import fetch_data_from_octoparse
from pipeline.sentiment_analysis import analyze_article, analyze_relevance
from pipeline.store_data_in_Airtable import store_data_in_airtable
from task_pipeline.task5_Duplicate_check import prep_identify_duplicate_syndicate
from pipeline.index_calculation import (
    fetch_index_data_from_airtable,
    filter_data_by_topic,
    filter_data_by_subtopic,
    track_index_values_over_time,
    check_data_quality,
    prepare_data_for_visualization,
    calculate_friday_to_friday_comparison,
    calculate_sentiment_index
)
from pipeline.visualization import (
    plot_sentiment_index_over_time,
    plot_sentiment_by_topic,
    plot_friday_to_friday_comparison,
)

# Set up logging to print to the terminal
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# List of task IDs to fetch data from
TASK_IDS = [
    # "c1db3e19-6adb-413d-9b6e-dc6c0b00eaaa",  # Example task ID
    "bff732d8-cb71-4c48-9eeb-fcf9bcad06b8",  # Example task ID
    # "96826ce7-30b2-4e96-9149-5e1224482a4d"
]

def main():
    # Load configuration
    config = load_config()
    if not config:
        logging.error("Failed to load configuration. Exiting.")
        return

    # Get topics and subtopics
    topics = get_topics(config)
    logging.info(f"Available topics: {topics}")

    all_data = []  # List to store data from all tasks

    # Step 1: Fetch data from Octoparse and store in Airtable
    for task_id in TASK_IDS:
        logging.info(f"Fetching data for task ID: {task_id}")
        # Fetch data from Octoparse
        data, request_id = fetch_data_from_octoparse(task_id, size=5)

        if data:
            for record in data:
                # Add the requestId to each record
                record["requestId"] = request_id

                # Calculate the Prep_Identify_Duplicate_Syndicate score
                duplicate_syndicate_score = prep_identify_duplicate_syndicate(record, all_data)
                logging.info(f"Prep_Identify_Duplicate_Syndicate score: {duplicate_syndicate_score}")
                record["Prep_Identify_Duplicate_Syndicate"] = duplicate_syndicate_score

                # Perform sentiment analysis only if the score is 1 or 2
                if duplicate_syndicate_score in [1, 2]:
                    try:
                        headline = record.get("Headline", "")
                        body = record.get("Body", "") or record.get("Content", "")
                        
                        if not body:
                            logging.warning(f"No body content found for article: {headline}")
                            continue  # Skip this article if there's no body content
                        
                        # Analyze the article
                        sentiment_data = analyze_article(headline, body)
                        relevance_score = analyze_relevance(headline, body)
                        
                        if sentiment_data:
                            # Add sentiment data to the record
                            record["Sentiment Data"] = sentiment_data
                            logging.info(f"Analyzed sentiment for article: {headline}")
                            logging.info(f"Full JSON response: {json.dumps(sentiment_data, indent=2)}")
                        
                        if relevance_score:
                            # Add relevance score to the record
                            record["Relevance Score"] = relevance_score
                            logging.info(f"Relevance Score: {relevance_score}. Headline: {headline}")
                    
                    except Exception as e:
                        logging.error(f"Error analyzing article: {headline} - {e}")
                else:
                    logging.info(f"Skipping sentiment analysis for record with score: {duplicate_syndicate_score}")
                    
                # Store the record in Airtable (regardless of score)
                store_data_in_airtable([record])
                all_data.append(record)
        else:
            logging.warning(f"No data fetched for task ID: {task_id}")
    
    # Step 2: Fetch index data from Airtable
    df = fetch_index_data_from_airtable()
    if df.empty:
        logging.warning("No data fetched from Airtable. Exiting.")
        return

    # Step 3: Perform data quality checks
    if not check_data_quality(df):
        logging.error("Data quality checks failed. Exiting.")
        return

    # Step 4: Filter data by dynamic topic and subtopic
    for topic in topics:
        subtopics = get_subtopics(config, topic)
        for subtopic in subtopics:
            filtered_by_topic = filter_data_by_topic(df, "Topic", topic)
            filtered_by_subtopic = filter_data_by_subtopic(filtered_by_topic, "Subtopic", subtopic)

            logging.info(f"Filtered by topic '{topic}' and subtopic '{subtopic}': {len(filtered_by_subtopic)} records")

    # Step 5: Track index values over time
    index_over_time = track_index_values_over_time(df)
    logging.info(f"Index values over time:\n{index_over_time}")

    # Step 6: Calculate Friday-to-Friday comparison
    friday_comparison_df = calculate_friday_to_friday_comparison(df)
    logging.info(f"Friday-to-Friday comparison data:\n{friday_comparison_df}")

    # Step 7: Prepare data for visualization
    visualization_data = prepare_data_for_visualization(df)
    logging.info(f"Data prepared for visualization:\n{visualization_data}")

    # Step 8: Generate visualizations
    if not df.empty:
        # Calculate Sentiment Index if not already calculated
        if "Sentiment_Index" not in df.columns:
            df = calculate_sentiment_index(df)

        # Plot Date vs Sentiment Index
        plot_sentiment_index_over_time(df, "sentiment_index_over_time.png")
    else:
        logging.error("No data available for visualization.")

    # Step 9: Generate additional visualizations
    plot_sentiment_by_topic(df, "sentiment_by_topic.png")
    plot_friday_to_friday_comparison(friday_comparison_df, "friday_to_friday_comparison.png")

if __name__ == "__main__":
    # Run the main function
    main()