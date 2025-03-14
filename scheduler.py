import schedule
import time
import logging
from main import main

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def run_pipeline():
    """
    Run the pipeline by calling the main function.
    """
    logging.info("Pipeline started.")
    try:
        main()  # Call the main function from main.py
        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")

# Schedule the pipeline to run daily at 8:00 AM
schedule.every().day.at("08:00").do(run_pipeline)

# Log the scheduler start
logging.info("Scheduler started. Waiting for the scheduled time...")

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)  # Sleep for 1 second to avoid high CPU usage