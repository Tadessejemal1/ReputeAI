from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Slack configuration
SLACK_TOKEN = "xoxb-your-slack-token"
SLACK_CHANNEL = "#pipeline-alerts"

def send_slack_notification(message):
    """
    Send a notification to Slack.
    """
    try:
        client = WebClient(token=SLACK_TOKEN)
        response = client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
        logging.info(f"Slack notification sent: {response['ts']}")
    except SlackApiError as e:
        logging.error(f"Slack API error: {e.response['error']}")