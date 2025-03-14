import matplotlib.pyplot as plt
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def plot_sentiment_index_over_time(df, output_file="sentiment_index_over_time.png"):
    """
    Plot Date vs Sentiment Index and save the chart as a PNG file.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data.
        output_file (str): The name of the output PNG file.
    """
    try:
        if "Date" in df.columns and "Sentiment_Index" in df.columns:
            # Convert Date column to datetime
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values(by="Date")

            # Plot Sentiment Index over time
            plt.figure(figsize=(10, 6))
            plt.plot(df["Date"], df["Sentiment_Index"], marker="o", linestyle="-", color="b")
            plt.title("Sentiment Index Over Time")
            plt.xlabel("Date")
            plt.ylabel("Sentiment Index")
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the plot as a PNG file
            plt.savefig(output_file)
            logging.info(f"Sentiment Index over time chart saved as {output_file}")
            plt.close()
        else:
            logging.warning("Required columns 'Date' or 'Sentiment Index' not found in DataFrame.")
    except Exception as e:
        logging.error(f"Error plotting Sentiment Index over time: {e}")

def plot_sentiment_by_topic(df, output_file="sentiment_by_topic.png"):
    """
    Plot sentiment by topic and save the chart as a PNG file.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data.
        output_file (str): The name of the output PNG file.
    """
    try:
        if "Topic" in df.columns and "Overall_Sentiment" in df.columns:
            # Group by Topic and calculate mean sentiment
            topic_sentiment = df.groupby("Topic")["Overall_Sentiment"].mean().reset_index()

            # Plot sentiment by topic
            plt.figure(figsize=(10, 6))
            plt.bar(topic_sentiment["Topic"], topic_sentiment["Overall_Sentiment"], color="skyblue")
            plt.title("Sentiment by Topic")
            plt.xlabel("Topic")
            plt.ylabel("Average Overall Sentiment")
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the plot as a PNG file
            plt.savefig(output_file)
            logging.info(f"Sentiment by topic chart saved as {output_file}")
            plt.close()
        else:
            logging.warning("Required columns 'Topic' or 'Overall_Sentiment' not found in DataFrame.")
    except Exception as e:
        logging.error(f"Error plotting sentiment by topic: {e}")
        
def plot_friday_to_friday_comparison(df, output_file="friday_to_friday_comparison.png"):
    """
    Plot Friday-to-Friday comparison for sentiment indices.
    
    Args:
        df (pd.DataFrame): DataFrame containing the data.
        output_file (str): The name of the output PNG file.
    """
    try:
        if "Date" in df.columns and "Week-over-Week Change" in df.columns:
            # Plot Friday-to-Friday comparison
            plt.figure(figsize=(10, 6))
            plt.plot(df["Date"], df["Week-over-Week Change"], marker="o", linestyle="-", color="r")
            plt.title("Friday-to-Friday Sentiment Index Comparison")
            plt.xlabel("Date")
            plt.ylabel("Week-over-Week Change (%)")
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()

            # Save the plot as a PNG file
            plt.savefig(output_file)
            logging.info(f"Friday-to-Friday comparison chart saved as {output_file}")
            plt.close()
        else:
            logging.warning("Required columns 'Date' or 'Week-over-Week Change' not found in DataFrame.")
    except Exception as e:
        logging.error(f"Error plotting Friday-to-Friday comparison: {e}")