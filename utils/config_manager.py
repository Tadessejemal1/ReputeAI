import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config(file_path="config.json"):
    """
    Load the configuration file.
    
    Args:
        file_path (str): Path to the configuration file.
    
    Returns:
        dict: Configuration data.
    """
    try:
        with open(file_path, "r") as file:
            config = json.load(file)
        logging.info("Configuration file loaded successfully.")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {file_path}")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON format in configuration file: {file_path}")
        return {}

def get_topics(config):
    """
    Get the list of topics from the configuration.
    
    Args:
        config (dict): Configuration data.
    
    Returns:
        list: List of topics.
    """
    return list(config.get("topics", {}).keys())

def get_subtopics(config, topic):
    """
    Get the list of subtopics for a given topic.
    
    Args:
        config (dict): Configuration data.
        topic (str): The topic to get subtopics for.
    
    Returns:
        list: List of subtopics.
    """
    return config.get("topics", {}).get(topic, {}).get("subtopics", [])

def get_disruptors(config):
    """
    Get the list of disruptors from the configuration.
    
    Args:
        config (dict): Configuration data.
    
    Returns:
        list: List of disruptors.
    """
    return config.get("disruptors", [])