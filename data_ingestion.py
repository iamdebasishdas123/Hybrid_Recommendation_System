import requests
import pandas as pd
import os
import logging
from pathlib import Path

# Ensure directories exist
Path("data").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    filename="logs/data_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Common headers
HEADERS = {"Flic-Token": "flic_6e2d8d25dc29a4ddd382c2383a903cf4a688d1a117f6eb43b35a1e7fadbb84b8"}

def fetch_data(endpoint, output_file):
    """
    Fetch data from the given API endpoint and save it to a CSV file.

    Args:
        endpoint (str): The API endpoint to fetch data from.
        output_file (str): The file path to save the fetched data.
    """
    try:
        url = f"https://api.socialverseapp.com/{endpoint}"
        response = requests.get(url, headers=HEADERS)
        
        # Check for errors in the response
        if response.status_code != 200:
            logging.error(f"Failed to fetch data from {url}. Status code: {response.status_code}. Response: {response.text}")
            return
        
        # Parse JSON data and save to CSV
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(output_file, index=False)
        logging.info(f"Data from {url} saved successfully to {output_file}.")
    
    except Exception as e:
        logging.error(f"An error occurred while fetching data from {url}: {str(e)}")

def fetch_viewed_posts():
    fetch_data("posts/view?page=1&page_size=1000", "data/raw/view.csv")

def fetch_liked_posts():
    fetch_data("posts/like?page=1&page_size=1000", "data/raw/liked.csv")

def fetch_inspired_posts():
    fetch_data("posts/inspire?page=1&page_size=1000", "data/raw/inspire.csv")

def fetch_rated_posts():
    fetch_data("posts/rating?page=1&page_size=1000", "data/raw/rating.csv")

def fetch_users():
    fetch_data("users/get_all?page=1&page_size=1000", "data/raw/get_all.csv")

if __name__ == "__main__":
    try:
        logging.info("Starting data ingestion...")
        
        # Fetch all data
        fetch_viewed_posts()
        fetch_liked_posts()
        fetch_inspired_posts()
        fetch_rated_posts()
        fetch_users()
        
        logging.info("Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main function: {str(e)}")
