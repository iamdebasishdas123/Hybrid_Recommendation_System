import os
import pandas as pd
import ast
import logging
from pathlib import Path

# Logging configuration
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename="logs/data_preprocessing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configuration
DATA_PATH = r"C:\Users\Debasish Das\Desktop\Internship Project\Hybrid_rccomendation_System\data\raw"
OUTPUT_FILE = "data/preprocessed/merged_data.csv"

# Column configurations
USER_COLUMNS_TO_DROP = [
    "status", "message", "page", "page_size", "max_page_size", "last_name", "username", "email", "profile_url", 
    "website_url", "instagram-url", "youtube_url", "following", "tictok_url", "isVerified", "referral_code", 
    "has_wallet", "last_login", "is_online", "latitude", "longitude","following"
]
COMMON_COLUMNS_TO_DROP = [
    "status", "message", "page", "page_size", "max_page_size", "slug", "identifier", "contract_address", 
    "chain_id", "chart_url", "is_locked", "created_at", "last_name", "bookmarked", "thumbnail_url", 
    "gif_thumbnail_url", "picture_url", "category.image_url", "baseToken.address", "baseToken.name", 
    "baseToken.symbol", "baseToken.image_url","first_name","following","post_summary"
]

def read_and_normalize(file_path, column_to_normalize):
    try:
        df = pd.read_csv(file_path)
        if column_to_normalize in df.columns:
            df[column_to_normalize] = df[column_to_normalize].apply(ast.literal_eval)
            normalized_df = pd.json_normalize(df[column_to_normalize])
            return pd.concat([df.drop(columns=[column_to_normalize]), normalized_df], axis=1)
        logging.warning(f"Column {column_to_normalize} not found in {file_path}.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return pd.DataFrame()

def drop_columns(df, columns_to_drop):
    try:
        df.drop(columns=columns_to_drop, inplace=True, errors="ignore")
        return df
    except Exception as e:
        logging.warning(f"Error dropping columns: {e}")
        return df

def save_dataframe(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")
    except Exception as e:
        logging.error(f"Error saving DataFrame: {e}")

def merge_dataframes(dataframes, merge_keys,user):
    merged_df = dataframes[0]
    for df in dataframes[1:4]:
        merged_df = pd.merge(merged_df, df, on=merge_keys, how="outer")
    merged_df=pd.merge(user, merged_df, on="id", how="outer")
    return merged_df

def preprocess_data():
    try:
        files = sorted(os.listdir(DATA_PATH))
        user_df = read_and_normalize(os.path.join(DATA_PATH, files[0]), "users")
        inspire_df = read_and_normalize(os.path.join(DATA_PATH, files[1]), "posts")
        liked_df = read_and_normalize(os.path.join(DATA_PATH, files[2]), "posts")
        rating_df = read_and_normalize(os.path.join(DATA_PATH, files[3]), "posts")
        view_df = read_and_normalize(os.path.join(DATA_PATH, files[4]), "posts")

        user_df = drop_columns(user_df, USER_COLUMNS_TO_DROP)
        inspire_df = drop_columns(inspire_df, COMMON_COLUMNS_TO_DROP)
        liked_df = drop_columns(liked_df, COMMON_COLUMNS_TO_DROP)
        rating_df = drop_columns(rating_df, COMMON_COLUMNS_TO_DROP)
        view_df = drop_columns(view_df, COMMON_COLUMNS_TO_DROP)

        merge_keys = [
            "id", "title", "comment_count", "upvote_count", "view_count", "exit_count", "rating_count", 
            "average_rating", "share_count", "upvoted", "category.name", "category.count", "category.description", 
            "username", "category.id", "video_link"
        ]

        merged_df = merge_dataframes([liked_df, view_df, rating_df, inspire_df], merge_keys,user_df)
        
        save_dataframe(merged_df, OUTPUT_FILE)
        logging.info("Data preprocessing completed successfully.")
    except Exception as e:
        logging.error(f"Preprocessing failed: {e}")

if __name__ == "__main__":
    preprocess_data()
