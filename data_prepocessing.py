import os
import pandas as pd
import ast
import logging
from pathlib import Path

# Set up logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename="logs/data_preprocessing.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configuration
DATA_PATH = r"C:\Users\Debasish Das\Desktop\Internship Project\Hybrid_rccomendation_System\data\raw"
OUTPUT_FILE = "data\prepocessed\merged_data.csv"

def read_and_normalize(file_path, column_to_normalize):
    """
    Reads a CSV file and normalizes a JSON-like column.

    Args:
        file_path (str): Path to the CSV file.
        column_to_normalize (str): Column to apply normalization.

    Returns:
        pd.DataFrame: Normalized DataFrame.
    """
    try:
        logging.info(f"Reading and normalizing data from {file_path}...")
        df = pd.read_csv(file_path)
        if column_to_normalize in df.columns:
            df[column_to_normalize] = df[column_to_normalize].apply(ast.literal_eval)
            normalized_df = pd.json_normalize(df[column_to_normalize])
            logging.info(f"Normalization successful for column '{column_to_normalize}'.")
            return pd.concat([df.drop(columns=[column_to_normalize]), normalized_df], axis=1)
        else:
            logging.error(f"Column {column_to_normalize} not found in {file_path}.")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error reading and normalizing {file_path}: {str(e)}")
        return pd.DataFrame()


def drop_unused_columns(df, columns_to_drop):
    """
    Drops specified columns from a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        columns_to_drop (list): List of column names to drop.

    Returns:
        pd.DataFrame: DataFrame with specified columns dropped.
    """
    try:
        logging.info("Dropping unused columns...")
        df.drop(columns=columns_to_drop, inplace=True, errors="ignore")
        logging.info("Unused columns dropped successfully.")
    except Exception as e:
        logging.warning(f"Error dropping columns: {str(e)}")
    return df


def save_dataframe(df, file_path):
    try:
        logging.info(f"Saving DataFrame to {file_path}...")
        df.to_csv(file_path, index=False)
        logging.info(f"DataFrame saved successfully to {file_path}.")
    except Exception as e:
        logging.error(f"Error saving DataFrame to {file_path}: {str(e)}")


def main():
    try:
        logging.info("Starting data preprocessing...")

        # Load data files
        all_files = os.listdir(DATA_PATH)
        logging.info(f"Loaded data files: {all_files}")

        # Read and normalize data
        user_df = read_and_normalize(os.path.join(DATA_PATH, all_files[0]), "users")
        inspire_df = read_and_normalize(os.path.join(DATA_PATH, all_files[1]), "posts")
        liked_df = read_and_normalize(os.path.join(DATA_PATH, all_files[2]), "posts")
        rating_df = read_and_normalize(os.path.join(DATA_PATH, all_files[3]), "posts")
        view_df = read_and_normalize(os.path.join(DATA_PATH, all_files[4]), "posts")

        # Drop unused columns
        columns_to_drop_user = ["status","message","page","page_size","max_page_size",'last_name', 'username', 'email',
       'profile_url','website_url', 'instagram-url', 'youtube_url','following',
       'tictok_url', 'isVerified', 'referral_code', 'has_wallet', 'last_login', 'is_online', 'latitude', 'longitude'] 
        columns_to_drop=["status","message","page","page_size","max_page_size",'slug', 'identifier', 'contract_address', 'chain_id',
       'chart_url', 'is_locked', 'created_at', 'last_name',
       'bookmarked', 'thumbnail_url',
       'gif_thumbnail_url', 'picture_url', 'category.image_url', 'baseToken.address',
       'baseToken.name', 'baseToken.symbol', 'baseToken.image_url']
        user_df = drop_unused_columns(user_df, columns_to_drop_user)
        inspire_df = drop_unused_columns(inspire_df, columns_to_drop)
        liked_df = drop_unused_columns(liked_df, columns_to_drop)
        rating_df = drop_unused_columns(rating_df, columns_to_drop)
        view_df = drop_unused_columns(view_df, columns_to_drop)

        # merge data
        df=pd.merge(liked_df,view_df,on=["id","title",'comment_count', 'upvote_count',
       'view_count', 'exit_count', 'rating_count', 'average_rating',
       'share_count','video_link','upvoted','category.name', 'category.count',
       'category.description',"username",'category.id'],how="outer")
        df=pd.merge(df,rating_df,on=["id","title",'comment_count', 'upvote_count',
       'view_count', 'exit_count', 'rating_count', 'average_rating',
       'share_count','upvoted','category.name', 'category.count',
       'category.description',"username","video_link",'category.id'],how="outer")
        merged_df = pd.merge(df, inspire_df, on=["id","title",'comment_count', 'upvote_count',
       'view_count', 'exit_count', 'rating_count', 'average_rating',
       'share_count','upvoted','category.name', 'category.count','video_link',
       'category.description',"username",'category.id'], how="outer",suffixes=('_left', '_right'))
        
        # Save merged data
        save_dataframe(merged_df, OUTPUT_FILE)

        logging.info("Data preprocessing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during data preprocessing: {str(e)}")


if __name__ == "__main__":
    main()
