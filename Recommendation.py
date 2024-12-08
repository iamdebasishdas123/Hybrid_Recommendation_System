import pandas as pd
import logging
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
import numpy as np
from pathlib import Path
import json

# Set up logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    filename="logs/recomendation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Define function to load data
def load_data(file_path):
    try:
        data = pd.read_csv(file_path)
        logging.info(f"Data loaded successfully from {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error loading data from {file_path}: {e}")
        return None

# Define function to preprocess data
def preprocess_data(merged_data):
    try:
        # Convert relevant columns to numeric, forcing errors to NaN
        merged_data['comment_count'] = pd.to_numeric(merged_data['comment_count'], errors='coerce').fillna(0)
        merged_data['upvote_count'] = pd.to_numeric(merged_data['upvote_count'], errors='coerce').fillna(0)
        merged_data['view_count'] = pd.to_numeric(merged_data['view_count'], errors='coerce').fillna(0)
        merged_data['share_count'] = pd.to_numeric(merged_data['share_count'], errors='coerce').fillna(0)
        # merged_data["follower_count"] = merged_data["follower_count"].fillna('')
        # merged_data["following_count"] = merged_data["following_count"].fillna('')

        # Calculate engagement score
        merged_data['engagement_score'] = (
            merged_data['comment_count'] +
            merged_data['upvote_count'] +
            merged_data['view_count'] +
            merged_data['share_count']
        )

        # Ensure all text columns are strings before concatenation
        merged_data['title'] = merged_data['title'].astype(str)
        merged_data['post_summary_x'] = merged_data['post_summary_x'].astype(str)
        merged_data['category.name'] = merged_data['category.name'].astype(str)
        merged_data['category.description'] = merged_data['category.description'].astype(str)
        # merged_data["role"] = merged_data["role"].astype(str)
        # merged_data["follower_count"] = merged_data["follower_count"].astype(str)
        # merged_data["bio"] = merged_data["bio"].astype(str)
        # merged_data["following_count"] = merged_data["following_count"].astype(str)
        # merged_data["is_verified"] = merged_data["is_verified"].astype(str)

        # Combine text-based columns to create a metadata summary for the content
        merged_data['metadata'] = (
            merged_data['title'] + ' ' +
            merged_data['post_summary_x'] + ' ' +
            merged_data['category.name'] + ' ' +
            merged_data['category.description']
        )
        logging.info("Data preprocessing completed successfully.")
    except Exception as e:
        logging.error(f"Error during data preprocessing: {e}")

# Define function for TF-IDF vectorization
def compute_cosine_similarity(merged_data):
    try:
        tfidf = TfidfVectorizer(stop_words="english")
        tfidf_matrix = tfidf.fit_transform(merged_data["metadata"])
        cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)
        logging.info("Cosine similarity computed successfully.")
        return cosine_sim
    except Exception as e:
        logging.error(f"Error during TF-IDF vectorization or cosine similarity computation: {e}")
        return None

# Function to recommend content based on similarity
def recommend_content_based(video_id, cosine_sim, posts_df):
    try:
        idx = posts_df[posts_df["id"] == video_id].index[0]
        similarity_scores = list(enumerate(cosine_sim[idx]))
        similarity_scores = sorted(similarity_scores, key=lambda x: x[1], reverse=True)
        similar_videos = [posts_df.iloc[i[0]]["video_link"] for i in similarity_scores[0:6]]
        return similar_videos
    except Exception as e:
        logging.error(f"Error in recommend_content_based: {e}")
        return []

# Define function for collaborative filtering
def recommend_collaborative(user, user_item_matrix, user_factors, item_factors, df):
    try:
        user_idx = user_item_matrix.index.get_loc(user)
        user_vector = user_factors[user_idx]
        scores = np.dot(user_vector, item_factors)
        top_item_indices = np.argsort(scores)[::-1][:5]
        ids = user_item_matrix.columns[top_item_indices]
        recommendations = df[df["id"].isin(ids)]
        return recommendations[["username", "id", "title", "video_link", "engagement_score"]].to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error in recommend_collaborative: {e}")
        return []

# Define function for new user recommendations
def recommend_new_user_videos(merged_data):
    try:
        top_categories = (
            merged_data.groupby("category.name")["view_count"]
            .sum()
            .sort_values(ascending=False)
            .head(6)
            .index
        )

        category_recommendations = (
            merged_data[merged_data["category.name"].isin(top_categories)]
            .sort_values(by="view_count", ascending=False)
            .head(6)
        )

        top_rated_videos = (
            merged_data.sort_values(by="average_rating", ascending=False)
            .head(6)
        )

        most_commented_videos = (
            merged_data.sort_values(by="comment_count", ascending=False)
            .head(6)
        )

        top_viewed_videos = (
            merged_data.sort_values(by="view_count", ascending=False)
            .head(6)
        )

        recommendations = pd.concat(
            [category_recommendations, top_rated_videos, most_commented_videos, top_viewed_videos]
        ).drop_duplicates(subset="id")

        return recommendations[["id", "title", "category.name", "video_link", "view_count", "average_rating", "comment_count"]].sample(5).to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error in recommend_new_user_videos: {e}")
        return []

# Define hybrid recommendation function
def recommend_hybrid(user, user_item_matrix, user_factors, item_factors, cosine_sim, posts_df):
    try:
        if user in user_item_matrix.index:
            content = recommend_collaborative(user, user_item_matrix, user_factors, item_factors, posts_df)
            user_based = recommend_collaborative(user, user_item_matrix, user_factors, item_factors, posts_df)
            recommendation = pd.concat([content, user_based])
            return recommendation.to_dict(orient="records")
        else:
            return recommend_new_user_videos(posts_df)
    except Exception as e:
        logging.error(f"Error in recommend_hybrid: {e}")
        return []

# Main script
merged_data = load_data(r"C:\Users\Debasish Das\Desktop\Internship Project\Hybrid_rccomendation_System\data\prepocessed\merged_data.csv")
if merged_data is not None:
    preprocess_data(merged_data)
    cosine_sim = compute_cosine_similarity(merged_data)

    # Create a User-Item Interaction Matrix
    user_item_matrix = merged_data.pivot_table(index="username", columns="id", values="engagement_score", fill_value=0)

    # Apply SVD
    svd = TruncatedSVD(n_components=10)
    user_factors = svd.fit_transform(user_item_matrix)
    item_factors = svd.components_

    user = "new_user"  # Test with a new user
    recommended_videos = recommend_hybrid(user, user_item_matrix, user_factors, item_factors, cosine_sim, merged_data)
    
    with open('recommendations.json', 'w') as file:
        json.dump(recommended_videos, file, indent=4)
