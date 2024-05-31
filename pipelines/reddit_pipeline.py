import sys
sys.path.append('C:/Users/DEEP/RedditDataEngineering')
from utils.constants import SECRET
from utils.constants import CLIENT_ID, OUTPUT_PATH
from etls.reddit_etl import connect_reddit
from etls.reddit_etl import extract_posts
from etls.reddit_etl import transform_data
from etls.reddit_etl import load_data_to_csv
import pandas as pd

def reddit_pipeline(file_name : str, subreddit : str, time_filter = 'day', limit = None):
    # Connection to reddit instance
    instance = connect_reddit(CLIENT_ID, SECRET, 'python:fozzy:v1.0 (by /u/Delpotro-21)')

    # Extraction
    posts = extract_posts(instance, subreddit, time_filter, limit)

    # Transformation
    post_df = pd.DataFrame(posts)
    post_df = transform_data(post_df)

    # Loading data to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path