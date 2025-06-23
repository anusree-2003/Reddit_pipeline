from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import praw
import json
import boto3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def fetch_reddit_and_upload():
    # Initialize Reddit API
    reddit = praw.Reddit(
        client_id=os.environ['my_id'],
        client_secret=os.environ['secret_id'],
        user_agent=os.environ['user_agent']
    )

    # Fetch posts
    posts = []
    for submission in reddit.subreddit('sports').new(limit=100):
        posts.append({
            'title': submission.title,
            'score': submission.score,
            'url': submission.url,
            'author': str(submission.author)
        })

    # Convert list of posts into a proper JSON array string
    json_data = json.dumps(posts, indent=2)

    # Define S3 location
    s3 = boto3.client('s3')
    bucket_name = 'myredditbuckkk'
    key_name = f'raw_data/redditdata_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'

    # Upload to S3
    try:
        s3.put_object(Bucket=bucket_name, Key=key_name, Body=json_data)
        print(f"✅ Upload successful: {key_name}")
    except Exception as e:
        print("❌ Upload failed:", e)
        raise

# Default DAG arguments
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id='reddit_to_s3_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    reddit_task = PythonOperator(
        task_id='fetch_and_upload_to_s3',
        python_callable=fetch_reddit_and_upload
    )
