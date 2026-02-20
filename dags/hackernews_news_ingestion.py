from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import os

def fetch_hackernews(**context):
    url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    response = requests.get(url)
    story_ids = response.json()[:10]
    
    stories = []
    for sid in story_ids:
        story_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
        story = requests.get(story_url).json()
        if story:
            stories.append({
                'id': story.get('id'),
                'title': story.get('title'),
                'score': story.get('score', 0)
            })
    
    print(f"Fetched {len(stories)} stories")
    context['ti'].xcom_push(key='stories', value=stories)
    return len(stories)

def fetch_news(**context):
    url = "https://newsapi.org/v2/top-headlines"
    params = {
        'apiKey': '4c0b422955354a18bc0ceba15d288299',
        'category': 'technology',
        'pageSize': 10
    }
    
    response = requests.get(url, params=params)
    articles = response.json().get('articles', [])
    
    news = [{'title': a['title'], 'source': a['source']['name']} for a in articles]
    
    print(f"Fetched {len(news)} articles")
    context['ti'].xcom_push(key='news', value=news)
    return len(news)

def save_data(**context):
    stories = context['ti'].xcom_pull(task_ids='fetch_hackernews', key='stories')
    news = context['ti'].xcom_pull(task_ids='fetch_news', key='news')
    
    data = {'hackernews': stories, 'news': news}
    
    output_dir = '/opt/airflow/dags/output'
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = f"{output_dir}/data_{context['ds']}.json"
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved to {output_file}")
    return output_file

with DAG(
    'hackernews_news_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['demo']
) as dag:
    
    task1 = PythonOperator(task_id='fetch_hackernews', python_callable=fetch_hackernews)
    task2 = PythonOperator(task_id='fetch_news', python_callable=fetch_news)
    task3 = PythonOperator(task_id='save_data', python_callable=save_data)
    
    [task1, task2] >> task3