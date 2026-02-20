from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import uuid

def fetch_hackernews(**context):
    """Fetch HackerNews stories"""
    try:
        url = "https://hacker-news.firebaseio.com/v0/topstories.json"
        response = requests.get(url, timeout=10)
        story_ids = response.json()[:30]
        
        stories = []
        for sid in story_ids:
            try:
                story_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
                story = requests.get(story_url, timeout=5).json()
                if story and story.get('type') == 'story':
                    stories.append({
                        'id': story.get('id'),
                        'title': story.get('title', ''),
                        'url': story.get('url', ''),
                        'score': story.get('score', 0),
                        'by': story.get('by', 'unknown'),
                        'time': story.get('time', 0)
                    })
            except:
                continue
        
        print(f"✅ Fetched {len(stories)} HackerNews stories")
        context['ti'].xcom_push(key='stories', value=stories)
        return len(stories)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def fetch_news(**context):
    """Fetch News API articles"""
    try:
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            'apiKey': '4c0b422955354a18bc0ceba15d288299',
            'category': 'technology',
            'language': 'en',
            'pageSize': 30
        }
        response = requests.get(url, params=params, timeout=10)
        articles = response.json().get('articles', [])
        
        news = []
        for article in articles:
            news.append({
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'source': article['source']['name'],
                'published_at': article.get('publishedAt', '')
            })
        
        print(f"✅ Fetched {len(news)} news articles")
        context['ti'].xcom_push(key='news', value=news)
        return len(news)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def load_to_snowflake(**context):
    """Bulk load ALL data to Snowflake Bronze in one SQL call"""
    try:
        stories = context['ti'].xcom_pull(task_ids='fetch_hackernews', key='stories')
        news = context['ti'].xcom_pull(task_ids='fetch_news', key='news')
        
        run_id = str(uuid.uuid4())
        execution_date = context['execution_date']
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        all_rows = []

        for story in stories:
            load_id = f"HN_{story['id']}_{execution_date.strftime('%Y%m%d%H%M')}"
            json_data = json.dumps(story).replace("'", "''")
            all_rows.append(f"""
                '{load_id}', 'hackernews', 'story',
                PARSE_JSON('{json_data}'), '{execution_date}'
            """)

        for article in news:
            load_id = f"NEWS_{abs(hash(article['url']))}_{execution_date.strftime('%Y%m%d%H%M')}"
            json_data = json.dumps(article).replace("'", "''")
            all_rows.append(f"""
                '{load_id}', 'news_api', 'article',
                PARSE_JSON('{json_data}'), '{execution_date}'
            """)

        union_sql = "\nUNION ALL\nSELECT ".join(all_rows)
        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.BRONZE.RAW_SOCIAL_MEDIA_DATA
        (load_id, source, content_type, raw_data, fetched_at)
        SELECT {union_sql}
        """)
        print(f"✅ Bulk loaded {len(all_rows)} records to BRONZE")

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.BRONZE.PIPELINE_RUNS
        (run_id, dag_id, execution_date, records_fetched, records_loaded,
         status, started_at, completed_at)
        SELECT '{run_id}', '{context['dag'].dag_id}', '{execution_date}',
            {len(all_rows)}, {len(all_rows)}, 'SUCCESS',
            '{execution_date}', CURRENT_TIMESTAMP()
        """)
        return len(all_rows)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def transform_to_silver(**context):
    """Transform BRONZE → SILVER entirely inside Snowflake"""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')

        hook.run(f"""
        DELETE FROM SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        WHERE DATE(loaded_at) = '{date_str}'
        """)

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        (post_id, source, title, url, score, author, description, published_at, loaded_at)
        SELECT
            raw_data:id::VARCHAR, 'hackernews',
            TRIM(raw_data:title::VARCHAR),
            TRIM(raw_data:url::VARCHAR),
            raw_data:score::INT,
            TRIM(raw_data:by::VARCHAR),
            NULL,
            TO_TIMESTAMP(raw_data:time::INT),
            CURRENT_TIMESTAMP()
        FROM SOCIAL_ANALYTICS.BRONZE.RAW_SOCIAL_MEDIA_DATA
        WHERE source = 'hackernews'
          AND DATE(fetched_at) = '{date_str}'
          AND TRIM(raw_data:url::VARCHAR) != ''
          AND raw_data:url::VARCHAR IS NOT NULL
          AND TRIM(raw_data:title::VARCHAR) != ''
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY raw_data:id::VARCHAR
            ORDER BY raw_data:score::INT DESC
        ) = 1
        """)
        print("✅ HackerNews → SILVER")

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        (post_id, source, title, url, score, author, description, published_at, loaded_at)
        SELECT
            load_id, 'news_api',
            TRIM(REGEXP_REPLACE(raw_data:title::VARCHAR, ' - [^-]+$', '')),
            TRIM(raw_data:url::VARCHAR),
            0,
            TRIM(raw_data:source::VARCHAR),
            TRIM(raw_data:description::VARCHAR),
            TRY_TO_TIMESTAMP(raw_data:published_at::VARCHAR),
            CURRENT_TIMESTAMP()
        FROM SOCIAL_ANALYTICS.BRONZE.RAW_SOCIAL_MEDIA_DATA
        WHERE source = 'news_api'
          AND DATE(fetched_at) = '{date_str}'
          AND raw_data:title::VARCHAR IS NOT NULL
          AND TRIM(raw_data:title::VARCHAR) != ''
          AND raw_data:url::VARCHAR IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY raw_data:url::VARCHAR
            ORDER BY raw_data:published_at::VARCHAR DESC
        ) = 1
        """)
        print("✅ News API → SILVER")

        results = hook.get_records(f"""
        SELECT source, COUNT(*) as total
        FROM SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        WHERE DATE(loaded_at) = '{date_str}'
        GROUP BY source
        """)
        for row in results:
            print(f"   {row[0]}: {row[1]} records")
        return sum(row[1] for row in results)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def transform_to_gold(**context):
    """Aggregate SILVER → GOLD inside Snowflake"""
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')

        hook.run(f"DELETE FROM SOCIAL_ANALYTICS.GOLD.DAILY_TOP_STORIES WHERE report_date = '{date_str}'")
        hook.run(f"DELETE FROM SOCIAL_ANALYTICS.GOLD.DAILY_SOURCE_SUMMARY WHERE report_date = '{date_str}'")

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.GOLD.DAILY_TOP_STORIES
        (report_date, source, title, url, score, author, rank, loaded_at)
        SELECT
            DATE(published_at), source, title, url, score, author,
            ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC),
            CURRENT_TIMESTAMP()
        FROM SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        WHERE DATE(loaded_at) = '{date_str}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY source ORDER BY score DESC) <= 10
        """)
        print("✅ GOLD: Top stories loaded")

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.GOLD.DAILY_SOURCE_SUMMARY
        (report_date, source, total_articles, avg_score, max_score, top_story, loaded_at)
        SELECT
            '{date_str}'::DATE, source,
            COUNT(*), ROUND(AVG(score), 1), MAX(score),
            MAX_BY(title, score), CURRENT_TIMESTAMP()
        FROM SOCIAL_ANALYTICS.SILVER.SOCIAL_POSTS_CLEAN
        WHERE DATE(loaded_at) = '{date_str}'
        GROUP BY source
        """)
        print("✅ GOLD: Source summary loaded")
        return True

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

def build_combined_insights(**context):
    """
    COMBINED_INSIGHTS — the 'so what' of the entire project.
    Joins trending tech news with crypto price movements.
    Answers: 'When AI stories trend, what happens to crypto prices?'
    """
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')

        hook.run(f"""
        DELETE FROM SOCIAL_ANALYTICS.GOLD.COMBINED_INSIGHTS
        WHERE report_date = '{date_str}'
        """)

        hook.run(f"""
        INSERT INTO SOCIAL_ANALYTICS.GOLD.COMBINED_INSIGHTS
        (report_date, top_hn_story, hn_max_score, total_articles,
         btc_avg_price, eth_avg_price, sol_avg_price, btc_price_change, loaded_at)
        SELECT
            '{date_str}'::DATE                              AS report_date,

            -- Top trending HackerNews story today
            news.top_story                                  AS top_hn_story,
            news.max_score                                  AS hn_max_score,
            news.total_articles                             AS total_articles,

            -- Average crypto prices today from streaming data
            crypto.btc_avg                                  AS btc_avg_price,
            crypto.eth_avg                                  AS eth_avg_price,
            crypto.sol_avg                                  AS sol_avg_price,

            -- BTC price change: today's avg vs yesterday's avg
            ROUND(
                ((crypto.btc_avg - crypto.btc_prev) / NULLIF(crypto.btc_prev, 0)) * 100,
                2
            )                                               AS btc_price_change,

            CURRENT_TIMESTAMP()                             AS loaded_at

        FROM (
            -- News summary: top story + stats from GOLD
            SELECT
                MAX_BY(top_story, max_score)    AS top_story,
                MAX(max_score)                  AS max_score,
                SUM(total_articles)             AS total_articles
            FROM SOCIAL_ANALYTICS.GOLD.DAILY_SOURCE_SUMMARY
            WHERE report_date = '{date_str}'
              AND source = 'hackernews'
        ) news

        CROSS JOIN (
            -- Crypto prices: today's averages from Silver streaming data
            SELECT
                AVG(CASE WHEN symbol = 'BTC-USD' THEN price END) AS btc_avg,
                AVG(CASE WHEN symbol = 'ETH-USD' THEN price END) AS eth_avg,
                AVG(CASE WHEN symbol = 'SOL-USD' THEN price END) AS sol_avg,
                -- Yesterday's BTC avg for price change calculation
                (SELECT AVG(price)
                 FROM SOCIAL_ANALYTICS.SILVER.CRYPTO_PRICES_CLEAN
                 WHERE symbol = 'BTC-USD'
                   AND DATE(event_time) = DATEADD(day, -1, '{date_str}')
                ) AS btc_prev
            FROM SOCIAL_ANALYTICS.SILVER.CRYPTO_PRICES_CLEAN
            WHERE DATE(event_time) = '{date_str}'
        ) crypto
        """)

        # Print the combined insight
        result = hook.get_first(f"""
        SELECT
            report_date, top_hn_story, hn_max_score,
            total_articles, btc_avg_price, eth_avg_price,
            sol_avg_price, btc_price_change
        FROM SOCIAL_ANALYTICS.GOLD.COMBINED_INSIGHTS
        WHERE report_date = '{date_str}'
        """)

        if result:
            print("\n🔍 COMBINED INSIGHTS Report:")
            print("=" * 60)
            print(f"Date          : {result[0]}")
            print(f"Top HN Story  : {result[1]}")
            print(f"HN Score      : {result[2]}")
            print(f"Total Articles: {result[3]}")
            print(f"BTC Avg Price : ${result[4]:,.2f}" if result[4] else "BTC Avg Price : N/A")
            print(f"ETH Avg Price : ${result[5]:,.2f}" if result[5] else "ETH Avg Price : N/A")
            print(f"SOL Avg Price : ${result[6]:,.2f}" if result[6] else "SOL Avg Price : N/A")
            print(f"BTC Change    : {result[7]}%" if result[7] else "BTC Change    : N/A (need 2 days of data)")
            print("=" * 60)

        return True

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise

# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    'social_media_to_snowflake',
    default_args={
        'owner': 'data_engineer',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Full pipeline: APIs → Bronze → Silver → Gold → Combined Insights',
    schedule='*/15 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'bronze', 'silver', 'gold', 'production'],
) as dag:

    t1 = PythonOperator(task_id='fetch_hackernews', python_callable=fetch_hackernews)
    t2 = PythonOperator(task_id='fetch_news', python_callable=fetch_news)
    t3 = PythonOperator(task_id='load_to_snowflake', python_callable=load_to_snowflake)
    t4 = PythonOperator(task_id='transform_to_silver', python_callable=transform_to_silver)
    t5 = PythonOperator(task_id='transform_to_gold', python_callable=transform_to_gold)
    t6 = PythonOperator(task_id='build_combined_insights', python_callable=build_combined_insights)

    # Full pipeline:
    # fetch in parallel → bulk load bronze → clean silver → aggregate gold → combine insights
    [t1, t2] >> t3 >> t4 >> t5 >> t6