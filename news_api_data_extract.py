
import json
import requests #requests module not present in lamda so creating a layer
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    api_key = os.environ.get("api_key")
    
    def fetch_news(keyword):
        page_size = 10
        url = f"https://newsapi.org/v2/everything?q={keyword}&language=en&pageSize={page_size}&apiKey={api_key}"
        response = requests.get(url)
        data = response.json()
        return data["articles"]
    articles = fetch_news("stocks")
    #print(articles)

    client = boto3.client("s3")
    file_name = "news_" + str(datetime.now()) + ".json"
    #file_name = f"news_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    client.put_object(Bucket="my-news-pipeline",
    Key="raw_data/to_be_processed/"+ file_name,
    Body=json.dumps(articles)
)
