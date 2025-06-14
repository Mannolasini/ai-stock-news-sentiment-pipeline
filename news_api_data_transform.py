import json
import boto3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
from datetime import datetime
from io import StringIO

analyzer = SentimentIntensityAnalyzer()
def get_tags(text):
    tags = []
    keywords = {
        "earnings": ["earnings", "quarterly results", "revenue"],
        "ipo": ["IPO", "initial public offering"],
        "merger": ["merger", "acquisition", "buyout"],
        "layoffs": ["layoff", "job cut", "downsizing"]
    }
    text = text.lower()
    for tag, words in keywords.items():
        if any(word in text for word in words):
            tags.append(tag)
    return tags


def analyze_sentiment(title, description):
    full_text = (title or "") + ". " + (description or "")
    vs = analyzer.polarity_scores(full_text)
    score = vs['compound']
    
    if score > 0.1:
        sentiment = "Positive"
    elif score < -0.1:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"
    return sentiment, score, full_text

def fetch_news_with_sentiment(articles):
    result = []
    for article in articles:
        title = article.get("title", "") or ""
        description = article.get("description", "") or ""
        publishedAt = article.get("publishedAt", "")
        url = article.get("url", "")
        source_name = article.get("source", {}).get("name", "Unknown Source")  # fallback for missing
        
        sentiment, score, full_text = analyze_sentiment(title, description)
        tags = get_tags(title + " " + description)
        
        result.append({
            "title": title,
            "description": description,
            "publishedAt": publishedAt,
            "url": url,
            "sentiment": sentiment,
            "score": score,
            "source_name": source_name,
            "word_count": len(full_text.split()),
            "title_length": len(title),
            "tag": ", ".join(tags)
        })
    return result


def lambda_handler(event, context):
    s3= boto3.client("s3")
    Bucket = "my-news-pipeline"
    Key= "raw_data/to_be_processed/"
    news_data = []
    key_data=[]


    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)["Contents"]:
        file_key = file["Key"] 
        if (file_key.split('.')[-1] == "json"):
            response = s3.get_object(Bucket=Bucket,Key=file_key)
            content= response["Body"]
            articles = json.loads(content.read())
            news_data.append(articles)
            key_data.append(file_key)

    for a in news_data:
        final_result = fetch_news_with_sentiment(a)
        df = pd.DataFrame(final_result)
        print(df.head())
        df.drop_duplicates(inplace=True)
        #df["publishedAt"] = pd.to_datetime(df["publishedAt"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        df["publishedAt"] = pd.to_datetime(df["publishedAt"])
        news_sentiment_data_key = "transformed_data/news_with_sentiment_data/news_with_sentiment_transformed_" + str(datetime.now()) + ".csv"
        news_data_buffer = StringIO()
        df.to_csv(news_data_buffer,index=False)
        news_data_content = news_data_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=news_sentiment_data_key,Body=news_data_content)

    s3_resource = boto3.resource('s3')
    for key in key_data:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
