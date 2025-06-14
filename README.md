# ai-stock-news-sentiment-pipeline
Implement Complete Data Pipeline Data Engineering Project using News API & Sentiment Analysis

Integrating with NewsAPI to extract real-time stock-related news articles.
Deploying extraction code on AWS Lambda to pull news headlines daily.
Adding CloudWatch trigger to automate daily extraction of articles from NewsAPI.
Writing transformation function using Python and VADER Sentiment Analyzer to classify news as Positive, Negative, or Neutral.
Building automated trigger to run the transformation Lambda after each extraction.
Storing raw and transformed files on S3, using a structured folder system:
Building analytics-ready tables using AWS Glue Crawler and running SQL queries via Athena
