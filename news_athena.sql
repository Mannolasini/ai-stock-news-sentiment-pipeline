-- View the first few rows
SELECT * from news_sentiment_db.news_with_sentiment_data;

-- Count total records
SELECT COUNT(*) FROM news_sentiment_db.news_with_sentiment_data;

-- Count by sentiment type
SELECT sentiment, COUNT(*) AS count
FROM news_sentiment_db.news_with_sentiment_data
GROUP BY sentiment;

-- List all unique tags
SELECT DISTINCT tag FROM news_sentiment_db.news_with_sentiment_data;

-- Distribution of articles by word count
SELECT word_count, COUNT(*) AS count
FROM news_sentiment_db.news_with_sentiment_data
GROUP BY word_count
ORDER BY word_count DESC;

-- Sentiment trend over time (daily)
SELECT DATE(publishedAt) AS date, sentiment, COUNT(*) AS article_count
FROM news_with_sentiment_data
GROUP BY DATE(publishedAt), sentiment
ORDER BY date;

