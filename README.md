🌐 Financial Institutions Mobile Banking Review Analysis
A Complete NLP, Data Engineering & Insights Pipeline for Ethiopian Banks
📌 Overview

This project analyzes customer reviews from Google Play Store to evaluate the digital banking experience of five major Ethiopian banks:

Commercial Bank of Ethiopia (CBE)

Dashen Bank

Bank of Abyssinia (BOA)

Abay Bank

Zemen Bank

The project builds a full end-to-end data pipeline, applying web scraping, text preprocessing, sentiment analysis, thematic clustering, database engineering, and strategic insights generation.

It answers the questions:

What do customers like about these mobile banking apps?

What frustrates them the most?

Which bank performs better digitally?

How can the apps be improved?

🚀 Project Pipeline

The solution is built around four major tasks:

🔹 Task 1: Data Collection & Preprocessing

Scraped reviews using the Google Play Scraper API

Cleaned text (lowercasing, noise removal, stop-word filtering)

Standardized dates

Removed duplicates and corrupted entries

Final dataset: 1,376 high-quality reviews

🔹 Task 2: Sentiment & Thematic Analysis

Used four sentiment engines:

VADER

TextBlob

ML Classifier

BERT Transformer Model

Performed:

Sentiment tagging (Positive, Neutral, Negative)

TF-IDF keyword extraction

Thematic clustering: App Reliability, Login Issues, UI/UX, Transactions, Customer Support

🔹 Task 3: PostgreSQL Database Engineering

Built a relational database: bank_reviews

Tables: banks, reviews

Inserted 11,782 enriched records (including sentiment outputs)

Performed SQL validation and quality checks

🔹 Task 4: Insights & Strategic Recommendations

Compared banks by sentiment, rating, and theme

Identified drivers and pain points

Created clear visualizations:

Sentiment distribution

Rating distribution

Word clouds

Delivered bank-specific recommendations, KPIs, and a 12-month improvement roadmap

📊 Key Insights
⭐ Top Positive Drivers

Ease of use

Smooth navigation

Fast transactions

Good UI/UX design

❗ Major Pain Points

Frequent crashes

Login/OTP failures

Transaction delays

Poor update quality

🏆 Best Performing Banks

CBE – highest stability, strong sentiment

Dashen – excellent UI/UX

⚠ Banks Needing Improvement

BOA and Zemen — high crash rates, poor sentiment profiles

🛠 Tech Stack

Languages & Tools

Python

PostgreSQL

Pandas, NumPy

NLTK, TextBlob, Scikit-Learn

BERT / Transformers

Matplotlib, Seaborn

Google Play Scraper

psycopg2

📁 Folder Structure
├── task1_data_preprocessing/
│   ├── raw/
│   ├── processed/
│
├── task2_nlp_analysis/
│   ├── sentiment_models/
│   ├── tfidf_keywords/
│
├── task3_database/
│   ├── schema/
│   ├── sql_dumps/
│
├── task4_insights_recommendations/
│   ├── visualizations/
│   ├── reports/
│
└── assets/

📈 Visualizations Included

Rating distribution

Sentiment comparison

Theme frequency chart

Positive vs. Negative word clouds



Run the dataset loader script:

python task3_database/load_reviews.py

🧠 Future Improvements

Add multilingual sentiment model (Amharic, Afaan Oromo)

Integrate Apple App Store reviews

Deploy a live dashboard (Streamlit / PowerBI)

Build automated weekly scraping pipeline