import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def vader_sentiment(df, text_col='review_text'):
    analyzer = SentimentIntensityAnalyzer()

    df['vader_score'] = df[text_col].apply(
        lambda x: analyzer.polarity_scores(str(x))['compound']
    )

    df['vader_label'] = df['vader_score'].apply(
        lambda x: 'positive' if x > 0.05 else ('negative' if x < -0.05 else 'neutral')
    )
    return df
