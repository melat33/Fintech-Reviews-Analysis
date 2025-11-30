from textblob import TextBlob

def textblob_sentiment(df, text_col='review_text'):
    df['textblob_score'] = df[text_col].apply(
        lambda x: TextBlob(str(x)).sentiment.polarity
    )

    df['textblob_label'] = df['textblob_score'].apply(
        lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'neutral')
    )
    return df
