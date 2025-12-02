from transformers import pipeline

def bert_sentiment(df, text_col='review_text'):
    classifier = pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english"
    )

    results = df[text_col].apply(lambda x: classifier(str(x))[0])

    df['bert_label'] = results.apply(lambda x: x['label'].lower())
    df['bert_score'] = results.apply(lambda x: x['score'])

    return df
