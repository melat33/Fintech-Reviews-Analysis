def ensemble_sentiment(df):
    def vote(row):
        labels = [
            row['vader_label'],
            row['textblob_label'],
            row.get('ml_label_pred'),
            row.get('bert_label')
        ]
        labels = [l for l in labels if l]
        return max(set(labels), key=labels.count)

    df['ensemble_label'] = df.apply(vote, axis=1)
    return df
