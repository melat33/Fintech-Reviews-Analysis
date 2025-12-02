from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import joblib

def train_ml_model(df, text_col='review_text', label_col='rating'):
    df['ml_label'] = df[label_col].apply(
        lambda x: 'positive' if x >= 4 else ('negative' if x <= 2 else 'neutral')
    )

    vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
    X = vectorizer.fit_transform(df[text_col])
    y = df['ml_label']

    model = LogisticRegression(max_iter=1000)
    model.fit(X, y)

    joblib.dump((model, vectorizer), 'ml_sentiment_model.pkl')
    return model, vectorizer


def predict_ml_sentiment(df, model, vectorizer, text_col='review_text'):
    X = vectorizer.transform(df[text_col])
    df['ml_label_pred'] = model.predict(X)
    return df
