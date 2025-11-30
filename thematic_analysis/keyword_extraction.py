import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

def extract_keywords(df, text_col='review_text', top_n=10):
    vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
    X = vectorizer.fit_transform(df[text_col])
    feature_array = vectorizer.get_feature_names_out()
    tfidf_sorting = X.toarray().sum(axis=0)
    top_keywords = [feature_array[i] for i in tfidf_sorting.argsort()[-top_n:][::-1]]
    return top_keywords
