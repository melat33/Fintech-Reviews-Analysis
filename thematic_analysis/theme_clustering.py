from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

def cluster_themes(df, text_col='review_text', n_clusters=5):
    vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
    X = vectorizer.fit_transform(df[text_col])
    km = KMeans(n_clusters=n_clusters, random_state=42)
    km.fit(X)
    df['theme_cluster'] = km.labels_
    return df
