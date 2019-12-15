# small demo

from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer

corpus = [
    'This is the first document.',
    'This is the second document.',
    'This document is the second document.',
    'And this is the third one.',
    'Is this the first document?',
]

vectorizer = TfidfVectorizer()
x = vectorizer.fit_transform(corpus)
a = x.toarray()
cos = cosine_similarity(a[0].reshape(1, -1), a[1].reshape(1, -1))
print(cos)
print(cos.shape)
