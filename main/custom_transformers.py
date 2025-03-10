import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class AgeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, age_column):
        self.age_column = age_column

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        age_bins = [0, 18, 25, 35, 45, 55, 65, 70, 100]

        X[self.age_column] = pd.cut(X[self.age_column], bins=age_bins, labels=range(len(age_bins) - 1))
        return X