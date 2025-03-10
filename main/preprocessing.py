from matplotlib import pyplot as plt
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.calibration import LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.discriminant_analysis import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from custom_transformers import AgeTransformer
from rw_helpers.rw_helper import get_connection, execute_query
import pandas as pd
import seaborn as sns

def get_data_from_db(query):
    
    conn = get_connection()  
    print(conn)

    res, data = execute_query(conn, query)
    print("Data:", data)
    
    return data

def prepare_dataframe(data_list):

    if data_list:
        df = pd.DataFrame(data_list, columns=[
            "jedinstveni_identifikator", "datum_registracije", "starost", "pol", 
            "bracno_stanje", "poslednja_posjeta", "pozajmica", "kredit", 
            "elektronsko_bankarstvo", "kreditna_kartica", "isplate_i_uplate_salter_broj", 
            "isplate_i_uplate_salter_iznos", "isplate_i_uplate_bankomat_broj", 
            "isplate_i_uplate_bankomat_iznos", "nalozi_salter_broj", "nalozi_salter_iznos", 
            "nalozi_devizni_salter_broj", "nalozi_devizni_salter_iznos", 
            "nalozi_ebankarstvo_broj", "nalozi_ebankarstvo_iznos", 
            "nalozi_smartatm_broj", "nalozi_smartatm_iznos", "trajni_nalog_broj", 
            "trajni_nalog_iznos", "placanje_i_isplata_kreditna_kartica_broj", 
            "placanje_i_isplata_kreditna_kartica_iznos", "placanje_kreditna_kartica_web_broj", 
            "placanje_kreditna_kartica_web_iznos"
        ])
        print(df.head())
        print(df.info())
    else:
        print("Nema podataka!")
    
    return df

def identify_columns(df):
   
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
    
    print(f"Numeričke kolone:\n{num_cols}")
    print(f"Kategorijske kolone:\n{cat_cols}")
    
    return num_cols, cat_cols

def preprocess_data(df, num_cols, cat_cols, age_column="starost"):
    
    num_cols_without_age = num_cols.copy()
    if age_column in num_cols_without_age:
        num_cols_without_age.remove(age_column)
    
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_cols_without_age),
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1), cat_cols),
        ],
        remainder="passthrough",
        verbose_feature_names_out=False,
    ).set_output(transform="pandas")
    
    pipeline = Pipeline(
        [
            ("preprocessor", preprocessor),
            ("age", AgeTransformer(age_column=age_column)),
        ]
    )
    
    transformed_df = pipeline.fit_transform(df)
    
    return transformed_df

def encode_targets(df_target):
    
    label_encoder = LabelEncoder()
    
    y_encoded = label_encoder.fit_transform(df_target)
    y = pd.Series(y_encoded, name="target")
    
    return y

def calculate_correlation(df, selected_features):

    correlation = df[selected_features].corr()
    print("Korelacija:")
    print(correlation)
    
    return correlation

from sklearn.model_selection import train_test_split

def prepare_data_for_training(df, target_column, selected_features, test_size=0.35, random_state=42):
    
    X = df[selected_features]
    y = df[target_column]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    return X_train, X_test, y_train, y_test

