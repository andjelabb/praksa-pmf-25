from preprocessing import calculate_correlation, encode_targets, get_data_from_db, identify_columns, prepare_data_for_training, prepare_dataframe, preprocess_data
from wrapper import LightGBMWrapper


query = """SELECT * FROM agregirani_mv"""


data = get_data_from_db(query)


df = prepare_dataframe(data)


num_cols, cat_cols = identify_columns(df)


transformed_credit_df = preprocess_data(df, num_cols, cat_cols)



y1 = encode_targets(df["pozajmica"])

selected_features1 = ['kreditna_kartica', 'trajni_nalog_broj', 'trajni_nalog_iznos']  

correlation = calculate_correlation(df, selected_features1)

target_column1 = 'pozajmica' 

X_train, X_test, y_train, y_test = prepare_data_for_training(df, target_column1, selected_features1)

print(f"Trening set: X_train: {X_train.shape}, y_train: {y_train.shape}")
print(f"Test set: X_test: {X_test.shape}, y_test: {y_test.shape}")



#y2 = encode_targets(df["kredit"])

#selected_features2 = ['kreditna_kartica', 'trajni_nalog_broj', 'trajni_nalog_iznos']  

#correlation = calculate_correlation(df, selected_features2)

#target_column2 = 'kredit' 

#X_train, X_test, y_train, y_test = prepare_data_for_training(df, target_column2, selected_features2)

#print(f"Trening set: X_train: {X_train.shape}, y_train: {y_train.shape}")
#print(f"Test set: X_test: {X_test.shape}, y_test: {y_test.shape}")



#y3 = encode_targets(df["elektronsko_bankarstvo"])

#selected_features3 = ['nalozi_salter_broj', 'nalozi_salter_iznos', 'nalozi_devizni_salter_broj', 'nalozi_devizni_salter_iznos', 'nalozi_smartatm_broj', 'nalozi_smartatm_iznos', 'trajni_nalog_broj', 'trajni_nalog_iznos', 'placanje_kreditna_kartica_web_broj', 'placanje_kreditna_kartica_web_iznos']  

#correlation = calculate_correlation(df, selected_features3)

#target_column3 = 'elektronsko_bankarstvo' 

#X_train, X_test, y_train, y_test = prepare_data_for_training(df, target_column3, selected_features3)

#print(f"Trening set: X_train: {X_train.shape}, y_train: {y_train.shape}")
#print(f"Test set: X_test: {X_test.shape}, y_test: {y_test.shape}")



model_params = {
    "num_leaves": 8,
    "max_depth": 4,
    "random_state": 42,
    "n_jobs": 12
}


wrapper = LightGBMWrapper(model_params)


wrapper.train(X_train, y_train)


predictions = wrapper.predict(X_test)


wrapper.save_model("lightgbm_model.pkl", "models_bucket")


wrapper.load_model("lightgbm_model.pkl", "models_bucket")
