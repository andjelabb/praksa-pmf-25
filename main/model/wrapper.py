import sklearn
import lightgbm as lgb
from utils.minio_utils.minio_utils import get_minio_client, load_pkl_data_from_minio, save_pkl_data_to_minio

class LightGBMWrapper:
    def __init__(self, model_params=None):
        self.model = lgb.LGBMClassifier(
            num_leaves=model_params.get("num_leaves", 8),
            max_depth=model_params.get("max_depth", 4),
            random_state=model_params.get("random_state", 42),
            n_jobs=model_params.get("n_jobs", 12)
        )

    def train(self, X_train, y_train):
        print("Treniranje modela...")
        self.model.fit(X_train, y_train)
        print("Model je uspješno treniran!")

    def predict(self, X):
        return self.model.predict(X)

    def save_model(self, file_name, bucket_name):
        print(f"Čuvanje modela u MinIO: {file_name}")
        minio_client = get_minio_client()
        save_pkl_data_to_minio(minio_client, file_name, self.model, bucket_name)
        print(f"Model je sačuvan kao {file_name} u MinIO bucketu {bucket_name}.")

    def load_model(self, file_name, bucket_name):
        print(f"Učitavanje modela iz MinIO: {file_name}")
        minio_client = get_minio_client()
        self.model = load_pkl_data_from_minio(minio_client, file_name, bucket_name)
        print(f"Model je uspješno učitan sa {file_name} iz MinIO bucket-a {bucket_name}.")

