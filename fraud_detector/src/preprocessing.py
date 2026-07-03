import json
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

drop_cols = ["name_1", "name_2", "street", "post_code"]
categorical_cols = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
continuous_cols = ["amount", "population_city", "distance"]

ARTIFACTS_PATH = "./models/preprocessing_config.json"

logger.info("Loading preprocessing artifacts from %s", ARTIFACTS_PATH)
with open(ARTIFACTS_PATH) as f:
    artifacts = json.load(f)
logger.info("Preprocessing artifacts loaded successfully.")


def add_time_features(data: pd.DataFrame) -> pd.DataFrame:
    result = data.copy()
    result["transaction_time"] = pd.to_datetime(result["transaction_time"])

    dt = result["transaction_time"].dt
    result["hour"] = dt.hour
    result["year"] = dt.year
    result["month"] = dt.month
    result["day_of_month"] = dt.day
    result["day_of_week"] = dt.dayofweek

    return result.drop(columns="transaction_time")


def add_distance_features(data: pd.DataFrame) -> pd.DataFrame:
    result = data.copy()

    radius_km = 6371
    lat1 = np.radians(result["lat"])
    lon1 = np.radians(result["lon"])
    lat2 = np.radians(result["merchant_lat"])
    lon2 = np.radians(result["merchant_lon"])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    hav = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    )

    result["distance"] = radius_km * 2 * np.arctan2(np.sqrt(hav), np.sqrt(1 - hav))

    return result.drop(columns=["lat", "lon", "merchant_lat", "merchant_lon"])


def apply_category_maps(data: pd.DataFrame, category_maps: dict[str, dict]) -> pd.DataFrame:
    result = data.copy()

    for col, mapping in category_maps.items():
        result[f"{col}_cat"] = result[col].astype(str).map(mapping).fillna("cat_NAN")
        result = result.drop(columns=col)

    return result


def apply_mean_encoding(
    data: pd.DataFrame,
    mean_maps: dict[str, dict],
    global_mean: float,
) -> pd.DataFrame:
    result = data.copy()

    for col, mean_map in mean_maps.items():
        result[f"{col}_mean_enc"] = result[col].astype(str).map(mean_map).fillna(global_mean)
        result = result.drop(columns=col)

    return result


def run_preproc(input_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting preprocessing. Input shape: %s", input_df.shape)

    output_df = input_df.copy()

    output_df = output_df.drop(columns=drop_cols)
    logger.info("Dropped unused columns. Shape: %s", output_df.shape)

    output_df = add_time_features(output_df)
    logger.info("Added time features. Shape: %s", output_df.shape)

    output_df = apply_category_maps(
        output_df,
        artifacts["category_maps"],
    )
    logger.info("Applied category mappings. Shape: %s", output_df.shape)

    output_df = add_distance_features(output_df)
    logger.info("Added distance features. Shape: %s", output_df.shape)

    output_df = apply_mean_encoding(
        output_df,
        artifacts["mean_maps"],
        artifacts["global_mean"],
    )
    logger.info("Applied mean encoding. Shape: %s", output_df.shape)

    for col, value in artifacts["imputer_values"].items():
        output_df[col] = output_df[col].fillna(value)
    logger.info("Imputed continuous features.")

    for col in continuous_cols:
        output_df[f"{col}_log"] = np.log1p(output_df[col])

    output_df = output_df.drop(columns=continuous_cols)
    logger.info("Added log features and dropped raw continuous columns. Shape: %s", output_df.shape)

    output_df = output_df.reindex(
        columns=artifacts["feature_columns"],
        fill_value=0,
    )
    logger.info("Aligned feature columns. Final output shape: %s", output_df.shape)

    return output_df