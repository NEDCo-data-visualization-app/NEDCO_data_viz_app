import os
import joblib
import numpy as np
import pandas as pd

# ---------------------------------
# Feature preparation
# ---------------------------------
def _prepare_features(df: pd.DataFrame, as_of: str) -> pd.DataFrame:
    df = df.copy()
    df["chargedate"] = pd.to_datetime(df["chargedate"])
    df = df[df["chargedate"] <= pd.to_datetime(as_of)].sort_values(["meterid", "chargedate"])

    # basic temporal features
    df["month_num"] = df["chargedate"].dt.month
    df["year"] = df["chargedate"].dt.year
    df["month_sin"] = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_num"] / 12)

    # categorical encodings
    df["loc_enc"] = df["loc"].astype("category").cat.codes
    df["res_enc"] = df["res"].astype("category").cat.codes

    # lag features
    for lag in [1, 2, 3]:
        for col in ["kwh", "ghc", "paymoney"]:
            df[f"{col}_lag{lag}"] = df.groupby("meterid")[col].shift(lag)

    # rolling means and stds
    for col in ["kwh", "ghc", "paymoney"]:
        df[f"{col}_roll3_mean"] = df.groupby("meterid")[col].transform(lambda x: x.rolling(3, min_periods=2).mean())
        df[f"{col}_roll3_std"] = df.groupby("meterid")[col].transform(lambda x: x.rolling(3, min_periods=2).std())
        df[f"{col}_roll6_mean"] = df.groupby("meterid")[col].transform(lambda x: x.rolling(6, min_periods=3).mean())
        df[f"{col}_roll6_std"] = df.groupby("meterid")[col].transform(lambda x: x.rolling(6, min_periods=3).std())

    # aggregate meter stats
    prof = df.groupby("meterid")[["kwh", "ghc", "paymoney"]].agg(["mean", "std", "count"])
    prof.columns = ["_".join(c) for c in prof.columns]
    df = df.merge(prof, on="meterid", how="left")

    # deltas and ratios
    for col in ["kwh", "ghc", "paymoney"]:
        df[f"{col}_delta1"] = df[col] - df[f"{col}_lag1"]
        df[f"{col}_delta2"] = df[f"{col}_lag1"] - df[f"{col}_lag2"]

    df["ghc_per_kwh"] = df["ghc"] / df["kwh"].replace(0, np.nan)
    df["paymoney_ratio"] = df["paymoney"] / df["ghc"].replace(0, np.nan)

    df = df.dropna(subset=["kwh_lag3", "ghc_lag3", "paymoney_lag3"])
    return df


# ---------------------------------
# Model loading
# ---------------------------------
def _load_models(models_dir: str) -> dict:
    paths = {
        "kwh_next": os.path.join(models_dir, "lgbm_final_kwh_next.pkl"),
        "ghc_next": os.path.join(models_dir, "lgbm_final_ghc_next.pkl"),
        "paymoney_next": os.path.join(models_dir, "lgbm_final_paymoney_next.pkl"),
    }
    return {t: joblib.load(p) for t, p in paths.items()}


# ---------------------------------
# Main prediction function
# ---------------------------------
def predict_next_month(df_raw: pd.DataFrame, models_dir: str, as_of: str) -> pd.DataFrame:
    """
    Predict next-month [kwh, ghc, paymoney] for each meterid using data up to `as_of`.
    Returns one row per meterid with *_next_pred columns.
    """
    df = _prepare_features(df_raw, as_of)
    last_idx = df.groupby("meterid")["chargedate"].idxmax()

    features = [c for c in df.columns if c not in ["meterid", "chargedate", "loc", "res"]]
    X = df.loc[last_idx, features].copy()
    meta = df.loc[last_idx, ["meterid", "chargedate"]].rename(columns={"chargedate": "as_of_month"}).reset_index(drop=True)

    models = _load_models(models_dir)
    out = meta.copy()

    for target, model in models.items():
        out[f"{target}_pred"] = model.predict(X)

    return out.sort_values("meterid").reset_index(drop=True)
