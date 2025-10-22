# predictor.py  — LSTM forecaster
# Uses a 1-step LSTM to roll forecasts 12 months ahead.
# Expects artifacts saved at training time:
#   - model_path:   Keras model (e.g., ../models/lstm_next.keras)
#   - scaler_path:  joblib StandardScaler/MinMaxScaler fitted on training inputs
#   - feature_cols_path: JSON list[str] of input feature columns in exact order
#
# Required input dataframe columns (at minimum):
#   meterid, chargedate, kwh, ghc, paymoney  (+ any features you trained on like loc/res)
#
# Public API:
#   predict_next_month_lstm(...)
#   predict_next_12_months_lstm(...)

import json
from typing import List, Sequence, Optional

import joblib
import numpy as np
import pandas as pd
import tensorflow as tf
from pandas.tseries.offsets import MonthEnd


# ----------------------------
# Date helpers
# ----------------------------
def _to_month_end(ts) -> pd.Timestamp:
    ts = pd.to_datetime(ts)
    return (ts.to_period("M").to_timestamp("M")).tz_localize(None)


def _advance_month(ts, n: int = 1) -> pd.Timestamp:
    return _to_month_end(ts) + MonthEnd(n)


# ----------------------------
# Feature prep (align with training)
# ----------------------------
def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Light prep used both for training-time parity and roll-forward simulation."""
    df = df.copy()
    df["chargedate"] = pd.to_datetime(df["chargedate"])
    df = df.sort_values(["meterid", "chargedate"])

    # temporal encodings
    df["month_num"] = df["chargedate"].dt.month
    df["year"] = df["chargedate"].dt.year
    df["month_sin"] = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_num"] / 12)

    # simple categorical encodings if present
    if "loc" in df.columns:
        df["loc_enc"] = df["loc"].astype("category").cat.codes
    if "res" in df.columns:
        df["res_enc"] = df["res"].astype("category").cat.codes

    return df


# ----------------------------
# Artifact loading
# ----------------------------
def _load_lstm_artifacts(
    model_path: str,
    scaler_path: str,
    feature_cols_path: str,
):
    model = tf.keras.models.load_model(model_path)
    scaler = joblib.load(scaler_path)
    with open(feature_cols_path, "r", encoding="utf-8") as f:
        feature_cols: List[str] = json.load(f)
    return model, scaler, feature_cols


# ----------------------------
# Core step: one forward prediction for a single meter
# ----------------------------
def _predict_one_step(
    sim_frame: pd.DataFrame,
    meter_id,
    feature_cols: Sequence[str],
    scaler,
    model,
    seq_len: int,
    target_names: Sequence[str],
) -> Optional[np.ndarray]:
    """Build last seq_len window for a meter and run the LSTM once."""
    feat = _prepare_frame(sim_frame)
    if not set(feature_cols).issubset(feat.columns):
        missing = sorted(set(feature_cols) - set(feat.columns))
        raise ValueError(f"Missing required features: {missing}")

    block = feat.loc[feat["meterid"] == meter_id, feature_cols].tail(seq_len)
    if block.shape[0] < seq_len:
        return None

    X2d = block.to_numpy(dtype=float)            # (seq_len, n_feat)
    Xs = scaler.transform(X2d)                   # same scaler used at training
    X = Xs.reshape(1, seq_len, Xs.shape[1])      # (1, seq_len, n_feat)

    yhat = model.predict(X, verbose=0)           # (1, n_targets) or (1,1,n_targets)
    yhat = np.array(yhat).reshape(-1)
    if yhat.size != len(target_names):
        raise ValueError(
            f"Model output size {yhat.size} != len(target_names) {len(target_names)}"
        )
    return yhat


# ----------------------------
# Public: next-month prediction
# ----------------------------
def predict_next_month_lstm(
    df_raw: pd.DataFrame,
    as_of: str,
    model_path: str,
    scaler_path: str,
    feature_cols_path: str,
    seq_len: int,
    target_names: List[str] = ["kwh", "ghc", "paymoney"],
) -> pd.DataFrame:
    """
    One-step forecast for each meter using a 1-step LSTM model.
    Returns one row per meter with columns: meterid, as_of_month, <target>_pred
    """
    model, scaler, feature_cols = _load_lstm_artifacts(model_path, scaler_path, feature_cols_path)

    df = _prepare_frame(df_raw)
    cutoff = _to_month_end(as_of)
    df = df[df["chargedate"] <= cutoff]

    out_rows = []
    for meter_id, g in df.groupby("meterid", sort=True):
        if g.shape[0] < seq_len:
            continue

        yhat = _predict_one_step(
            sim_frame=g,
            meter_id=meter_id,
            feature_cols=feature_cols,
            scaler=scaler,
            model=model,
            seq_len=seq_len,
            target_names=target_names,
        )
        if yhat is None:
            continue

        row = {"meterid": meter_id, "as_of_month": _to_month_end(cutoff)}
        row.update({f"{t}_pred": float(v) for t, v in zip(target_names, yhat)})
        out_rows.append(row)

    if not out_rows:
        return pd.DataFrame(columns=["meterid", "as_of_month"] + [f"{t}_pred" for t in target_names])

    out = pd.DataFrame(out_rows).sort_values("meterid").reset_index(drop=True)
    return out


# ----------------------------
# Public: 12-month recursive forecast
# ----------------------------
def predict_next_12_months_lstm(
    df_raw: pd.DataFrame,
    as_of: str,
    model_path: str,
    scaler_path: str,
    feature_cols_path: str,
    seq_len: int,
    target_names: List[str] = ["kwh", "ghc", "paymoney"],
) -> pd.DataFrame:
    """
    Roll the 1-step LSTM forward for 12 months.
    Assumes the model outputs len(target_names) values for the next month.
    Appends predictions back into the simulation frame so lags/encodings advance.
    Returns rows: meterid, forecast_month, <target>_pred
    """
    model, scaler, feature_cols = _load_lstm_artifacts(model_path, scaler_path, feature_cols_path)

    df = _prepare_frame(df_raw)
    cutoff = _to_month_end(as_of)
    df = df[df["chargedate"] <= cutoff]

    outputs = []
    for meter_id, g in df.groupby("meterid", sort=True):
        sim = g.sort_values("chargedate").copy()
        if sim.shape[0] < seq_len:
            continue

        # carry forward static fields if needed
        last_loc = sim["loc"].iloc[-1] if "loc" in sim.columns else None
        last_res = sim["res"].iloc[-1] if "res" in sim.columns else None

        month_cursor = _advance_month(cutoff, 1)
        for _ in range(12):
            yhat = _predict_one_step(
                sim_frame=sim,
                meter_id=meter_id,
                feature_cols=feature_cols,
                scaler=scaler,
                model=model,
                seq_len=seq_len,
                target_names=target_names,
            )
            if yhat is None:
                break

            outputs.append(
                {
                    "meterid": meter_id,
                    "forecast_month": month_cursor,
                    **{f"{t}_pred": float(v) for t, v in zip(target_names, yhat)},
                }
            )

            # append synthetic next month so future steps can consume it
            add_row = {
                "meterid": meter_id,
                "chargedate": month_cursor,
                "kwh": np.nan,
                "ghc": np.nan,
                "paymoney": np.nan,
                "loc": last_loc,
                "res": last_res,
            }
            # if targets are also part of inputs, place predictions so they feed forward
            for t, v in zip(target_names, yhat):
                add_row[t] = float(v)

            sim = pd.concat([sim, pd.DataFrame([add_row])], ignore_index=True)
            month_cursor = _advance_month(month_cursor, 1)

    if not outputs:
        return pd.DataFrame(columns=["meterid", "forecast_month"] + [f"{t}_pred" for t in target_names])

    out = pd.DataFrame(outputs).sort_values(["meterid", "forecast_month"]).reset_index(drop=True)
    out["forecast_month"] = pd.to_datetime(out["forecast_month"])
    return out
