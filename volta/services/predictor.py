# # volta/services/predictor.py
# from __future__ import annotations
# import json
# from pathlib import Path
# from typing import Iterable, Optional, Tuple
#
# import duckdb
# import joblib
# import numpy as np
# import pandas as pd
# import tensorflow as tf
# from tensorflow import keras
#
# # ---- defaults (relative to project root) ----
# DB_PATH       = Path("data/warehouse.duckdb")
# TABLE         = "electricity.billing_records"
# MODEL_PATH    = Path("models/lstm_next.keras")
# SCALER_PATH   = Path("models/scaler.pkl")          # must contain tuple (sx, sy)
# FEATURES_PATH = Path("models/feature_cols.json")
#
# SEQ_LEN  = 12
# HORIZON  = 12
# TARGETS  = ["kwh", "ghc", "paymoney"]
#
# # ========= custom objects (match your training notebook) =========
# def _horizon_weights(H, decay=0.15):
#     w = tf.exp(-decay * tf.cast(tf.range(H), tf.float32))
#     return w / tf.reduce_sum(w)
#
# _TW = {"kwh": 2.0, "ghc": 1.0, "paymoney": 1.0}
# def _target_weights(names):  # names == TARGETS
#     return tf.constant([_TW.get(t, 1.0) for t in names], dtype=tf.float32)
#
# def _make_weighted_huber(target_names, delta=1.0, decay=0.15):
#     tw = _target_weights(target_names)
#     def loss(y_true, y_pred):
#         H = tf.shape(y_true)[1]
#         hw = _horizon_weights(H, decay)[tf.newaxis, :, tf.newaxis]
#         err = tf.abs(y_true - y_pred)
#         hub = tf.where(err < delta, 0.5 * tf.square(err), delta * (err - 0.5 * delta))
#         return tf.reduce_sum(hub * hw * tw[tf.newaxis, tf.newaxis, :])
#     loss.__name__ = "weighted_huber_ht"
#     return loss
#
# def _weighted_rmse(decay=0.15):
#     def fn(y_true, y_pred):
#         hw = _horizon_weights(tf.shape(y_true)[1], decay)[tf.newaxis, :, tf.newaxis]
#         return tf.sqrt(tf.reduce_sum(tf.square(y_pred - y_true) * hw))
#     fn.__name__ = f"wRMSE_d{str(decay).replace('.','p')}"
#     return fn
#
# def _weighted_smape(decay=0.15, eps=1e-6):
#     def fn(y_true, y_pred):
#         hw  = _horizon_weights(tf.shape(y_true)[1], decay)[tf.newaxis, :, tf.newaxis]
#         num = tf.abs(y_pred - y_true)
#         den = tf.abs(y_true) + tf.abs(y_pred) + eps
#         return 200.0 * tf.reduce_sum((num / den) * hw)
#     fn.__name__ = f"wSMAPE_d{str(decay).replace('.','p')}"
#     return fn
#
# def _mae_h1(y_true, y_pred):
#     return tf.reduce_mean(tf.abs(y_true[:, 0, :] - y_pred[:, 0, :]))
#
# _CUSTOM_OBJECTS = {
#     "weighted_huber_ht": _make_weighted_huber(TARGETS, 1.0, 0.15),
#     _weighted_rmse(0.15).__name__:  _weighted_rmse(0.15),
#     _weighted_smape(0.15).__name__: _weighted_smape(0.15),
#     "mae_h1": _mae_h1,
# }
#
# # ========= feature engineering (same as training) =========
# def _add_features(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.sort_values(["meterid", "chargedate"]).copy()
#     # time
#     df["month_num"]  = df["chargedate"].dt.month
#     df["year"]       = df["chargedate"].dt.year
#     df["month_sin"]  = np.sin(2 * np.pi * df["month_num"] / 12)
#     df["month_cos"]  = np.cos(2 * np.pi * df["month_num"] / 12)
#     df["weekday"]    = df["chargedate"].dt.weekday
#     df["is_weekend"] = (df["weekday"] >= 5).astype(int)
#     df["season"]     = pd.cut(df["month_num"], [0, 3, 6, 9, 12], labels=[0, 1, 2, 3]).astype(int)
#     # cats
#     df["loc_enc"] = df["loc"].astype("category").cat.codes
#     df["res_enc"] = df["res"].astype("category").cat.codes
#
#     def per_meter(g: pd.DataFrame) -> pd.DataFrame:
#         g = g.copy()
#         for c in TARGETS:
#             g[f"{c}_lag1"]  = g[c].shift(1)
#             g[f"{c}_lag2"]  = g[c].shift(2)
#             g[f"{c}_lag3"]  = g[c].shift(3)
#             g[f"{c}_ma3"]   = g[c].shift(1).rolling(3,  min_periods=3).mean()
#             g[f"{c}_ma6"]   = g[c].shift(1).rolling(6,  min_periods=6).mean()
#             g[f"{c}_ma12"]  = g[c].shift(1).rolling(12, min_periods=12).mean()
#             g[f"{c}_diff1"] = g[c].diff(1)
#             g[f"{c}_pc1"]   = g[c].pct_change(1)  # alias name used in feature_cols.json
#         return g
#
#     df = df.groupby("meterid", group_keys=False).apply(per_meter)
#     df = df.groupby("meterid", group_keys=False).apply(lambda g: g.iloc[12:]).reset_index(drop=True)
#     return df
#
# # ========= data access =========
# def _stream_recent_history(db_path: Path,
#                            table: str,
#                            cutoff: pd.Timestamp,
#                            rows_per_meter: int = 27,
#                            meters: Optional[Iterable[int]] = None):
#     where_extra = ""
#     params = [pd.to_datetime(cutoff), rows_per_meter]
#     if meters:
#         where_extra = " AND meterid IN (" + ",".join(["?"] * len(meters)) + ") "
#         params = [pd.to_datetime(cutoff), rows_per_meter, *list(meters)]
#
#     sql = f"""
#     WITH base AS (
#       SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#       FROM {table}
#       WHERE chargedate <= ? {where_extra}
#         AND kwh >= 0 AND ghc >= 0 AND paymoney >= 0
#     ),
#     d AS (
#       SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid, chargedate ORDER BY chargedate) rn
#       FROM base
#     ),
#     k AS (
#       SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#       FROM d WHERE rn = 1
#     )
#     SELECT *
#     FROM k
#     QUALIFY ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY chargedate DESC) <= ?
#     ORDER BY meterid, chargedate
#     """
#     con = duckdb.connect(str(db_path))
#     reader = con.execute(sql, params).fetch_record_batch()
#     for rb in reader:  # Arrow RecordBatches
#         yield rb.to_pandas()
#     con.close()
#
# # ========= predictor =========
# class Predictor:
#     def __init__(self,
#                  db_path: Path = DB_PATH,
#                  table: str = TABLE,
#                  model_path: Path = MODEL_PATH,
#                  scaler_path: Path = SCALER_PATH,
#                  features_path: Path = FEATURES_PATH):
#         self.db_path = Path(db_path)
#         self.table = table
#         self.feature_cols = json.loads(Path(features_path).read_text())
#         self.sx, self.sy = joblib.load(scaler_path)  # tuple (sx, sy)
#         self.model = keras.models.load_model(model_path, custom_objects=_CUSTOM_OBJECTS)
#
#     @staticmethod
#     def _to_month_end(ts) -> pd.Timestamp:
#         ts = pd.to_datetime(ts)
#         return ts.to_period("M").to_timestamp("M")
#
#     @staticmethod
#     def _advance_month(ts, n=1) -> pd.Timestamp:
#         return Predictor._to_month_end(ts) + pd.offsets.MonthEnd(n)
#
#     def _batch_to_last_windows(
#         self, df_batch: pd.DataFrame, cutoff: pd.Timestamp
#     ) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
#         X_list, meters = [], []
#         for mid, g in df_batch.groupby("meterid", sort=False):
#             g = g[g["chargedate"] <= cutoff]
#             if len(g) < SEQ_LEN:
#                 continue
#             g = _add_features(g)
#
#             # ensure all required columns exist
#             missing = [c for c in self.feature_cols if c not in g.columns]
#             if missing:
#                 # skip meters that lack history-derived columns
#                 continue
#
#             X_last = g[self.feature_cols].to_numpy(dtype=float)[-SEQ_LEN:]
#             X_list.append(X_last)
#             meters.append(mid)
#
#         if not X_list:
#             return None, None
#         return np.stack(X_list, axis=0), np.array(meters)
#
#     def forecast(
#         self,
#         as_of: str,
#         rows_per_meter: int = 27,
#         meters: Optional[Iterable[int]] = None,
#         batch_rows: int = 400_000,
#     ) -> pd.DataFrame:
#         """Forecast next 12 months per meter as of `as_of`."""
#         cutoff = self._to_month_end(as_of)
#         outputs = []
#
#         for df_batch in _stream_recent_history(self.db_path, self.table, cutoff, rows_per_meter, meters):
#             df_batch["chargedate"] = pd.to_datetime(df_batch["chargedate"])
#             X_last, meter_ids = self._batch_to_last_windows(df_batch, cutoff)
#             if X_last is None:
#                 continue
#
#             # scale inputs and predict
#             Xs = self.sx.transform(X_last.reshape(-1, X_last.shape[2])).reshape(X_last.shape)
#             preds = self.model.predict(Xs, batch_size=256, verbose=0)  # (M, 12, 3)
#             M, H, T = preds.shape
#
#             # inverse scale outputs
#             preds_inv = self.sy.inverse_transform(preds.reshape(-1, T)).reshape(M, H, T)
#
#             rows = []
#             for i, mid in enumerate(meter_ids):
#                 for h in range(1, H + 1):
#                     rows.append([
#                         int(mid),
#                         self._advance_month(cutoff, h),
#                         h,
#                         *preds_inv[i, h - 1, :].tolist()
#                     ])
#             outputs.append(pd.DataFrame(rows, columns=[
#                 "meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"
#             ]))
#
#         if not outputs:
#             return pd.DataFrame(columns=["meterid","forecast_date","horizon","kwh","ghc","paymoney"])
#         out = pd.concat(outputs, ignore_index=True)
#         return out.sort_values(["meterid", "forecast_date"]).reset_index(drop=True)


# Minimal overwrite: class with a single method to fetch data from DuckDB into a pandas DataFrame.

# predictor.py — LightGBM loader + data fetch + vectorized feature builder (no TensorFlow)

from __future__ import annotations
from pathlib import Path
from typing import Iterable, Optional, Dict, Any
import duckdb
import joblib
import numpy as np
import pandas as pd

from pandas import DataFrame

def _advance_as_of(as_of: str) -> pd.Timestamp:
    return _to_month_end(as_of) + pd.offsets.MonthEnd(1)

# paths: file is at volta/services/, data/ and models/ are two levels up
DB_PATH       = Path("../../data/warehouse.duckdb")
TABLE         = "electricity.billing_records"
LGBM_PATH     = Path("../../models/lgbm_bundle.pkl")       # {"models": {"kwh":..., "ghc":..., "paymoney":...}} or {"model": ...}
FEATURES_PATH = Path("../../models/feature_cols.json")     # optional; falls back to FEATURE_COLS_TRAINED

# locked feature list used in training (order matters, 47 cols)
FEATURE_COLS_TRAINED = [
    "month_num","year","month_sin","month_cos",
    "loc_enc","res_enc",
    "kwh","ghc","paymoney",
    "kwh_lag1","kwh_lag2","kwh_lag3",
    "ghc_lag1","ghc_lag2","ghc_lag3",
    "paymoney_lag1","paymoney_lag2","paymoney_lag3",
    "kwh_roll3_mean","kwh_roll3_std","kwh_roll6_mean","kwh_roll6_std",
    "ghc_roll3_mean","ghc_roll3_std","ghc_roll6_mean","ghc_roll6_std",
    "paymoney_roll3_mean","paymoney_roll3_std","paymoney_roll6_mean","paymoney_roll6_std",
    "kwh_mean","kwh_std","kwh_count","ghc_mean","ghc_std","ghc_count",
    "paymoney_mean","paymoney_std","paymoney_count",
    "kwh_delta1","kwh_delta2","ghc_delta1","ghc_delta2","paymoney_delta1","paymoney_delta2",
    "ghc_per_kwh","paymoney_ratio"
]
_PUT_INDEX = {n: i for i, n in enumerate(FEATURE_COLS_TRAINED)}

def _to_month_end(ts) -> pd.Timestamp:
    ts = pd.to_datetime(ts)
    return ts.to_period("M").to_timestamp("M")

def _month_features_for(dt: pd.Timestamp) -> dict:
    m = dt.month
    return {
        "month_num": m,
        "year": dt.year,
        "month_sin": float(np.sin(2*np.pi*m/12)),
        "month_cos": float(np.cos(2*np.pi*m/12)),
    }

class PredictorLGBM:
    def __init__(self,
                 db_path: Path = DB_PATH,
                 table: str = TABLE,
                 bundle_path: Path = LGBM_PATH,
                 features_path: Path = FEATURES_PATH):
        # DB
        self.db_path = Path(db_path)
        self.table = table

        # features: prefer saved list, else locked list
        try:
            if Path(features_path).exists():
                import json
                self.feature_cols = json.loads(Path(features_path).read_text())
            else:
                self.feature_cols = FEATURE_COLS_TRAINED
        except Exception:
            self.feature_cols = FEATURE_COLS_TRAINED

        # models: bundle supports dict of per-target models or single multioutput
        bundle: Dict[str, Any] = joblib.load(bundle_path)
        self.models: Optional[Dict[str, Any]] = bundle.get("models")
        self.model = bundle.get("model")

        # required names from boosters; fallback to feature list
        self.req_names: Dict[str, list[str]] = {}
        if self.models is not None:
            for t, m in self.models.items():
                names = m.booster_.feature_name() if hasattr(m, "booster_") else None
                self.req_names[t] = list(names) if names else list(self.feature_cols)
        else:
            names = self.model.booster_.feature_name() if hasattr(self.model, "booster_") else None
            self.req_names["all"] = list(names) if names else list(self.feature_cols)

        # precompute indices from our template order into each model’s expected order
        def _build_idx(req: list[str]) -> np.ndarray:
            idx = np.fromiter((_PUT_INDEX.get(n, -1) for n in req), dtype=np.int64)
            if np.any(idx < 0):
                missing = [req[i] for i in np.where(idx < 0)[0]]
                raise RuntimeError(f"Missing feature(s) in template: {missing}")
            return idx

        self.req_idx: Dict[str, np.ndarray] = {}
        if self.models is not None:
            for t, names in self.req_names.items():
                self.req_idx[t] = _build_idx(names)
        else:
            self.req_idx["all"] = _build_idx(self.req_names["all"])

    # ---------- 1) fetch data to pandas ----------
    def load_snapshot(self,
                      as_of: str,
                      rows_per_meter: int = 27,
                      meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
        """
        Fetch recent history up to `as_of` from DuckDB as pandas DataFrame.
        Returns at most `rows_per_meter` most-recent rows per meter.
        """
        cutoff = _to_month_end(as_of)
        where_extra = ""
        params = [pd.to_datetime(cutoff), rows_per_meter]

        if meters:
            meters = list(meters)
            placeholders = ",".join(["?"] * len(meters))
            where_extra = f" AND meterid IN ({placeholders}) "
            params = [pd.to_datetime(cutoff), rows_per_meter, *meters]

        sql = f"""
        WITH base AS (
          SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
          FROM {self.table}
          WHERE chargedate <= ? {where_extra}
            AND kwh >= 0 AND ghc >= 0 AND paymoney >= 0
        ),
        d AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid, chargedate ORDER BY chargedate) rn
          FROM base
        ),
        k AS (
          SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
          FROM d WHERE rn = 1
        )
        SELECT *
        FROM k
        QUALIFY ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY chargedate DESC) <= ?
        ORDER BY meterid, chargedate
        """

        con = duckdb.connect(str(self.db_path))
        df = con.execute(sql, params).df()
        con.close()

        if df.empty:
            return df

        df["chargedate"] = pd.to_datetime(df["chargedate"]).dt.to_period("M").dt.to_timestamp("M")
        return df.sort_values(["meterid", "chargedate"]).reset_index(drop=True)

    # ---------- 2) vectorized transformation to model-ready one-step features ----------
    def transform_snapshot(self, df: pd.DataFrame, as_of: str) -> pd.DataFrame:
        """
        Build the exact FEATURE_COLS_TRAINED to predict the NEXT month
        for every meter present in `df` (history up to cutoff). One row per meter.
        """
        if df.empty:
            return pd.DataFrame(columns=["meterid", *FEATURE_COLS_TRAINED])

        cutoff = _to_month_end(as_of)
        target_dt = cutoff + pd.offsets.MonthEnd(1)

        gdf = df.sort_values(["meterid", "chargedate"]).copy()

        # last observed per meter
        last = gdf.groupby("meterid", as_index=False).tail(1)[
            ["meterid","chargedate","loc","res","kwh","ghc","paymoney"]
        ].rename(columns={"chargedate": "chargedate_last"})

        # lags at cutoff
        for col in ["kwh","ghc","paymoney"]:
            gdf[f"{col}_lag1"] = gdf.groupby("meterid")[col].shift(1)
            gdf[f"{col}_lag2"] = gdf.groupby("meterid")[col].shift(2)
            gdf[f"{col}_lag3"] = gdf.groupby("meterid")[col].shift(3)

        lags = gdf.groupby("meterid", as_index=False).tail(1)[
            ["meterid",
             "kwh_lag1","kwh_lag2","kwh_lag3",
             "ghc_lag1","ghc_lag2","ghc_lag3",
             "paymoney_lag1","paymoney_lag2","paymoney_lag3"]
        ]

        # rolling windows (3, 6) mean/std at cutoff
        roll3_mean = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(3, min_periods=3).mean().reset_index(level=0, drop=True)
        roll3_std  = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(3, min_periods=3).std().reset_index(level=0, drop=True)
        roll6_mean = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(6, min_periods=3).mean().reset_index(level=0, drop=True)
        roll6_std  = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(6, min_periods=3).std().reset_index(level=0, drop=True)

        gdf[["kwh_roll3_mean","ghc_roll3_mean","paymoney_roll3_mean"]] = roll3_mean.values
        gdf[["kwh_roll3_std","ghc_roll3_std","paymoney_roll3_std"]]    = roll3_std.values
        gdf[["kwh_roll6_mean","ghc_roll6_mean","paymoney_roll6_mean"]] = roll6_mean.values
        gdf[["kwh_roll6_std","ghc_roll6_std","paymoney_roll6_std"]]    = roll6_std.values

        rolls = gdf.groupby("meterid", as_index=False).tail(1)[
            ["meterid",
             "kwh_roll3_mean","kwh_roll3_std","kwh_roll6_mean","kwh_roll6_std",
             "ghc_roll3_mean","ghc_roll3_std","ghc_roll6_mean","ghc_roll6_std",
             "paymoney_roll3_mean","paymoney_roll3_std","paymoney_roll6_mean","paymoney_roll6_std"]
        ]

        # global stats per meter (mean/std/count) up to cutoff
        agg = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].agg(["mean","std","count"])
        agg.columns = [
            "kwh_mean","kwh_std","kwh_count",
            "ghc_mean","ghc_std","ghc_count",
            "paymoney_mean","paymoney_std","paymoney_count"
        ]
        stats = agg.reset_index()

        # deltas at cutoff
        for col in ["kwh","ghc","paymoney"]:
            gdf[f"{col}_delta1"] = gdf.groupby("meterid")[col].diff(1)
            gdf[f"{col}_delta2"] = gdf.groupby("meterid")[col].diff(2)
        deltas = gdf.groupby("meterid", as_index=False).tail(1)[
            ["meterid","kwh_delta1","kwh_delta2","ghc_delta1","ghc_delta2","paymoney_delta1","paymoney_delta2"]
        ]

        # categorical encodings at cutoff
        enc = last[["meterid","loc","res"]].copy()
        enc["loc_enc"] = enc["loc"].astype("category").cat.codes
        enc["res_enc"] = enc["res"].astype("category").cat.codes
        enc = enc[["meterid","loc_enc","res_enc"]]

        # ratios from last observed
        ratios = last[["meterid","kwh","ghc","paymoney"]].copy()
        ratios["ghc_per_kwh"]    = np.where((ratios["kwh"] != 0) & ratios["kwh"].notna(), ratios["ghc"]/ratios["kwh"], 0.0)
        ratios["paymoney_ratio"] = np.where(ratios["ghc"].notna(), ratios["paymoney"]/(ratios["ghc"] + 1e-9), 0.0)
        ratios = ratios[["meterid","ghc_per_kwh","paymoney_ratio"]]

        # assemble one row per meter
        base = last[["meterid","kwh","ghc","paymoney"]].copy()
        feat = (base.merge(lags, on="meterid", how="left")
                    .merge(rolls, on="meterid", how="left")
                    .merge(stats, on="meterid", how="left")
                    .merge(deltas, on="meterid", how="left")
                    .merge(enc, on="meterid", how="left")
                    .merge(ratios, on="meterid", how="left"))

        # time features for the target month (cutoff + 1M)
        tfeat = pd.DataFrame([_month_features_for(target_dt)]).assign(key=1)
        meters = feat[["meterid"]].assign(key=1)
        feat = meters.merge(tfeat, on="key", how="left").drop(columns="key").merge(feat, on="meterid", how="left")

        # order and fill
        for c in FEATURE_COLS_TRAINED:
            if c not in feat.columns:
                feat[c] = 0.0
        feat = feat[["meterid", *FEATURE_COLS_TRAINED]].copy()
        feat[FEATURE_COLS_TRAINED] = feat[FEATURE_COLS_TRAINED].astype(float).fillna(0.0)

        return feat

    # --- add inside PredictorLGBM in predictor.py ---

    def predict_next(self,
                     as_of: str,
                     rows_per_meter: int = 24,
                     meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
        """
        One-step forecast for all selected meters (next month only).
        Returns columns: meterid, forecast_date, horizon, kwh, ghc, paymoney
        """
        # build features
        snap = self.load_snapshot(as_of=as_of, rows_per_meter=rows_per_meter, meters=meters)
        if snap.empty:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])
        feat = self.transform_snapshot(snap, as_of)
        if feat.empty:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

        vecs = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

        # predict
        if self.models is not None:
            Xk = vecs[:, self.req_idx["kwh"]]
            Xg = vecs[:, self.req_idx["ghc"]]
            Xp = vecs[:, self.req_idx["paymoney"]]
            kwh_hat = self.models["kwh"].predict(Xk, predict_disable_shape_check=True).reshape(-1)
            ghc_hat = self.models["ghc"].predict(Xg, predict_disable_shape_check=True).reshape(-1)
            pay_hat = self.models["paymoney"].predict(Xp, predict_disable_shape_check=True).reshape(-1)
        else:
            Xa = vecs[:, self.req_idx["all"]]
            arr = np.asarray(self.model.predict(Xa, predict_disable_shape_check=True))
            if arr.ndim == 1:
                arr = arr.reshape(-1, 3)
            kwh_hat, ghc_hat, pay_hat = arr[:, 0], arr[:, 1], arr[:, 2]

        target_dt = _to_month_end(as_of) + pd.offsets.MonthEnd(1)
        return pd.DataFrame({
            "meterid": feat["meterid"].astype(int).values,
            "forecast_date": target_dt,
            "horizon": 1,
            "kwh": kwh_hat,
            "ghc": ghc_hat,
            "paymoney": pay_hat,
        })

    def append_predictions(self, snapshot: DataFrame, preds: DataFrame) -> DataFrame:
        """
        Append one-step predictions as the next month's observed rows,
        so the frame is ready for building features for the following step.
        Keeps columns: meterid, chargedate, loc, res, kwh, ghc, paymoney.
        """
        if snapshot.empty or preds.empty:
            return snapshot.copy()

        base_cols = ["meterid","chargedate","loc","res","kwh","ghc","paymoney"]
        snap = snapshot[base_cols].copy()

        # get latest loc/res per meter to carry forward
        last_lr = (
            snap.sort_values(["meterid","chargedate"])
                .groupby("meterid", as_index=False)
                .tail(1)[["meterid","loc","res"]]
        )

        add = (
            preds[["meterid","forecast_date","kwh","ghc","paymoney"]]
            .rename(columns={"forecast_date":"chargedate"})
            .merge(last_lr, on="meterid", how="left")  # add loc,res
        )[["meterid","chargedate","loc","res","kwh","ghc","paymoney"]]

        add["chargedate"] = pd.to_datetime(add["chargedate"]).dt.to_period("M").dt.to_timestamp("M")

        out = (
            pd.concat([snap, add], ignore_index=True)
              .sort_values(["meterid","chargedate"])
              .reset_index(drop=True)
        )
        return out

    # --- add inside PredictorLGBM (predictor.py) ---

    def predict_recursive(self,
                          as_of: str,
                          steps: int = 3,
                          rows_per_meter: int = 24,
                          meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
        """
        Multi-step recursive forecast.
        Returns one tidy DataFrame with horizon=1..steps.
        """
        if steps <= 0:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

        # 1) base snapshot up to cutoff
        snap = self.load_snapshot(as_of=as_of, rows_per_meter=rows_per_meter, meters=meters)
        if snap.empty:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

        all_preds = []
        cur_as_of = as_of
        cur_snap = snap

        for h in range(1, steps + 1):
            # 2) one-step features and predict
            feat = self.transform_snapshot(cur_snap, cur_as_of)
            if feat.empty:
                break

            vecs = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

            if self.models is not None:
                Xk = vecs[:, self.req_idx["kwh"]]
                Xg = vecs[:, self.req_idx["ghc"]]
                Xp = vecs[:, self.req_idx["paymoney"]]
                kwh_hat = self.models["kwh"].predict(Xk, predict_disable_shape_check=True).reshape(-1)
                ghc_hat = self.models["ghc"].predict(Xg, predict_disable_shape_check=True).reshape(-1)
                pay_hat = self.models["paymoney"].predict(Xp, predict_disable_shape_check=True).reshape(-1)
            else:
                Xa = vecs[:, self.req_idx["all"]]
                arr = np.asarray(self.model.predict(Xa, predict_disable_shape_check=True))
                if arr.ndim == 1:
                    arr = arr.reshape(-1, 3)
                kwh_hat, ghc_hat, pay_hat = arr[:, 0], arr[:, 1], arr[:, 2]

            target_dt = _to_month_end(cur_as_of) + pd.offsets.MonthEnd(1)
            pred = pd.DataFrame({
                "meterid": feat["meterid"].astype(int).values,
                "forecast_date": target_dt,
                "horizon": h,
                "kwh": kwh_hat,
                "ghc": ghc_hat,
                "paymoney": pay_hat,
            })
            all_preds.append(pred)

            # 3) append predictions to snapshot for next step
            cur_snap = self.append_predictions(cur_snap, pred)
            cur_as_of = str(target_dt)

        if not all_preds:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

        out = pd.concat(all_preds, ignore_index=True)
        return out.sort_values(["meterid", "forecast_date", "horizon"]).reset_index(drop=True)

    # --- add inside predictor.py (inside PredictorLGBM) ---

    def load_meter_history(self,
                           meterid: int,
                           as_of: str,
                           rows_per_meter: int = 36) -> pd.DataFrame:
        """
        Fast fetch for ONE meter. Returns recent rows up to `as_of`,
        at most `rows_per_meter` months, sorted by chargedate.
        """
        cutoff = _to_month_end(as_of)
        sql = f"""
        WITH base AS (
          SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
          FROM {self.table}
          WHERE meterid = ? AND chargedate <= ?
            AND kwh >= 0 AND ghc >= 0 AND paymoney >= 0
        ),
        d AS (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid, chargedate ORDER BY chargedate) rn
          FROM base
        ),
        k AS (
          SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
          FROM d WHERE rn = 1
        )
        SELECT *
        FROM k
        QUALIFY ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY chargedate DESC) <= ?
        ORDER BY chargedate
        """
        con = duckdb.connect(str(self.db_path))
        df = con.execute(sql, [int(meterid), pd.to_datetime(cutoff), rows_per_meter]).df()
        con.close()
        if df.empty:
            return df
        df["chargedate"] = pd.to_datetime(df["chargedate"]).dt.to_period("M").dt.to_timestamp("M")
        return df.sort_values("chargedate").reset_index(drop=True)

    # --- add inside PredictorLGBM (single-meter recursive forecast) ---

    def predict_recursive_one(self,
                              meterid: int,
                              as_of: str,
                              steps: int = 12,
                              rows_per_meter: int = 36) -> pd.DataFrame:
        """
        Multi-step forecast for ONE meter.
        Returns rows with horizon=1..steps.
        """
        # 1) load only this meter's history
        snap = self.load_meter_history(meterid=meterid, as_of=as_of, rows_per_meter=rows_per_meter)
        if snap.empty:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

        all_preds = []
        cur_as_of = as_of
        cur_snap = snap

        for h in range(1, steps + 1):
            # 2) vectorized transform → one row for this meter
            feat = self.transform_snapshot(cur_snap, cur_as_of)
            if feat.empty:
                break

            vec = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

            # 3) predict one step
            if self.models is not None:
                xk = vec[:, self.req_idx["kwh"]]
                xg = vec[:, self.req_idx["ghc"]]
                xp = vec[:, self.req_idx["paymoney"]]
                kwh_hat = float(self.models["kwh"].predict(xk, predict_disable_shape_check=True).reshape(-1)[0])
                ghc_hat = float(self.models["ghc"].predict(xg, predict_disable_shape_check=True).reshape(-1)[0])
                pay_hat = float(self.models["paymoney"].predict(xp, predict_disable_shape_check=True).reshape(-1)[0])
            else:
                xa = vec[:, self.req_idx["all"]]
                arr = np.asarray(self.model.predict(xa, predict_disable_shape_check=True)).reshape(-1)
                kwh_hat, ghc_hat, pay_hat = float(arr[0]), float(arr[1]), float(arr[2])

            tgt = _to_month_end(cur_as_of) + pd.offsets.MonthEnd(1)
            pred = pd.DataFrame({
                "meterid": [int(meterid)],
                "forecast_date": [tgt],
                "horizon": [h],
                "kwh": [kwh_hat],
                "ghc": [ghc_hat],
                "paymoney": [pay_hat],
            })
            all_preds.append(pred)

            # 4) append prediction back for next step
            cur_snap = self.append_predictions(cur_snap, pred)
            cur_as_of = str(tgt)

        if not all_preds:
            return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])
        return pd.concat(all_preds, ignore_index=True).sort_values(["horizon"]).reset_index(drop=True)





