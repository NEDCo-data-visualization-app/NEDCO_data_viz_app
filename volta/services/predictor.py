
# # Minimal overwrite: class with a single method to fetch data from DuckDB into a pandas DataFrame.

# # predictor.py — LightGBM loader + data fetch + vectorized feature builder (no TensorFlow)

# from __future__ import annotations
# from pathlib import Path
# from typing import Iterable, Optional, Dict, Any
# import duckdb
# import joblib
# import numpy as np
# import pandas as pd

# from pandas import DataFrame

# def _advance_as_of(as_of: str) -> pd.Timestamp:
#     return _to_month_end(as_of) + pd.offsets.MonthEnd(1)

# # paths: file is at volta/services/, data/ and models/ are two levels up
# DB_PATH       = Path("../../data/warehouse.duckdb")
# TABLE         = "electricity.billing_records"
# LGBM_PATH     = Path("../../models/lgbm_bundle.pkl")       # {"models": {"kwh":..., "ghc":..., "paymoney":...}} or {"model": ...}
# FEATURES_PATH = Path("../../models/feature_cols.json")     # optional; falls back to FEATURE_COLS_TRAINED

# # locked feature list used in training (order matters, 47 cols)
# FEATURE_COLS_TRAINED = [
#     "month_num","year","month_sin","month_cos",
#     "loc_enc","res_enc",
#     "kwh","ghc","paymoney",
#     "kwh_lag1","kwh_lag2","kwh_lag3",
#     "ghc_lag1","ghc_lag2","ghc_lag3",
#     "paymoney_lag1","paymoney_lag2","paymoney_lag3",
#     "kwh_roll3_mean","kwh_roll3_std","kwh_roll6_mean","kwh_roll6_std",
#     "ghc_roll3_mean","ghc_roll3_std","ghc_roll6_mean","ghc_roll6_std",
#     "paymoney_roll3_mean","paymoney_roll3_std","paymoney_roll6_mean","paymoney_roll6_std",
#     "kwh_mean","kwh_std","kwh_count","ghc_mean","ghc_std","ghc_count",
#     "paymoney_mean","paymoney_std","paymoney_count",
#     "kwh_delta1","kwh_delta2","ghc_delta1","ghc_delta2","paymoney_delta1","paymoney_delta2",
#     "ghc_per_kwh","paymoney_ratio"
# ]
# _PUT_INDEX = {n: i for i, n in enumerate(FEATURE_COLS_TRAINED)}

# def _to_month_end(ts) -> pd.Timestamp:
#     ts = pd.to_datetime(ts)
#     return ts.to_period("M").to_timestamp("M")

# def _month_features_for(dt: pd.Timestamp) -> dict:
#     m = dt.month
#     return {
#         "month_num": m,
#         "year": dt.year,
#         "month_sin": float(np.sin(2*np.pi*m/12)),
#         "month_cos": float(np.cos(2*np.pi*m/12)),
#     }

# class PredictorLGBM:
#     def __init__(self,
#                  db_path: Path = DB_PATH,
#                  table: str = TABLE,
#                  bundle_path: Path = LGBM_PATH,
#                  features_path: Path = FEATURES_PATH):
#         # DB
#         self.db_path = Path(db_path)
#         self.table = table

#         # features: prefer saved list, else locked list
#         try:
#             if Path(features_path).exists():
#                 import json
#                 self.feature_cols = json.loads(Path(features_path).read_text())
#             else:
#                 self.feature_cols = FEATURE_COLS_TRAINED
#         except Exception:
#             self.feature_cols = FEATURE_COLS_TRAINED

#         # models: bundle supports dict of per-target models or single multioutput
#         bundle: Dict[str, Any] = joblib.load(bundle_path)
#         self.models: Optional[Dict[str, Any]] = bundle.get("models")
#         self.model = bundle.get("model")

#         # required names from boosters; fallback to feature list
#         self.req_names: Dict[str, list[str]] = {}
#         if self.models is not None:
#             for t, m in self.models.items():
#                 names = m.booster_.feature_name() if hasattr(m, "booster_") else None
#                 self.req_names[t] = list(names) if names else list(self.feature_cols)
#         else:
#             names = self.model.booster_.feature_name() if hasattr(self.model, "booster_") else None
#             self.req_names["all"] = list(names) if names else list(self.feature_cols)

#         # precompute indices from our template order into each model’s expected order
#         def _build_idx(req: list[str]) -> np.ndarray:
#             idx = np.fromiter((_PUT_INDEX.get(n, -1) for n in req), dtype=np.int64)
#             if np.any(idx < 0):
#                 missing = [req[i] for i in np.where(idx < 0)[0]]
#                 raise RuntimeError(f"Missing feature(s) in template: {missing}")
#             return idx

#         self.req_idx: Dict[str, np.ndarray] = {}
#         if self.models is not None:
#             for t, names in self.req_names.items():
#                 self.req_idx[t] = _build_idx(names)
#         else:
#             self.req_idx["all"] = _build_idx(self.req_names["all"])

#     # ---------- 1) fetch data to pandas ----------
#     def load_snapshot(self,
#                       as_of: str,
#                       rows_per_meter: int = 27,
#                       meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
#         """
#         Fetch recent history up to `as_of` from DuckDB as pandas DataFrame.
#         Returns at most `rows_per_meter` most-recent rows per meter.
#         """
#         cutoff = _to_month_end(as_of)
#         where_extra = ""
#         params = [pd.to_datetime(cutoff), rows_per_meter]

#         if meters:
#             meters = list(meters)
#             placeholders = ",".join(["?"] * len(meters))
#             where_extra = f" AND meterid IN ({placeholders}) "
#             params = [pd.to_datetime(cutoff), rows_per_meter, *meters]

#         sql = f"""
#         WITH base AS (
#           SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#           FROM {self.table}
#           WHERE chargedate <= ? {where_extra}
#             AND kwh >= 0 AND ghc >= 0 AND paymoney >= 0
#         ),
#         d AS (
#           SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid, chargedate ORDER BY chargedate) rn
#           FROM base
#         ),
#         k AS (
#           SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#           FROM d WHERE rn = 1
#         )
#         SELECT *
#         FROM k
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY chargedate DESC) <= ?
#         ORDER BY meterid, chargedate
#         """

#         con = duckdb.connect(str(self.db_path))
#         df = con.execute(sql, params).df()
#         con.close()

#         if df.empty:
#             return df
##ssjddfds
#         df["chargedate"] = pd.to_datetime(df["chargedate"]).dt.to_period("M").dt.to_timestamp("M")
#         return df.sort_values(["meterid", "chargedate"]).reset_index(drop=True)

#     # ---------- 2) vectorized transformation to model-ready one-step features ----------
#     def transform_snapshot(self, df: pd.DataFrame, as_of: str) -> pd.DataFrame:
#         """
#         Build the exact FEATURE_COLS_TRAINED to predict the NEXT month
#         for every meter present in `df` (history up to cutoff). One row per meter.
#         """
#         if df.empty:
#             return pd.DataFrame(columns=["meterid", *FEATURE_COLS_TRAINED])

#         cutoff = _to_month_end(as_of)
#         target_dt = cutoff + pd.offsets.MonthEnd(1)

#         gdf = df.sort_values(["meterid", "chargedate"]).copy()

#         # last observed per meter
#         last = gdf.groupby("meterid", as_index=False).tail(1)[
#             ["meterid","chargedate","loc","res","kwh","ghc","paymoney"]
#         ].rename(columns={"chargedate": "chargedate_last"})

#         # lags at cutoff
#         for col in ["kwh","ghc","paymoney"]:
#             gdf[f"{col}_lag1"] = gdf.groupby("meterid")[col].shift(1)
#             gdf[f"{col}_lag2"] = gdf.groupby("meterid")[col].shift(2)
#             gdf[f"{col}_lag3"] = gdf.groupby("meterid")[col].shift(3)

#         lags = gdf.groupby("meterid", as_index=False).tail(1)[
#             ["meterid",
#              "kwh_lag1","kwh_lag2","kwh_lag3",
#              "ghc_lag1","ghc_lag2","ghc_lag3",
#              "paymoney_lag1","paymoney_lag2","paymoney_lag3"]
#         ]

#         # rolling windows (3, 6) mean/std at cutoff
#         roll3_mean = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(3, min_periods=3).mean().reset_index(level=0, drop=True)
#         roll3_std  = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(3, min_periods=3).std().reset_index(level=0, drop=True)
#         roll6_mean = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(6, min_periods=3).mean().reset_index(level=0, drop=True)
#         roll6_std  = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].rolling(6, min_periods=3).std().reset_index(level=0, drop=True)

#         gdf[["kwh_roll3_mean","ghc_roll3_mean","paymoney_roll3_mean"]] = roll3_mean.values
#         gdf[["kwh_roll3_std","ghc_roll3_std","paymoney_roll3_std"]]    = roll3_std.values
#         gdf[["kwh_roll6_mean","ghc_roll6_mean","paymoney_roll6_mean"]] = roll6_mean.values
#         gdf[["kwh_roll6_std","ghc_roll6_std","paymoney_roll6_std"]]    = roll6_std.values

#         rolls = gdf.groupby("meterid", as_index=False).tail(1)[
#             ["meterid",
#              "kwh_roll3_mean","kwh_roll3_std","kwh_roll6_mean","kwh_roll6_std",
#              "ghc_roll3_mean","ghc_roll3_std","ghc_roll6_mean","ghc_roll6_std",
#              "paymoney_roll3_mean","paymoney_roll3_std","paymoney_roll6_mean","paymoney_roll6_std"]
#         ]

#         # global stats per meter (mean/std/count) up to cutoff
#         agg = gdf.groupby("meterid")[["kwh","ghc","paymoney"]].agg(["mean","std","count"])
#         agg.columns = [
#             "kwh_mean","kwh_std","kwh_count",
#             "ghc_mean","ghc_std","ghc_count",
#             "paymoney_mean","paymoney_std","paymoney_count"
#         ]
#         stats = agg.reset_index()

#         # deltas at cutoff
#         for col in ["kwh","ghc","paymoney"]:
#             gdf[f"{col}_delta1"] = gdf.groupby("meterid")[col].diff(1)
#             gdf[f"{col}_delta2"] = gdf.groupby("meterid")[col].diff(2)
#         deltas = gdf.groupby("meterid", as_index=False).tail(1)[
#             ["meterid","kwh_delta1","kwh_delta2","ghc_delta1","ghc_delta2","paymoney_delta1","paymoney_delta2"]
#         ]

#         # categorical encodings at cutoff
#         enc = last[["meterid","loc","res"]].copy()
#         enc["loc_enc"] = enc["loc"].astype("category").cat.codes
#         enc["res_enc"] = enc["res"].astype("category").cat.codes
#         enc = enc[["meterid","loc_enc","res_enc"]]

#         # ratios from last observed
#         ratios = last[["meterid","kwh","ghc","paymoney"]].copy()
#         ratios["ghc_per_kwh"]    = np.where((ratios["kwh"] != 0) & ratios["kwh"].notna(), ratios["ghc"]/ratios["kwh"], 0.0)
#         ratios["paymoney_ratio"] = np.where(ratios["ghc"].notna(), ratios["paymoney"]/(ratios["ghc"] + 1e-9), 0.0)
#         ratios = ratios[["meterid","ghc_per_kwh","paymoney_ratio"]]

#         # assemble one row per meter
#         base = last[["meterid","kwh","ghc","paymoney"]].copy()
#         feat = (base.merge(lags, on="meterid", how="left")
#                     .merge(rolls, on="meterid", how="left")
#                     .merge(stats, on="meterid", how="left")
#                     .merge(deltas, on="meterid", how="left")
#                     .merge(enc, on="meterid", how="left")
#                     .merge(ratios, on="meterid", how="left"))

#         # time features for the target month (cutoff + 1M)
#         tfeat = pd.DataFrame([_month_features_for(target_dt)]).assign(key=1)
#         meters = feat[["meterid"]].assign(key=1)
#         feat = meters.merge(tfeat, on="key", how="left").drop(columns="key").merge(feat, on="meterid", how="left")

#         # order and fill
#         for c in FEATURE_COLS_TRAINED:
#             if c not in feat.columns:
#                 feat[c] = 0.0
#         feat = feat[["meterid", *FEATURE_COLS_TRAINED]].copy()
#         feat[FEATURE_COLS_TRAINED] = feat[FEATURE_COLS_TRAINED].astype(float).fillna(0.0)

#         return feat

#     # --- add inside PredictorLGBM in predictor.py ---

#     def predict_next(self,
#                      as_of: str,
#                      rows_per_meter: int = 24,
#                      meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
#         """
#         One-step forecast for all selected meters (next month only).
#         Returns columns: meterid, forecast_date, horizon, kwh, ghc, paymoney
#         """
#         # build features
#         snap = self.load_snapshot(as_of=as_of, rows_per_meter=rows_per_meter, meters=meters)
#         if snap.empty:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])
#         feat = self.transform_snapshot(snap, as_of)
#         if feat.empty:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

#         vecs = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

#         # predict
#         if self.models is not None:
#             Xk = vecs[:, self.req_idx["kwh"]]
#             Xg = vecs[:, self.req_idx["ghc"]]
#             Xp = vecs[:, self.req_idx["paymoney"]]
#             kwh_hat = self.models["kwh"].predict(Xk, predict_disable_shape_check=True).reshape(-1)
#             ghc_hat = self.models["ghc"].predict(Xg, predict_disable_shape_check=True).reshape(-1)
#             pay_hat = self.models["paymoney"].predict(Xp, predict_disable_shape_check=True).reshape(-1)
#         else:
#             Xa = vecs[:, self.req_idx["all"]]
#             arr = np.asarray(self.model.predict(Xa, predict_disable_shape_check=True))
#             if arr.ndim == 1:
#                 arr = arr.reshape(-1, 3)
#             kwh_hat, ghc_hat, pay_hat = arr[:, 0], arr[:, 1], arr[:, 2]

#         target_dt = _to_month_end(as_of) + pd.offsets.MonthEnd(1)
#         return pd.DataFrame({
#             "meterid": feat["meterid"].astype(int).values,
#             "forecast_date": target_dt,
#             "horizon": 1,
#             "kwh": kwh_hat,
#             "ghc": ghc_hat,
#             "paymoney": pay_hat,
#         })

#     def append_predictions(self, snapshot: DataFrame, preds: DataFrame) -> DataFrame:
#         """
#         Append one-step predictions as the next month's observed rows,
#         so the frame is ready for building features for the following step.
#         Keeps columns: meterid, chargedate, loc, res, kwh, ghc, paymoney.
#         """
#         if snapshot.empty or preds.empty:
#             return snapshot.copy()

#         base_cols = ["meterid","chargedate","loc","res","kwh","ghc","paymoney"]
#         snap = snapshot[base_cols].copy()

#         # get latest loc/res per meter to carry forward
#         last_lr = (
#             snap.sort_values(["meterid","chargedate"])
#                 .groupby("meterid", as_index=False)
#                 .tail(1)[["meterid","loc","res"]]
#         )

#         add = (
#             preds[["meterid","forecast_date","kwh","ghc","paymoney"]]
#             .rename(columns={"forecast_date":"chargedate"})
#             .merge(last_lr, on="meterid", how="left")  # add loc,res
#         )[["meterid","chargedate","loc","res","kwh","ghc","paymoney"]]

#         add["chargedate"] = pd.to_datetime(add["chargedate"]).dt.to_period("M").dt.to_timestamp("M")

#         out = (
#             pd.concat([snap, add], ignore_index=True)
#               .sort_values(["meterid","chargedate"])
#               .reset_index(drop=True)
#         )
#         return out

#     # --- add inside PredictorLGBM (predictor.py) ---

#     def predict_recursive(self,
#                           as_of: str,
#                           steps: int = 3,
#                           rows_per_meter: int = 24,
#                           meters: Optional[Iterable[int]] = None) -> pd.DataFrame:
#         """
#         Multi-step recursive forecast.
#         Returns one tidy DataFrame with horizon=1..steps.
#         """
#         if steps <= 0:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

#         # 1) base snapshot up to cutoff
#         snap = self.load_snapshot(as_of=as_of, rows_per_meter=rows_per_meter, meters=meters)
#         if snap.empty:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

#         all_preds = []
#         cur_as_of = as_of
#         cur_snap = snap

#         for h in range(1, steps + 1):
#             # 2) one-step features and predict
#             feat = self.transform_snapshot(cur_snap, cur_as_of)
#             if feat.empty:
#                 break

#             vecs = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

#             if self.models is not None:
#                 Xk = vecs[:, self.req_idx["kwh"]]
#                 Xg = vecs[:, self.req_idx["ghc"]]
#                 Xp = vecs[:, self.req_idx["paymoney"]]
#                 kwh_hat = self.models["kwh"].predict(Xk, predict_disable_shape_check=True).reshape(-1)
#                 ghc_hat = self.models["ghc"].predict(Xg, predict_disable_shape_check=True).reshape(-1)
#                 pay_hat = self.models["paymoney"].predict(Xp, predict_disable_shape_check=True).reshape(-1)
#             else:
#                 Xa = vecs[:, self.req_idx["all"]]
#                 arr = np.asarray(self.model.predict(Xa, predict_disable_shape_check=True))
#                 if arr.ndim == 1:
#                     arr = arr.reshape(-1, 3)
#                 kwh_hat, ghc_hat, pay_hat = arr[:, 0], arr[:, 1], arr[:, 2]

#             target_dt = _to_month_end(cur_as_of) + pd.offsets.MonthEnd(1)
#             pred = pd.DataFrame({
#                 "meterid": feat["meterid"].astype(int).values,
#                 "forecast_date": target_dt,
#                 "horizon": h,
#                 "kwh": kwh_hat,
#                 "ghc": ghc_hat,
#                 "paymoney": pay_hat,
#             })
#             all_preds.append(pred)

#             # 3) append predictions to snapshot for next step
#             cur_snap = self.append_predictions(cur_snap, pred)
#             cur_as_of = str(target_dt)

#         if not all_preds:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

#         out = pd.concat(all_preds, ignore_index=True)
#         return out.sort_values(["meterid", "forecast_date", "horizon"]).reset_index(drop=True)

#     # --- add inside predictor.py (inside PredictorLGBM) ---

#     def load_meter_history(self,
#                            meterid: int,
#                            as_of: str,
#                            rows_per_meter: int = 36) -> pd.DataFrame:
#         """
#         Fast fetch for ONE meter. Returns recent rows up to `as_of`,
#         at most `rows_per_meter` months, sorted by chargedate.
#         """
#         cutoff = _to_month_end(as_of)
#         sql = f"""
#         WITH base AS (
#           SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#           FROM {self.table}
#           WHERE meterid = ? AND chargedate <= ?
#             AND kwh >= 0 AND ghc >= 0 AND paymoney >= 0
#         ),
#         d AS (
#           SELECT *, ROW_NUMBER() OVER (PARTITION BY meterid, chargedate ORDER BY chargedate) rn
#           FROM base
#         ),
#         k AS (
#           SELECT meterid, chargedate, loc, res, kwh, ghc, paymoney
#           FROM d WHERE rn = 1
#         )
#         SELECT *
#         FROM k
#         QUALIFY ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY chargedate DESC) <= ?
#         ORDER BY chargedate
#         """
#         con = duckdb.connect(str(self.db_path))
#         df = con.execute(sql, [int(meterid), pd.to_datetime(cutoff), rows_per_meter]).df()
#         con.close()
#         if df.empty:
#             return df
#         df["chargedate"] = pd.to_datetime(df["chargedate"]).dt.to_period("M").dt.to_timestamp("M")
#         return df.sort_values("chargedate").reset_index(drop=True)

#     # --- add inside PredictorLGBM (single-meter recursive forecast) ---

#     def predict_recursive_one(self,
#                               meterid: int,
#                               as_of: str,
#                               steps: int = 12,
#                               rows_per_meter: int = 36) -> pd.DataFrame:
#         """
#         Multi-step forecast for ONE meter.
#         Returns rows with horizon=1..steps.
#         """
#         # 1) load only this meter's history
#         snap = self.load_meter_history(meterid=meterid, as_of=as_of, rows_per_meter=rows_per_meter)
#         if snap.empty:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])

#         all_preds = []
#         cur_as_of = as_of
#         cur_snap = snap

#         for h in range(1, steps + 1):
#             # 2) vectorized transform → one row for this meter
#             feat = self.transform_snapshot(cur_snap, cur_as_of)
#             if feat.empty:
#                 break

#             vec = feat[FEATURE_COLS_TRAINED].to_numpy(dtype=float)

#             # 3) predict one step
#             if self.models is not None:
#                 xk = vec[:, self.req_idx["kwh"]]
#                 xg = vec[:, self.req_idx["ghc"]]
#                 xp = vec[:, self.req_idx["paymoney"]]
#                 kwh_hat = float(self.models["kwh"].predict(xk, predict_disable_shape_check=True).reshape(-1)[0])
#                 ghc_hat = float(self.models["ghc"].predict(xg, predict_disable_shape_check=True).reshape(-1)[0])
#                 pay_hat = float(self.models["paymoney"].predict(xp, predict_disable_shape_check=True).reshape(-1)[0])
#             else:
#                 xa = vec[:, self.req_idx["all"]]
#                 arr = np.asarray(self.model.predict(xa, predict_disable_shape_check=True)).reshape(-1)
#                 kwh_hat, ghc_hat, pay_hat = float(arr[0]), float(arr[1]), float(arr[2])

#             tgt = _to_month_end(cur_as_of) + pd.offsets.MonthEnd(1)
#             pred = pd.DataFrame({
#                 "meterid": [int(meterid)],
#                 "forecast_date": [tgt],
#                 "horizon": [h],
#                 "kwh": [kwh_hat],
#                 "ghc": [ghc_hat],
#                 "paymoney": [pay_hat],
#             })
#             all_preds.append(pred)

#             # 4) append prediction back for next step
#             cur_snap = self.append_predictions(cur_snap, pred)
#             cur_as_of = str(tgt)

#         if not all_preds:
#             return pd.DataFrame(columns=["meterid", "forecast_date", "horizon", "kwh", "ghc", "paymoney"])
#         return pd.concat(all_preds, ignore_index=True).sort_values(["horizon"]).reset_index(drop=True)

ffrom __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Optional, Iterable

import duckdb
import joblib
import numpy as np
import pandas as pd
from pandas import DataFrame


class PredictorLGBM:
    """
    Forecasting pipeline wrapper for NEDCo 12-month LightGBM models.

    Public entrypoints:
        - predict_from_raw(df_raw)
        - predict_from_db(meters=None, history_months=24)
        - predict_all_from_db(history_months=24)
        - predict_one_meter_from_db(meterid, history_months=24)

    Internal helpers:
        - _fetch_raw_from_db(...)
        - _build_features_from_raw(df_raw)
        - _predict_from_features(df_features)
    """

    def __init__(
        self,
        model_dir: Path,
        db_path: Optional[Path] = None,
        raw_table: Optional[str] = None,
    ):
        """
        On initialization:
          - Load 36 separate LightGBM model files from model_dir
          - Optionally connect to DuckDB
        """
        self.model_dir = Path(model_dir)

        # ----------------------------------------------------
        # Load fixed feature list (you no longer have a bundle)
        # ----------------------------------------------------
        self.feature_cols = [
            'ocd_paymoney','ocd_energy','ocd_cash_received','utility','tariff_type',
            'year','month','quarter','sin_month','cos_month',
            'ocd_paymoney_lag1','ocd_paymoney_lag2','ocd_paymoney_lag3',
            'ocd_paymoney_lag6','ocd_paymoney_lag12',
            'ocd_energy_lag1','ocd_energy_lag2','ocd_energy_lag3',
            'ocd_energy_lag6','ocd_energy_lag12',
            'ocd_cash_received_lag1','ocd_cash_received_lag2','ocd_cash_received_lag3',
            'ocd_cash_received_lag6','ocd_cash_received_lag12',
            'ocd_paymoney_roll3','ocd_paymoney_roll6','ocd_paymoney_roll12',
            'ocd_energy_roll3','ocd_energy_roll6','ocd_energy_roll12',
            'ocd_cash_received_roll3','ocd_cash_received_roll6','ocd_cash_received_roll12',
            'energy_diff','paymoney_diff','cash_diff','pay_ratio','collection_eff',
            'energy_tariff','pay_tariff'
        ]

        # ----------------------------------------------------
        # Load all individual models
        # ----------------------------------------------------
        self.models_paymoney: Dict[int, Any] = {}
        self.models_energy: Dict[int, Any] = {}
        self.models_cash: Dict[int, Any] = {}

        for h in range(1, 13):
            pay_path = self.model_dir / f"lgbm_paymoney_h{h}.pkl"
            energy_path = self.model_dir / f"lgbm_energy_h{h}.pkl"
            cash_path = self.model_dir / f"lgbm_cash_h{h}.pkl"

            try:
                self.models_paymoney[h] = joblib.load(pay_path)
                self.models_energy[h] = joblib.load(energy_path)
                self.models_cash[h] = joblib.load(cash_path)
            except Exception as e:
                raise RuntimeError(f"Failed loading model for horizon {h}: {e}")

        print("✔ Loaded individual LightGBM models:")
        print(f"   paymoney={len(self.models_paymoney)}")
        print(f"   energy={len(self.models_energy)}")
        print(f"   cash={len(self.models_cash)}")

        # ----------------------------------------------------
        # Connect to DuckDB (optional)
        # ----------------------------------------------------
        self.db_path = Path(db_path) if db_path is not None else None
        self.raw_table = raw_table
        self.con: Optional[duckdb.DuckDBPyConnection] = None

        if self.db_path is not None:
            try:
                self.con = duckdb.connect(str(self.db_path))
                print(f"✔ Connected to DuckDB at: {self.db_path}")
            except Exception as e:
                raise RuntimeError(f"Failed to connect to DuckDB at {self.db_path}: {e}")
        else:
            print("⚠ No DB path provided — predict_from_db() will be unavailable.")

        # ----------------------------------------------------
        # Validate raw table exists (optional)
        # ----------------------------------------------------
        if self.con is not None and self.raw_table is not None:
            try:
                tables_df = self.con.execute("SHOW TABLES").fetchdf()
                tables = tables_df["name"].tolist()
            except Exception as e:
                raise RuntimeError(f"Failed to list tables from DuckDB: {e}")

            if self.raw_table not in tables:
                raise ValueError(
                    f"Raw table '{self.raw_table}' does NOT exist in DuckDB.\n"
                    f"Available tables: {tables}"
                )
            print(f"✔ Raw table found: {self.raw_table}")
        elif self.con is not None and self.raw_table is None:
            print("⚠ No raw_table provided — you must pass df_raw to predict_from_raw().")


    # ======================================================================
    # INTERNAL: FETCH RAW DATA FROM DUCKDB
    # ======================================================================
    def _fetch_raw_from_db(
        self,
        meters: Optional[Iterable[int]] = None,
        as_of: Optional[str] = None,
        history_months: int = 24,
    ) -> DataFrame:
        """
        PRIVATE.

        Fetch raw monthly data from DuckDB with the minimum history needed
        for feature engineering.

        If as_of is None:
            - infer as_of as max(od_date) in the table.
        """
        if self.con is None or self.db_path is None:
            raise ValueError("Database connection is not initialized. Provide db_path in __init__.")

        if self.raw_table is None:
            raise ValueError("raw_table is not set. Provide raw_table in __init__.")

        # -----------------------------------------
        # Determine as_of if not provided
        # -----------------------------------------
        if as_of is None:
            max_date_query = f"SELECT max(od_date)::DATE AS max_date FROM {self.raw_table}"
            res = self.con.execute(max_date_query).fetchdf()
            if res.empty or res["max_date"].iloc[0] is None:
                raise RuntimeError(f"Could not determine max(od_date) from table {self.raw_table}.")
            as_of = str(res["max_date"].iloc[0])
            print(f"Using inferred as_of = {as_of}")

        # -----------------------------------------
        # Build WHERE clause
        # -----------------------------------------
        conditions = [f"od_date <= DATE '{as_of}'"]

        # last N months window
        if history_months is not None and history_months > 0:
            conditions.append(
                f"od_date >= DATE '{as_of}' - INTERVAL {history_months} MONTH"
            )

        # meter filter
        if meters is not None:
            meter_list = ",".join(str(int(m)) for m in meters)
            conditions.append(f"meterid IN ({meter_list})")

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT *
            FROM {self.raw_table}
            WHERE {where_clause}
        """

        try:
            df_raw = self.con.execute(query).fetchdf()
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch raw data from DuckDB with query:\n{query}\nError: {e}"
            )

        if df_raw.empty:
            raise RuntimeError(
                f"No rows returned from {self.raw_table} for as_of={as_of}, "
                f"history_months={history_months}, meters={meters}."
            )

        print(
            f"Fetched {len(df_raw)} rows from {self.raw_table} "
            f"(as_of={as_of}, history_months={history_months}, meters={'ALL' if meters is None else meters})"
        )

        return df_raw

    # ======================================================================
    # INTERNAL: FEATURE ENGINEERING
    # ======================================================================
    def _build_features_from_raw(self, df_raw: DataFrame) -> DataFrame:
        """
        PRIVATE.
    
        Takes raw monthly data with at least:
            meterid, od_date,
            ocd_paymoney, ocd_energy, ocd_cash_received,
            utility, tariff_type
    
        Returns:
            One latest row per meter with columns:
                ['meterid', 'od_date'] + self.feature_cols
        """
        df = df_raw.copy()
    
        # -----------------------------
        # Basic required columns check
        # -----------------------------
        base_required = [
            "meterid",
            "od_date",
            "ocd_paymoney",
            "ocd_energy",
            "ocd_cash_received",
            "utility",
            "tariff_type",
        ]
        missing_base = [c for c in base_required if c not in df.columns]
        if missing_base:
            raise ValueError(f"Raw data missing required columns: {missing_base}")
    
        # -----------------------------
        # Time features
        # -----------------------------
        df["od_date"] = pd.to_datetime(df["od_date"])
        df = df.sort_values(["meterid", "od_date"])
    
        df["year"] = df["od_date"].dt.year
        df["month"] = df["od_date"].dt.month
        df["quarter"] = (df["month"] - 1) // 3 + 1
    
        # cyclical month encodings
        df["sin_month"] = np.sin(2 * np.pi * df["month"] / 12.0)
        df["cos_month"] = np.cos(2 * np.pi * df["month"] / 12.0)
    
        # -----------------------------
        # Encode categorical columns
        # -----------------------------
        # make both categorical -> integer codes (matches training)
        df["utility"] = df["utility"].astype("category").cat.codes
        df["tariff_type"] = df["tariff_type"].astype("category").cat.codes
    
        # -----------------------------
        # Lags and rolling means
        # -----------------------------
        by_meter = df.groupby("meterid", group_keys=False)
    
        amount_cols = ["ocd_paymoney", "ocd_energy", "ocd_cash_received"]
        lag_windows = [1, 2, 3, 6, 12]
        roll_windows = [3, 6, 12]
    
        # lags
        for col in amount_cols:
            for lag in lag_windows:
                lag_col = f"{col}_lag{lag}"
                df[lag_col] = by_meter[col].shift(lag)
    
        # rolling means
        for col in amount_cols:
            for w in roll_windows:
                roll_col = f"{col}_roll{w}"
                df[roll_col] = (
                    by_meter[col]
                    .rolling(window=w, min_periods=1)
                    .mean()
                    .reset_index(level=0, drop=True)
                )
    
        # -----------------------------
        # Diffs
        # -----------------------------
        df["energy_diff"] = df["ocd_energy"] - df["ocd_energy_lag1"]
        df["paymoney_diff"] = df["ocd_paymoney"] - df["ocd_paymoney_lag1"]
        df["cash_diff"] = df["ocd_cash_received"] - df["ocd_cash_received_lag1"]
    
        # -----------------------------
        # Ratios (safe division)
        # -----------------------------
        denom_energy = df["ocd_energy"].replace(0, np.nan)
        df["pay_ratio"] = (df["ocd_paymoney"] / denom_energy).fillna(0.0)
    
        denom_pay = df["ocd_paymoney"].replace(0, np.nan)
        df["collection_eff"] = (df["ocd_cash_received"] / denom_pay).fillna(0.0)
    
        # -----------------------------
        # Interactions with tariff_type
        # -----------------------------
        df["energy_tariff"] = df["ocd_energy"] * df["tariff_type"]
        df["pay_tariff"] = df["ocd_paymoney"] * df["tariff_type"]
    
        # -----------------------------
        # Keep the last month per meter for inference
        # -----------------------------
        df = df.sort_values(["meterid", "od_date"])
        df_last = df.groupby("meterid", as_index=False).tail(1)
    
        # -----------------------------
        # Ensure we have exactly the features expected by the models
        # -----------------------------
        needed_cols = ["meterid", "od_date"] + list(self.feature_cols)
        missing_feats = [c for c in needed_cols if c not in df_last.columns]
        if missing_feats:
            raise ValueError(
                f"Engineered data is missing expected feature columns: {missing_feats}"
            )
    
        return df_last[needed_cols].reset_index(drop=True)



    # ======================================================================
    # INTERNAL: MODEL INFERENCE
    # ======================================================================
    def _predict_from_features(self, df_features: DataFrame) -> DataFrame:
        """
        PRIVATE.

        Run the 36 LightGBM models on engineered features.

        Input:
            df_features: DataFrame with columns:
                ['meterid', 'od_date'] + self.feature_cols

        Output (long format):
            meterid | as_of | horizon | paymoney_pred | energy_pred | cash_pred
        """
        if not set(["meterid", "od_date"]).issubset(df_features.columns):
            raise ValueError("df_features must contain 'meterid' and 'od_date' columns.")

        X = df_features[self.feature_cols]
        meterids = df_features["meterid"].values
        as_of_dates = df_features["od_date"].values

        records = []

        for h in range(1, 13):
            if (
                h not in self.models_paymoney
                or h not in self.models_energy
                or h not in self.models_cash
            ):
                raise ValueError(f"Missing model for horizon {h} in bundle.")

            m_pay = self.models_paymoney[h]
            m_energy = self.models_energy[h]
            m_cash = self.models_cash[h]

            y_pay = m_pay.predict(X)
            y_energy = m_energy.predict(X)
            y_cash = m_cash.predict(X)
            
            for meter, as_of, yp, ye, yc in zip(meterids, as_of_dates, y_pay, y_energy, y_cash):
                pred_date = pd.to_datetime(as_of) + pd.DateOffset(months=h)
                records.append(
                    {
                        "meterid": int(meter),
                        "as_of": pd.to_datetime(as_of),
                        "horizon": h,
                        "prediction_date": pred_date,
                        "paymoney_pred": float(yp),
                        "energy_pred": float(ye),
                        "cash_pred": float(yc),
                    }
                )

        return pd.DataFrame(records)

    # ======================================================================
    # PUBLIC: PREDICT FROM RAW DATAFRAME
    # ======================================================================
    def predict_from_raw(self, df_raw: DataFrame) -> DataFrame:
        """
        Public API.

        Caller provides raw monthly data as a DataFrame (no DB).

        Steps:
            1) Build features
            2) Run models

        Returns:
            long-format predictions:
                meterid | as_of | horizon | paymoney_pred | energy_pred | cash_pred
        """
        df_features = self._build_features_from_raw(df_raw)
        df_preds = self._predict_from_features(df_features)
        return df_preds

    # ======================================================================
    # PUBLIC: PREDICT DIRECTLY FROM DUCKDB (generic)
    # ======================================================================
    def predict_from_db(
        self,
        meters: Optional[Iterable[int]] = None,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Public API (generic).

        - If meters is None: predicts for ALL meters in the table as of the latest available date.
        - If meters is a list: predicts only for those meterids, using history up to latest date.

        Steps:
            1) Fetch minimal history from DuckDB (as_of inferred automatically)
            2) Build features
            3) Run models and return long-format predictions
        """
        df_raw = self._fetch_raw_from_db(
            meters=meters,
            as_of=None,  # auto-infer max(od_date)
            history_months=history_months,
        )
        df_features = self._build_features_from_raw(df_raw)
        df_preds = self._predict_from_features(df_features)
        return df_preds

    # ======================================================================
    # PUBLIC: CONVENIENCE WRAPPERS
    # ======================================================================
    def predict_all_from_db(
        self,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Predict for ALL meterids, using the latest available date in the table
        as the reference point.

        Horizon 1 = next month after each meter's last observed od_date.
        """
        return self.predict_from_db(
            meters=None,
            history_months=history_months,
        )

    def predict_one_meter_from_db(
        self,
        meterid: int,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Predict for a single meterid, using history up to the latest available date.

        Horizon 1 = next month after this meter's last observed od_date.
        """
        return self.predict_from_db(
            meters=[meterid],
            history_months=history_months,
        )
