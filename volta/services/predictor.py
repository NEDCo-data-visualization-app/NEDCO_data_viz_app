from __future__ import annotations
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
        - _fetch_raw_from_parquet(...)
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
        self.raw_table = raw_table

    # ======================================================================
    # INTERNAL: FETCH RAW DATA FROM DUCKDB
    # ======================================================================
    def _fetch_raw_from_parquet(
       self,
        datastore,
        meters: list[int] | None = None,
        as_of: str | None = None,
        history_months: int = 24,
    ) -> pd.DataFrame:
        """
        Fetch raw monthly data directly from Parquet via DataStore + DuckDB in-memory.
        Returns a pandas DataFrame.
        """
        date_col = "od_date"  # or self.date_col
        clauses = [f"{date_col} IS NOT NULL"]
        params: list[object] = []

        if as_of:
            clauses.append(f"{date_col} <= ?")
            params.append(as_of)

        if meters:
            meter_list = ",".join(str(int(m)) for m in meters)
            clauses.append(f"meterid IN ({meter_list})")

        # Build SQL
        where_sql = " AND ".join(clauses)
        sql = f"SELECT * FROM '{datastore.parquet_path}' WHERE {where_sql}"

        # Fetch rows via DataStore
        rows = datastore.run_query(sql, params)

        if not rows:
            raise RuntimeError("No data returned from Parquet.")

        # Convert to DataFrame for ML models
        import pandas as pd
        df_raw = pd.DataFrame(rows)
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
        datastore, 
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
        df_raw = self._fetch_raw_from_parquet(
            datastore,
            meters=meters,
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
        datastore,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Predict for ALL meterids, using the latest available date in the table
        as the reference point.

        Horizon 1 = next month after each meter's last observed od_date.
        """
        return self.predict_from_db(
            datastore,
            meters=None,
            history_months=history_months,
        )

    def predict_one_meter_from_db(
        self,
        datastore, 
        meterid: int,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Predict for a single meterid, using history up to the latest available date.

        Horizon 1 = next month after this meter's last observed od_date.
        """
        return self.predict_from_db(
            datastore, 
            meters=[meterid],
            history_months=history_months,
        )
