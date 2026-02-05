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
    

    def _build_features_from_db(
        self,
        meters: Optional[Iterable[int]] = None,
        as_of: Optional[str] = None,
        history_months: int = 24,
    ) -> DataFrame:
        """
        Build engineered features mostly inside DuckDB.
        Returns ONE row per meter.
        """

        if self.con is None:
            raise ValueError("DuckDB connection not initialized")

        # -------------------------------
        # infer as_of
        # -------------------------------
        if as_of is None:
            as_of = self.con.execute(
                f"SELECT max(od_date)::DATE FROM {self.raw_table}"
            ).fetchone()[0]
            if as_of is None:
                raise RuntimeError("Could not infer as_of")
            as_of = str(as_of)

        conditions = [f"od_date <= DATE '{as_of}'"]

        if history_months > 0:
            conditions.append(
                f"od_date >= DATE '{as_of}' - INTERVAL {history_months} MONTH"
            )

        if meters is not None:
            meters_sql = ",".join(str(int(m)) for m in meters)
            conditions.append(f"meterid IN ({meters_sql})")

        where_clause = " AND ".join(conditions)

        # -------------------------------
        # SQL: raw → lags → rolls → diffs → last row
        # -------------------------------
        query = f"""
        WITH base AS (
            SELECT
                meterid,
                od_date::DATE AS od_date,
                ocd_paymoney,
                ocd_energy,
                ocd_cash_received,
                utility,
                tariff_type
            FROM {self.raw_table}
            WHERE {where_clause}
            AND ocd_paymoney >= 0
            AND ocd_energy >= 0
            AND ocd_cash_received >= 0
        ),

        ordered AS (
            SELECT
                *,
                DATE_PART('year', od_date) AS year,
                DATE_PART('month', od_date) AS month,
                ((DATE_PART('month', od_date) - 1) / 3)::INT + 1 AS quarter,
                SIN(2 * PI() * DATE_PART('month', od_date) / 12.0) AS sin_month,
                COS(2 * PI() * DATE_PART('month', od_date) / 12.0) AS cos_month,

                LAG(ocd_paymoney, 1) OVER w AS ocd_paymoney_lag1,
                LAG(ocd_paymoney, 2) OVER w AS ocd_paymoney_lag2,
                LAG(ocd_paymoney, 3) OVER w AS ocd_paymoney_lag3,
                LAG(ocd_paymoney, 6) OVER w AS ocd_paymoney_lag6,
                LAG(ocd_paymoney,12) OVER w AS ocd_paymoney_lag12,

                LAG(ocd_energy, 1) OVER w AS ocd_energy_lag1,
                LAG(ocd_energy, 2) OVER w AS ocd_energy_lag2,
                LAG(ocd_energy, 3) OVER w AS ocd_energy_lag3,
                LAG(ocd_energy, 6) OVER w AS ocd_energy_lag6,
                LAG(ocd_energy,12) OVER w AS ocd_energy_lag12,

                LAG(ocd_cash_received, 1) OVER w AS ocd_cash_received_lag1,
                LAG(ocd_cash_received, 2) OVER w AS ocd_cash_received_lag2,
                LAG(ocd_cash_received, 3) OVER w AS ocd_cash_received_lag3,
                LAG(ocd_cash_received, 6) OVER w AS ocd_cash_received_lag6,
                LAG(ocd_cash_received,12) OVER w AS ocd_cash_received_lag12,

                AVG(ocd_paymoney) OVER r3 AS ocd_paymoney_roll3,
                AVG(ocd_paymoney) OVER r6 AS ocd_paymoney_roll6,
                AVG(ocd_paymoney) OVER r12 AS ocd_paymoney_roll12,

                AVG(ocd_energy) OVER r3 AS ocd_energy_roll3,
                AVG(ocd_energy) OVER r6 AS ocd_energy_roll6,
                AVG(ocd_energy) OVER r12 AS ocd_energy_roll12,

                AVG(ocd_cash_received) OVER r3 AS ocd_cash_received_roll3,
                AVG(ocd_cash_received) OVER r6 AS ocd_cash_received_roll6,
                AVG(ocd_cash_received) OVER r12 AS ocd_cash_received_roll12,

                ROW_NUMBER() OVER (PARTITION BY meterid ORDER BY od_date DESC) AS rn
            FROM base
            WINDOW
                w   AS (PARTITION BY meterid ORDER BY od_date),
                r3  AS (PARTITION BY meterid ORDER BY od_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
                r6  AS (PARTITION BY meterid ORDER BY od_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
                r12 AS (PARTITION BY meterid ORDER BY od_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
        )

        SELECT
            meterid,
            od_date,
            ocd_paymoney,
            ocd_energy,
            ocd_cash_received,
            utility,
            tariff_type,
            year, month, quarter, sin_month, cos_month,

            ocd_paymoney_lag1, ocd_paymoney_lag2, ocd_paymoney_lag3,
            ocd_paymoney_lag6, ocd_paymoney_lag12,

            ocd_energy_lag1, ocd_energy_lag2, ocd_energy_lag3,
            ocd_energy_lag6, ocd_energy_lag12,

            ocd_cash_received_lag1, ocd_cash_received_lag2, ocd_cash_received_lag3,
            ocd_cash_received_lag6, ocd_cash_received_lag12,

            ocd_paymoney_roll3, ocd_paymoney_roll6, ocd_paymoney_roll12,
            ocd_energy_roll3, ocd_energy_roll6, ocd_energy_roll12,
            ocd_cash_received_roll3, ocd_cash_received_roll6, ocd_cash_received_roll12,

            (ocd_energy - ocd_energy_lag1)        AS energy_diff,
            (ocd_paymoney - ocd_paymoney_lag1)    AS paymoney_diff,
            (ocd_cash_received - ocd_cash_received_lag1) AS cash_diff,

            CASE WHEN ocd_energy = 0 THEN 0 ELSE ocd_paymoney / ocd_energy END AS pay_ratio,
            CASE WHEN ocd_paymoney = 0 THEN 0 ELSE ocd_cash_received / ocd_paymoney END AS collection_eff

        FROM ordered
        WHERE rn = 1
        ORDER BY meterid
        """

        df = self.con.execute(query).fetchdf()

        if df.empty:
            raise RuntimeError("No feature rows produced")

        # -------------------------------------------------
        # VERY LIGHT pandas-only post processing (SAFE)
        # -------------------------------------------------
        df["utility"] = df["utility"].astype("category").cat.codes
        df["tariff_type"] = df["tariff_type"].astype("category").cat.codes

        df["energy_tariff"] = df["ocd_energy"] * df["tariff_type"]
        df["pay_tariff"] = df["ocd_paymoney"] * df["tariff_type"]

        needed = ["meterid", "od_date", *self.feature_cols]
        return df[needed].reset_index(drop=True)



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
        df_features = self._build_features_from_db(
            meters=meters,
            as_of=None,  # auto-infer max(od_date)
            history_months=history_months,
        )
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