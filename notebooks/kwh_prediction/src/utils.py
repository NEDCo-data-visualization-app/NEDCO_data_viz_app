import pandas as pd
import time
from contextlib import contextmanager
from pathlib import Path

def memory_usage_mb(df: pd.DataFrame) -> float:
    '''Return approximate DataFrame memory usage (MB).'''
    return float(df.memory_usage(deep=True).sum()) / (1024 ** 2)

@contextmanager
def timer(name: str):
    t0 = time.time()
    yield
    print(f"[{name}] done in {time.time() - t0:.2f}s")

def safe_to_datetime(series, dayfirst=False):
    '''Coerce to datetime, return NaT on failure.'''
    return pd.to_datetime(series, errors='coerce', dayfirst=dayfirst)

def add_season_column(df: pd.DataFrame, month_col='month', out_col='season'):
    '''Add simple season labels based on month number.'''
    season_map = {
        12:'winter',1:'winter',2:'winter',
        3:'spring',4:'spring',5:'spring',
        6:'summer',7:'summer',8:'summer',
        9:'fall',10:'fall',11:'fall'
    }
    df[out_col] = df[month_col].map(season_map)
    return df