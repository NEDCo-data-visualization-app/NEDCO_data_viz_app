from pathlib import Path
class Config:
    JSON_SORT_KEYS = False
    TESTING = False
    DEBUG = False
    DATA_DIR = Path.cwd() / "data"

class DevConfig(Config):
    DEBUG = True

class ProdConfig(Config):
    DEBUG = False
