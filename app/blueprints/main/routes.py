from flask import Blueprint\n\nbp = Blueprint("main", __name__)\n\nfrom flask import Blueprint
﻿from app import create_app
from app.config import DevConfig
app = create_app(DevConfig)
if __name__ == "__main__":
    app.run()
