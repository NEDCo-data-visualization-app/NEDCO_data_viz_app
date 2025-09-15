import re, sys, pathlib

root = pathlib.Path(__file__).resolve().parents[1]
src  = root / "main.py"
dst  = root / "app" / "blueprints" / "main" / "routes.py"

code = src.read_text(encoding="utf-8")

# Remove dev runner
code = re.sub(r"if __name__\\s*==\\s*[\"\\']__main__[\"\\']:\\s*\\n[\\s\\S]*$", "", code, flags=re.M)

# Ensure Blueprint import
if "Blueprint" not in code:
    code = code.replace("from flask import ", "from flask import ", 1)
    if "from flask import " in code:
        # Append Blueprint into existing import line if present
        code = re.sub(r"(from\\s+flask\\s+import\\s+)([^\n]+)",
                      lambda m: m.group(1) + (m.group(2) + ", Blueprint" if "Blueprint" not in m.group(2) else m.group(2)),
                      code, count=1)
    else:
        code = "from flask import Blueprint\n" + code

# Replace app = Flask(__name__) with bp = Blueprint("main", __name__)
code = re.sub(r"\\bapp\\s*=\\s*Flask\\([^)]*\\)", 'bp = Blueprint("main", __name__)', code)

# Replace route decorators: @app.route -> @bp.route
code = code.replace("@app.route", "@bp.route")

# Drop app.run(...) calls if any remain
code = re.sub(r"\\bapp\\.run\\([^)]*\\)", "", code)

# Export bp symbol if not present
if "bp =" not in code:
    code = 'from flask import Blueprint\\n\\nbp = Blueprint("main", __name__)\\n\\n' + code

# Write routes.py
dst.write_text(code, encoding="utf-8")
print(f"Converted routes to {dst}")
