#!/usr/bin/env bash
set -euo pipefail

APP_PORT="${APP_PORT:-1012}"

# 进入代码目录
cd /app

# 依赖：优先用 requirements.txt，没有就装基础依赖
python -m pip install --upgrade pip >/dev/null 2>&1 || true
if [ -f requirements.txt ]; then
  pip install --no-cache-dir -r requirements.txt
else
  pip install --no-cache-dir flask gunicorn requests
fi

# 如果没设置 SECRET_KEY，自动注入到 webapp.py（仅当文件内不存在时）
if [ -z "${FLASK_SECRET_KEY:-}" ]; then
  FLASK_SECRET_KEY="$(python - <<'PY'
import secrets; print(secrets.token_hex(32))
PY
)"
fi

# 确保 webapp.py 能 0.0.0.0 监听 APP_PORT（若已写好则不改）
python - <<'PY'
from pathlib import Path
p = Path("webapp.py")
if p.exists():
    s = p.read_text(encoding="utf-8")
    if "def healthz" not in s:
        s += """

from flask import jsonify
@bp.route('/healthz')
def healthz():
    return jsonify({"ok": True})
"""
    # 确保主启动段监听 0.0.0.0:APP_PORT
    if "__main__" in s and "app.run(" in s and "0.0.0.0" not in s:
        s = s.replace("app.run(", "app.run(host='0.0.0.0', port=int(os.getenv('APP_PORT','1012')), ")
    p.write_text(s, encoding="utf-8")
PY

# 运行（优先 gunicorn，webapp.py 内必须存在 app 实例： webapp:app）
exec gunicorn -w 1 -b 0.0.0.0:"${APP_PORT}" webapp:app
