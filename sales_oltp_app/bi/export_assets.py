"""Export the Superset BI assets to YAML under bi/assets/ — the git-tracked source of truth.

Pulls the dashboard's full asset bundle (dashboard + charts + dataset + database) as a ZIP via the
REST export endpoint and unpacks it into bi/assets/. This is the "dashboards as code" round-trip:
the BI catalog becomes reviewable, diffable YAML that re-imports into any fresh Superset. Database
passwords are exported masked (null) by Superset, so nothing secret lands in git. See Sales_BI_Design.md.

Run:  sales_oltp_app/.venv/Scripts/python.exe bi/export_assets.py
"""
import importlib.util
import io
import os
import shutil
import urllib.request
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = os.path.join(HERE, "assets")
_spec = importlib.util.spec_from_file_location("setup_superset", os.path.join(HERE, "setup_superset.py"))
ss = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ss)

DASH_ID = 1


def export_zip():
    url = f"{ss.SS_URL}/api/v1/dashboard/export/?q=!({DASH_ID})"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {ss._state['access']}", "Referer": ss.SS_URL})
    with ss.opener.open(req, timeout=60) as r:
        return r.read()


def main():
    ss.login()
    raw = export_zip()
    zf = zipfile.ZipFile(io.BytesIO(raw))
    if os.path.isdir(ASSETS):
        shutil.rmtree(ASSETS)
    os.makedirs(ASSETS, exist_ok=True)
    written = []
    for name in zf.namelist():
        if name.endswith("/"):
            continue
        # strip the top-level export root dir so paths are stable across exports
        rel = name.split("/", 1)[1] if "/" in name else name
        dest = os.path.join(ASSETS, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with zf.open(name) as src, open(dest, "wb") as out:
            out.write(src.read())
        written.append(rel)
    print(f"exported {len(written)} files to bi/assets/:")
    for w in sorted(written):
        print(f"  {w}")


if __name__ == "__main__":
    main()
