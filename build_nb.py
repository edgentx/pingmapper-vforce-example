import json, os

md = lambda *s: {"cell_type": "markdown", "metadata": {}, "source": list(s)}
code = lambda *s: {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": list(s)}

cells = []

cells.append(md(
"# PINGMapper → VForce **Flow** + **Lakehouse**\n",
"\n",
"This notebook is the reference example for how **VForce Flow** (orchestration) and **VForce Lakehouse** (storage + catalog + analytics) work together, using\n",
"[PINGMapper](https://cameronbodine.github.io/PINGMapper/) — an open-source tool that turns consumer side-scan **sonar** recordings into georeferenced imagery and benthic-substrate maps.\n",
"\n",
"**The pattern**\n",
"1. **Process** — run PINGMapper on a sonar recording → GeoTIFFs, sonograms, substrate maps, mosaics.\n",
"2. **Store** — push that imagery into **VForce Lakehouse** so it's an object + a catalog row you can search, view, and query.\n",
"3. **Orchestrate** — in production you don't run this by hand: a **VForce Flow** pipeline does step 1 → step 2 on every new recording, and every execution shows up in the Flow **Runs** view. The flow lives as code in a **git-sourced project**.\n",
))

# ---- 0. setup
cells.append(md("## 0 · Setup\n",
"\nPINGMapper pulls heavy geospatial deps (GDAL, rasterio). The cleanest install is the project's Miniforge/`pixi` environment (see the [docs](https://cameronbodine.github.io/PINGMapper/)); for a notebook, `pip` works if your kernel already has GDAL:\n"))
cells.append(code(
"# Recommended: a conda/Miniforge env per the PINGMapper docs.\n",
"# In a ready kernel, this is enough:\n",
"%pip install -q pingmapper requests\n"))

# ---- 1. process
cells.append(md("## 1 · Process a sonar recording with PINGMapper\n",
"\n",
"Fastest path — the built-in test downloads a small sample recording and runs the whole pipeline:\n"))
cells.append(code(
"# One-liner (downloads the small sample dataset, ~ a few MB, and processes it):\n",
"# !python -m pingmapper test 1\n",
"#\n",
"# Below we do the same thing *scripted*, so we control the output folder and can\n",
"# hand the results to the Lakehouse in step 2.\n"))

cells.append(code(
"import os, sys, time, glob, zipfile, requests\n",
"\n",
"from pingmapper.main_readFiles import read_master_func\n",
"from pingmapper.main_rectify import rectify_master_func\n",
"from pingmapper.main_mapSubstrate import map_master_func\n",
"from pingmapper.funcs_model import DEPTH_DETECTION_AVAILABLE\n",
"\n",
"WORK = os.path.abspath('./pingmapper_work')\n",
"DATA = os.path.join(WORK, 'data')\n",
"os.makedirs(DATA, exist_ok=True)\n",
"\n",
"# --- download the small sample sonar recording (Humminbird .DAT + .SON) ---\n",
"ds_name = 'Test-Small-DS'\n",
"ds_path = os.path.join(DATA, ds_name)\n",
"if not os.path.exists(ds_path + '.DAT'):\n",
"    url = 'https://github.com/CameronBodine/PINGMapper/releases/download/data/%s.zip' % ds_name\n",
"    print('Downloading sample recording…')\n",
"    zf = ds_path + '.zip'\n",
"    with open(zf, 'wb') as f:\n",
"        f.write(requests.get(url, allow_redirects=True).content)\n",
"    with zipfile.ZipFile(zf) as z:\n",
"        z.extractall(DATA)\n",
"    os.remove(zf)\n",
"print('Recording:', ds_path + '.DAT')\n"))

cells.append(md("### Parameters\n",
"PINGMapper is driven by one parameter dict. These are the documented defaults from `test_PINGMapper.py`; tune freely (resolution, EGN gain, depth detection, substrate mapping, mosaics, …).\n"))
cells.append(code(
"inFile  = os.path.abspath(ds_path + '.DAT')\n",
"sonPath = os.path.abspath(ds_path)\n",
"projDir = os.path.abspath(os.path.join(WORK, 'PINGMapper-' + ds_name))\n",
"sonFiles = sorted(glob.glob(os.path.join(sonPath, '*.SON')))\n",
"\n",
"# PINGMapper copies the 'script' into the project for reproducibility — give it a real file.\n",
"script_src = os.path.abspath('PINGMapper_lakehouse_flow.ipynb')\n",
"if not os.path.exists(script_src):\n",
"    script_src = os.path.abspath('pingmapper_run.py'); open(script_src, 'a').close()\n",
"copied = 'PINGMapper_run_' + time.strftime('%Y-%m-%d_%H%M') + '.py'\n",
"\n",
"params = {\n",
"    'logfilename': os.path.join(projDir, 'log.txt'),\n",
"    'project_mode': 1,            # 1 = overwrite if it exists\n",
"    'script': [script_src, copied],\n",
"    'inFile': inFile, 'sonFiles': sonFiles, 'projDir': projDir,\n",
"    'tempC': 10, 'nchunk': 500, 'exportUnknown': True, 'fixNoDat': False, 'threadCnt': 0.5,\n",
"    'pix_res_son': 0.05, 'pix_res_map': 0.25,\n",
"    'x_offset': 0.0, 'y_offset': 0.0,\n",
"    'egn': True, 'egn_stretch': 1, 'egn_stretch_factor': 0.5, 'tone_gamma': 1.0, 'tone_gain': 1.0,\n",
"    'tileFile': '.jpg', 'wcp': True, 'wcr': True, 'wco': True, 'wcm': True,\n",
"    'sonogram_colorMap': 'copper', 'spdCor': True, 'maxCrop': True, 'USE_GPU': False,\n",
"    'remShadow': 1, 'detectDep': 1, 'smthDep': True, 'adjDep': 0, 'pltBedPick': True,\n",
"    'rect_wcp': True, 'rect_wcr': True, 'son_colorMap': 'Greys_r',\n",
"    'pred_sub': 1, 'map_sub': 1, 'export_poly': True, 'map_predict': False,\n",
"    'pltSubClass': True, 'map_class_method': 'max',\n",
"    'mosaic_nchunk': 0, 'mosaic': 1, 'map_mosaic': 1,\n",
"    'banklines': True, 'coverage': True,\n",
"}\n",
"\n",
"# Substrate prediction needs the ML deps; disable gracefully if absent.\n",
"if not DEPTH_DETECTION_AVAILABLE:\n",
"    params.update(remShadow=0, detectDep=0, pred_sub=0, pltSubClass=False,\n",
"                  map_sub=0, export_poly=False, map_predict=False)\n",
"print('%d SON channels · output → %s' % (len(sonFiles), projDir))\n"))

cells.append(md("### Run the pipeline\n", "`read → rectify → map` — the same three master functions the CLI calls.\n"))
cells.append(code(
"read_master_func(**params)                       # decode pings → sonograms\n",
"if params['rect_wcp'] or params['rect_wcr'] or params['banklines'] or params['coverage']:\n",
"    rectify_master_func(**params)                # georeference → GeoTIFF mosaics, tracklines\n",
"if params['pred_sub'] or params['map_sub'] or params['export_poly']:\n",
"    map_master_func(**params)                    # ML substrate classification → maps\n",
"print('Done. Outputs in', projDir)\n"))

cells.append(md("### What got produced\n"))
cells.append(code(
"outs = []\n",
"for ext in ('*.tif', '*.jpg', '*.shp', '*.png'):\n",
"    outs += glob.glob(os.path.join(projDir, '**', ext), recursive=True)\n",
"for p in sorted(outs)[:40]:\n",
"    print(round(os.path.getsize(p)/1024), 'KB ', os.path.relpath(p, projDir))\n",
"print('\\n%d output files total' % len(outs))\n"))

# ---- 2. lakehouse
cells.append(md("## 2 · Store the imagery in VForce Lakehouse\n",
"\n",
"Zip the georeferenced outputs and ingest them as a **batch** — the Lakehouse persists each as an object in open storage (MinIO/Iceberg) and a catalog row, so the imagery is searchable, viewable in the doc viewer, and queryable in SQL.\n",
"\n",
"> Set `LAKEHOUSE_URL` / `LAKEHOUSE_TOKEN` for your environment. The platform is fronted by oauth2 — pass a bearer token (or run inside the cluster).\n"))
cells.append(code(
"LAKEHOUSE_URL   = os.environ.get('LAKEHOUSE_URL', 'https://lakehouse.vforce360.ai')\n",
"LAKEHOUSE_TOKEN = os.environ.get('LAKEHOUSE_TOKEN', '')          # bearer token\n",
"TENANT          = os.environ.get('LAKEHOUSE_TENANT', 'default')\n",
"BATCH_ID        = 'sonar-' + ds_name + '-' + time.strftime('%Y%m%d-%H%M')\n",
"\n",
"# Bundle the GeoTIFFs + sonograms into one archive.\n",
"archive = os.path.join(WORK, BATCH_ID + '.zip')\n",
"with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:\n",
"    for p in outs:\n",
"        if p.lower().endswith(('.tif', '.jpg', '.png')):\n",
"            z.write(p, arcname=os.path.relpath(p, projDir))\n",
"print('Archive:', archive, '(%d KB)' % round(os.path.getsize(archive)/1024))\n",
"\n",
"headers = {'X-Tenant-Id': TENANT}\n",
"if LAKEHOUSE_TOKEN:\n",
"    headers['Authorization'] = 'Bearer ' + LAKEHOUSE_TOKEN\n",
"\n",
"with open(archive, 'rb') as f:\n",
"    resp = requests.post(LAKEHOUSE_URL + '/api/v1/ingest/archive',\n",
"                         headers=headers,\n",
"                         files={'file': (os.path.basename(archive), f, 'application/zip')})\n",
"print(resp.status_code, resp.text[:400])\n"))

cells.append(md("Single-object alternative — ingest one GeoTIFF directly (`POST /api/v1/ingest/document`, `kind` ∈ `tiff|jpeg|jp2|pdf`):\n"))
cells.append(code(
"import base64\n",
"tifs = [p for p in outs if p.lower().endswith('.tif')]\n",
"if tifs:\n",
"    with open(tifs[0], 'rb') as f:\n",
"        payload = {'batch_id': BATCH_ID, 'kind': 'tiff', 'data': base64.b64encode(f.read()).decode()}\n",
"    r = requests.post(LAKEHOUSE_URL + '/api/v1/ingest/document', headers=headers, json=payload)\n",
"    print(r.status_code, r.text[:300])   # -> {\"doc_id\": ..., \"doc_type\": ...}\n"))

cells.append(md("Confirm it landed — query the catalog over SQL (Trino):\n"))
cells.append(code(
"r = requests.post(LAKEHOUSE_URL + '/api/v1/compute/sql', headers=headers,\n",
"                  json={'sql': \"SELECT doc_type, count(*) FROM cedms.microfiche.pages GROUP BY 1\"})\n",
"print(r.status_code, r.text[:500])\n"))

# ---- 3. flow
cells.append(md("## 3 · The Flow pipeline — Flow + Lakehouse together\n",
"\n",
"Doing this by hand is the demo; in production a **VForce Flow** orchestrates it. The flow below triggers on a **new sonar recording**, calls a PINGMapper processing service, then hands the imagery to the **`lakehouse` connector** to store + catalog it — and every run is visible in the Flow **Runs** view.\n",
"\n",
"This is **flows-as-code**: it lives in a git repo bound to a Flow **Project**, and `flowc` compiles it into a deployable object that runs autonomously and reports its runs back to the platform.\n"))
cells.append(code(
"flow_yaml = '''id: pingmapper-ingest\n",
"name: PINGMapper -> Lakehouse ingest\n",
"version: 1\n",
"trigger:\n",
"  kind: event              # a new sonar recording lands in object storage\n",
"  config: { source: aws-s3, subject: \"sonar/uploads\" }\n",
"entry: process\n",
"nodes:\n",
"  - id: process\n",
"    name: Run PINGMapper\n",
"    kind: action           # call the PINGMapper processing service (heavy Python/GDAL/ML)\n",
"    connector: http\n",
"    action: request\n",
"    inputs:\n",
"      method: POST\n",
"      url: \"http://pingmapper-runner/process\"\n",
"      body: { recordingKey: \"{{ trigger.data.key }}\" }\n",
"  - id: store\n",
"    name: Store imagery in Lakehouse\n",
"    kind: action\n",
"    connector: lakehouse\n",
"    action: ingestDocument\n",
"    inputs:\n",
"      batch_id: \"{{ trigger.data.key }}\"\n",
"      kind: tiff\n",
"      data: \"{{ steps.process.output.geotiffBase64 }}\"\n",
"  - id: done\n",
"    name: Done\n",
"    kind: terminate\n",
"    config: { status: succeeded }\n",
"edges:\n",
"  - { from: process, to: store }\n",
"  - { from: store, to: done }\n",
"'''\n",
"with open('flows/pingmapper-ingest.yaml', 'w') as f:\n",
"    os.makedirs('flows', exist_ok=True); f.write(flow_yaml)\n",
"print(flow_yaml)\n"))

cells.append(md("Compile the flow into a deployable object with `flowc` (any CI/CD can do this):\n"))
cells.append(code(
"# !flowc validate flows/\n",
"# !flowc build flows/ -o dist/flowpack.tar.gz --name pingmapper\n",
"print('flowc validate flows/  →  flowc build flows/ -o dist/flowpack.tar.gz')\n"))

cells.append(md("---\n",
"**Recap** — PINGMapper turned a raw sonar log into georeferenced habitat imagery (**process**); VForce Lakehouse stored + cataloged it (**store**); and a VForce Flow ties the two into one autonomous, git-sourced pipeline whose runs are observable in the platform (**orchestrate**). That's the Flow + Lakehouse pattern.\n"))

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.11"},
    },
    "nbformat": 4, "nbformat_minor": 5,
}

os.makedirs('/tmp/pingmapper-example', exist_ok=True)
with open('/tmp/pingmapper-example/PINGMapper_lakehouse_flow.ipynb', 'w') as f:
    json.dump(nb, f, indent=1)
print('notebook cells:', len(cells))
