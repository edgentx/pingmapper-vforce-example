import os, re, subprocess, glob, base64, time, uuid, hashlib, hmac, datetime, csv, io
from io import BytesIO
from typing import Optional
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import pingmapper

app = FastAPI(title="pingmapper-runner")
OUT = os.environ.get("OUT_DIR", "/data/pingmapper")
TEST_OUT = os.path.join(os.path.dirname(os.path.dirname(pingmapper.__file__)), ".data", "test_runs")

# 12 MiB per-file cap, image cap for the streaming endpoint. The cap is raised
# above the substrate-map count so the leading before/after composites never
# crowd the classified substrate maps out of the stream.
MAX_FILE_BYTES = 12 * 1024 * 1024
MAX_IMAGES = 48

# Before/after composite tuning.
COMPOSITE_CHANNELS = ("ss_port", "ss_star")
MAX_COMPOSITES_PER_CHANNEL = 4
MAX_COMPOSITES_TOTAL = 8
COMPOSITE_PANEL_HEIGHT = 480
COMPOSITE_GUTTER = 16
COMPOSITE_LABEL_STRIP = 28
COMPOSITE_BG = (11, 11, 11)  # #0b0b0b
COMPOSITE_LABELS = {
    "raw": "RAW SIDE-SCAN SONAR",
    "classified": "PINGMAPPER SUBSTRATE CLASSIFICATION",
}

CONTENT_TYPES = {
    "png": "image/png",
    "tif": "image/tiff",
    "tiff": "image/tiff",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
}


@app.get("/health")
def health():
    return {"status": "ok"}


class Req(BaseModel):
    sample: bool = True
    recordingKey: Optional[str] = None


def collect(dirs):
    out = []
    for d in dirs:
        for ext in ("png", "tif", "tiff", "jpg", "jpeg"):
            out += glob.glob(os.path.join(d, "**", "*." + ext), recursive=True)
    return sorted(set(out))


def b64(path):
    with open(path, "rb") as fh:
        return base64.b64encode(fh.read()).decode()


def content_type_for(path):
    ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
    return CONTENT_TYPES.get(ext, "application/octet-stream")


def _scaled_to_height(img, height):
    # Preserve aspect ratio, scaling to a common panel height.
    from PIL import Image
    w, h = img.size
    if h <= 0:
        return img
    new_w = max(1, int(round(w * (height / float(h)))))
    return img.resize((new_w, height), Image.LANCZOS)


def compose_before_after(raw_path, classified_path, ch, idx):
    """Build a side-by-side before/after PNG: raw side-scan sonar (left) vs.
    PINGMapper substrate classification (right). Returns PNG bytes."""
    from PIL import Image, ImageDraw, ImageFont

    raw = _scaled_to_height(Image.open(raw_path).convert("RGB"), COMPOSITE_PANEL_HEIGHT)
    cls = _scaled_to_height(Image.open(classified_path).convert("RGB"), COMPOSITE_PANEL_HEIGHT)

    strip = COMPOSITE_LABEL_STRIP
    gutter = COMPOSITE_GUTTER
    panel_h = COMPOSITE_PANEL_HEIGHT

    left_w = raw.size[0]
    right_w = cls.size[0]
    canvas_w = left_w + gutter + right_w
    canvas_h = strip + panel_h

    canvas = Image.new("RGB", (canvas_w, canvas_h), COMPOSITE_BG)
    draw = ImageDraw.Draw(canvas)

    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    def _draw_label(x0, width, text):
        # Dark label strip with white text, centered horizontally.
        draw.rectangle([x0, 0, x0 + width - 1, strip - 1], fill=COMPOSITE_BG)
        if font is not None:
            try:
                bbox = draw.textbbox((0, 0), text, font=font)
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            except Exception:
                tw, th = (len(text) * 6, 11)
            tx = x0 + max(0, (width - tw) // 2)
            ty = max(0, (strip - th) // 2)
            draw.text((tx, ty), text, fill=(255, 255, 255), font=font)

    # Paste panels beneath their label strips.
    canvas.paste(raw, (0, strip))
    canvas.paste(cls, (left_w + gutter, strip))

    _draw_label(0, left_w, COMPOSITE_LABELS["raw"])
    _draw_label(left_w + gutter, right_w, COMPOSITE_LABELS["classified"])

    # Thin divider line in the gutter between the two panels.
    div_x = left_w + gutter // 2
    draw.line([(div_x, 0), (div_x, canvas_h - 1)], fill=(80, 80, 80), width=1)

    buf = BytesIO()
    canvas.save(buf, format="PNG")
    return buf.getvalue()


_IDX_RE = re.compile(r"_(\d{5})\.")


def _idx_of(path):
    m = _IDX_RE.search(os.path.basename(path))
    return m.group(1) if m else None


def find_composite_pairs():
    """Return matched (ch, idx, raw_path, classified_path) tuples that have BOTH
    a raw side-scan src image and a PINGMapper classified substrate plot. Capped
    per-channel (lowest indices first) and overall."""
    pairs = []
    for ch in COMPOSITE_CHANNELS:
        raws = {}
        for p in glob.glob(os.path.join(TEST_OUT, "**", ch, "src", "*src_%s_*.jpg" % ch), recursive=True):
            idx = _idx_of(p)
            if idx is not None:
                raws.setdefault(idx, p)
        classified = {}
        for p in glob.glob(
            os.path.join(TEST_OUT, "**", "substrate", "**", "*pltSub_classified_max_%s_*.png" % ch),
            recursive=True,
        ):
            idx = _idx_of(p)
            if idx is not None:
                classified.setdefault(idx, p)
        matched = sorted(set(raws) & set(classified))
        for idx in matched[:MAX_COMPOSITES_PER_CHANNEL]:
            pairs.append((ch, idx, raws[idx], classified[idx]))
    return pairs[:MAX_COMPOSITES_TOTAL]


@app.post("/process")
def process(r: Optional[Req] = None):
    if r is None:
        r = Req()
    os.makedirs(OUT, exist_ok=True)
    p = subprocess.run(["python", "-m", "pingmapper", "test"], capture_output=True, text=True, cwd=OUT, timeout=5400)
    imgs = collect([OUT, TEST_OUT])
    images = []
    for f in imgs[:30]:
        try:
            images.append({"name": os.path.basename(f), "path": f, "b64": b64(f)})
        except OSError:
            pass
    # scalar primary image — the flow binding engine cannot index arrays, so the
    # store node consumes steps.process.output.imageBase64 directly.
    primary = images[0] if images else None
    return {
        "ok": p.returncode == 0 and len(imgs) > 0,
        "returncode": p.returncode,
        "imageCount": len(imgs),
        "imageBase64": primary["b64"] if primary else "",
        "imageName": primary["name"] if primary else "",
        "images": images,
        "stdout": p.stdout[-3000:],
        "stderr": p.stderr[-2000:],
    }


def _sse_event(event, data):
    # data is a pre-serialized JSON string
    return "event: %s\ndata: %s\n\n" % (event, data)


def _json_escape(s):
    # minimal JSON string escaping (avoid importing json per the dependency note;
    # but json is stdlib — using it is fine. We keep a helper for clarity.)
    import json
    return json.dumps(s)


def _stream_gen():
    os.makedirs(OUT, exist_ok=True)
    proc = None
    try:
        proc = subprocess.Popen(
            ["python", "-m", "pingmapper", "test"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=OUT,
        )

        last_emit = time.time()
        # Read line by line; emit keepalives if the stream idles > 10s.
        # We use a simple blocking readline loop; PINGMapper is chatty enough
        # that idle gaps are rare, and keepalives cover the model-download lull.
        import select
        stdout = proc.stdout
        while True:
            ready, _, _ = select.select([stdout], [], [], 1.0)
            if ready:
                line = stdout.readline()
                if line == "":
                    break  # EOF
                line = line.rstrip()
                if not line:
                    continue
                payload = '{"message": %s}' % _json_escape(line)
                yield _sse_event("progress", payload)
                last_emit = time.time()
            else:
                if time.time() - last_emit > 10:
                    yield ":keepalive\n\n"
                    last_emit = time.time()
                if proc.poll() is not None:
                    # process gone and nothing left to read
                    rest = stdout.read()
                    if rest:
                        for rl in rest.splitlines():
                            rl = rl.rstrip()
                            if rl:
                                yield _sse_event("progress", '{"message": %s}' % _json_escape(rl))
                    break

        rc = proc.wait()

        count = 0

        # ---- Before/after composites FIRST -------------------------------
        # Emit raw-vs-classified side-by-side composites ahead of the substrate
        # maps so the demo form shows the before/after pairing up front. Each
        # pair is wrapped in its own try/except so one bad image never aborts
        # the stream.
        try:
            pairs = find_composite_pairs()
        except Exception as e:  # noqa: BLE001
            pairs = []
            yield _sse_event("progress", '{"message": %s}' % _json_escape("could not enumerate composite pairs: %s" % e))
        for ch, idx, raw_path, cls_path in pairs:
            name = "compare_%s_%s.png" % (ch, idx)
            try:
                png_bytes = compose_before_after(raw_path, cls_path, ch, idx)
                encoded = base64.b64encode(png_bytes).decode()
            except Exception as e:  # noqa: BLE001
                yield _sse_event("progress", '{"message": %s}' % _json_escape("could not compose %s: %s" % (name, e)))
                continue
            payload = '{"name": %s, "contentType": "image/png", "b64": %s}' % (
                _json_escape(name),
                _json_escape(encoded),
            )
            yield _sse_event("image", payload)
            count += 1

        imgs = collect([OUT, TEST_OUT])

        # Surface substrate-segmentation maps first so they are not truncated by
        # the MAX_IMAGES cap. Substrate classified maps/rasters are the headline
        # deep-learning deliverable; bedpicks/water-column come after.
        def _rank(path):
            low = path.lower()
            if "pltsub_classified" in low or "map_substrate" in low:
                return 0  # classified benthic substrate maps + rasters/mosaics
            if "substrate" in low and "probability" not in low:
                return 1  # other substrate outputs
            if "bedpick" in low:
                return 3
            return 2
        imgs = sorted(imgs, key=lambda f: (_rank(f), f))

        for f in imgs:
            if count >= MAX_IMAGES:
                break
            low = f.lower()
            if "model" in low or "trainhist" in low:
                continue
            # Skip oversized probability heatmaps / raw softmax tensors; they blow
            # the per-file cap and crowd out the classified substrate maps.
            if "pltsub_probability" in low or low.endswith(".npz"):
                continue
            try:
                if os.path.getsize(f) > MAX_FILE_BYTES:
                    continue
                encoded = b64(f)
            except OSError:
                continue
            payload = '{"name": %s, "contentType": %s, "b64": %s}' % (
                _json_escape(os.path.basename(f)),
                _json_escape(content_type_for(f)),
                _json_escape(encoded),
            )
            yield _sse_event("image", payload)
            count += 1

        yield _sse_event("done", '{"count": %d, "returncode": %d}' % (count, rc))
    except Exception as e:  # noqa: BLE001
        try:
            if proc is not None and proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        yield _sse_event("error", '{"message": %s}' % _json_escape(str(e)))


@app.get("/process/stream")
def process_stream():
    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return StreamingResponse(_stream_gen(), media_type="text/event-stream", headers=headers)


# --------------------------------------------------------------------------- #
# Ingest-by-reference: run PINGMapper, upload result images to MinIO, return a
# manifest of object keys (no inline bytes). The flow's store node consumes the
# manifest's keys instead of base64 payloads.
# --------------------------------------------------------------------------- #

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://192.168.0.118:32557").rstrip("/")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")
INGEST_BUCKET = os.environ.get("INGEST_BUCKET", "uploads")

# Resolve an S3 client backend at import time: prefer boto3, then minio, else a
# stdlib SigV4 signer over `requests`. Whichever is present in the conda env is
# used; nothing is installed.
_S3_BACKEND = None
try:
    import boto3  # noqa: F401
    _S3_BACKEND = "boto3"
except Exception:
    try:
        import minio  # noqa: F401
        _S3_BACKEND = "minio"
    except Exception:
        try:
            import requests  # noqa: F401
            _S3_BACKEND = "sigv4"
        except Exception:
            _S3_BACKEND = None


class IngestReq(BaseModel):
    sample: bool = True
    run_id: Optional[str] = None
    recordingKey: Optional[str] = None


# ---- SigV4 (stdlib + requests) fallback ----------------------------------- #
def _sig_key(key, date_stamp, region, service):
    def _h(k, m):
        return hmac.new(k, m.encode("utf-8"), hashlib.sha256).digest()
    k_date = _h(("AWS4" + key).encode("utf-8"), date_stamp)
    k_region = _h(k_date, region)
    k_service = _h(k_region, service)
    return _h(k_service, "aws4_request")


def _sigv4_request(method, bucket, key, body=b"", content_type=None):
    """Path-style SigV4-signed request to MinIO. Returns a requests.Response."""
    import requests
    from urllib.parse import urlparse, quote

    parsed = urlparse(MINIO_ENDPOINT)
    host = parsed.netloc
    path = "/" + bucket
    if key:
        path += "/" + quote(key)
    url = MINIO_ENDPOINT + path

    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = path
    canonical_qs = ""
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if content_type:
        headers["content-type"] = content_type
    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join("%s:%s\n" % (k, headers[k]) for k in sorted(headers))
    canonical_request = "\n".join(
        [method, canonical_uri, canonical_qs, canonical_headers, signed_headers, payload_hash]
    )

    scope = "%s/%s/s3/aws4_request" % (date_stamp, MINIO_REGION)
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _sig_key(MINIO_SECRET_KEY, date_stamp, MINIO_REGION, "s3")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    auth = (
        "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
        % (MINIO_ACCESS_KEY, scope, signed_headers, signature)
    )

    req_headers = {
        "Authorization": auth,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    if content_type:
        req_headers["Content-Type"] = content_type
    # Short timeout (connect, read): image staging is best-effort, so a slow or
    # unreachable MinIO must fail fast instead of blocking /process/ingest for
    # 60s per file (× dozens of outputs = a multi-minute hang that times out the
    # calling flow). See _emit's best-effort handling + staging short-circuit.
    return requests.request(method, url, headers=req_headers, data=body, timeout=(3, 5))


# ---- bucket + upload, backend-agnostic ------------------------------------ #
def _ensure_bucket():
    if _S3_BACKEND == "boto3":
        import boto3
        from botocore.client import Config
        c = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        try:
            c.head_bucket(Bucket=INGEST_BUCKET)
        except Exception:
            try:
                c.create_bucket(Bucket=INGEST_BUCKET)
            except Exception:
                pass
        return c
    if _S3_BACKEND == "minio":
        from minio import Minio
        from urllib.parse import urlparse
        parsed = urlparse(MINIO_ENDPOINT)
        secure = parsed.scheme == "https"
        c = Minio(parsed.netloc, access_key=MINIO_ACCESS_KEY,
                  secret_key=MINIO_SECRET_KEY, secure=secure)
        try:
            if not c.bucket_exists(INGEST_BUCKET):
                c.make_bucket(INGEST_BUCKET)
        except Exception:
            pass
        return c
    # sigv4: PUT the bucket (idempotent; 200 create, 409 already-owned both fine)
    try:
        _sigv4_request("PUT", INGEST_BUCKET, "")
    except Exception:
        pass
    return None


def _upload(client, key, body, content_type):
    if _S3_BACKEND == "boto3":
        client.put_object(Bucket=INGEST_BUCKET, Key=key, Body=body, ContentType=content_type)
        return
    if _S3_BACKEND == "minio":
        client.put_object(INGEST_BUCKET, key, BytesIO(body), length=len(body), content_type=content_type)
        return
    resp = _sigv4_request("PUT", INGEST_BUCKET, key, body=body, content_type=content_type)
    if resp.status_code not in (200, 201):
        raise RuntimeError("upload failed %s: %s" % (resp.status_code, resp.text[:200]))


_CHANNEL_RE = re.compile(r"(ss_port|ss_star)")


def _classify(name, content_type):
    """kind/channel/idx classification driven purely by the file name + type."""
    low = name.lower()
    if low.startswith("compare_"):
        kind = "composite"
    elif "map_substrate" in low or "pltsub" in low:
        kind = "substrate_map"
    elif content_type == "image/tiff":
        kind = "geotiff"
    elif low.startswith(("src_", "wcp_", "wco_", "wcm_")):
        kind = "sonogram"
    elif "bedpick" in low:
        kind = "bedpick"
    else:
        kind = "other"
    cm = _CHANNEL_RE.search(low)
    channel = cm.group(1) if cm else None
    idx = _idx_of(name)
    return kind, channel, idx


@app.post("/process/ingest")
def process_ingest(r: Optional[IngestReq] = None):
    if r is None:
        r = IngestReq()
    run_id = (r.run_id or "").strip() or uuid.uuid4().hex
    prefix = "pingmapper/%s/" % run_id

    os.makedirs(OUT, exist_ok=True)
    p = subprocess.run(
        ["python", "-m", "pingmapper", "test"],
        capture_output=True, text=True, cwd=OUT, timeout=5400,
    )

    # Same result set the stream emits: composites first, then ranked substrate
    # maps / geotiffs / sonograms — excluding model + trainhist artifacts.
    if _S3_BACKEND is None:
        return {
            "ok": False,
            "error": "no S3 client backend available (boto3/minio/requests)",
            "run_id": run_id,
            "returncode": p.returncode,
        }

    client = _ensure_bucket()

    outputs = []
    seen_names = set()
    # Best-effort staging state: once an upload fails (e.g. MinIO unreachable),
    # stop attempting the rest so the whole batch fails fast rather than paying
    # the per-file timeout N times. The manifest/CSV is still produced either
    # way — the object bytes are a bonus, not a prerequisite for the run to
    # succeed. staged=False marks a manifest whose bytes did not land.
    staging = {"down": False, "any_fail": False}

    def _emit(name, body, content_type):
        if name in seen_names:
            return
        key = prefix + name
        staged = False
        if not staging["down"]:
            try:
                _upload(client, key, body, content_type)
                staged = True
            except Exception:
                # First failure short-circuits the remaining uploads: an
                # unreachable/slow store won't get healthier mid-batch, and the
                # run must still return promptly with a complete manifest.
                staging["down"] = True
                staging["any_fail"] = True
        kind, channel, idx = _classify(name, content_type)
        outputs.append({
            "name": name, "kind": kind, "channel": channel,
            "idx": idx, "contentType": content_type, "key": key,
            "staged": staged,
        })
        seen_names.add(name)

    # ---- Before/after composites FIRST (generated, not on disk) ---------- #
    try:
        pairs = find_composite_pairs()
    except Exception:
        pairs = []
    for ch, idx, raw_path, cls_path in pairs:
        name = "compare_%s_%s.png" % (ch, idx)
        try:
            png_bytes = compose_before_after(raw_path, cls_path, ch, idx)
            _emit(name, png_bytes, "image/png")
        except Exception:
            continue

    # ---- On-disk result images, ranked like the stream ------------------- #
    imgs = collect([OUT, TEST_OUT])

    def _rank(path):
        low = path.lower()
        if "pltsub_classified" in low or "map_substrate" in low:
            return 0
        if "substrate" in low and "probability" not in low:
            return 1
        if "bedpick" in low:
            return 3
        return 2
    imgs = sorted(imgs, key=lambda f: (_rank(f), f))

    for f in imgs:
        if len(outputs) >= MAX_IMAGES:
            break
        low = f.lower()
        if "model" in low or "trainhist" in low:
            continue
        if "pltsub_probability" in low or low.endswith(".npz"):
            continue
        try:
            if os.path.getsize(f) > MAX_FILE_BYTES:
                continue
            with open(f, "rb") as fh:
                body = fh.read()
        except OSError:
            continue
        try:
            _emit(os.path.basename(f), body, content_type_for(f))
        except Exception:
            continue

    counts = {
        "total": len(outputs),
        "composites": sum(1 for o in outputs if o["kind"] == "composite"),
        "substrate_maps": sum(1 for o in outputs if o["kind"] == "substrate_map"),
        "geotiffs": sum(1 for o in outputs if o["kind"] == "geotiff"),
    }

    # ---- CSV view of the manifest, 1 row per output (lakehouse ingest/data) -- #
    # Header is fixed; values CSV-escaped via stdlib csv into a StringIO so a
    # downstream lakehouse ingest/data lands one governed row per output doc.
    processed_at = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    _csv_buf = io.StringIO()
    _csv_w = csv.writer(_csv_buf)
    _csv_w.writerow([
        "run_id", "name", "kind", "channel", "idx",
        "content_type", "object_bucket", "object_key", "processed_at",
    ])
    for o in outputs:
        _csv_w.writerow([
            run_id,
            o["name"],
            o["kind"],
            o["channel"] if o["channel"] is not None else "",
            o["idx"] if o["idx"] is not None else "",
            o["contentType"],
            "uploads",
            o["key"],
            processed_at,
        ])
    csv_str = _csv_buf.getvalue()

    return {
        "run_id": run_id,
        "bucket": INGEST_BUCKET,
        "prefix": prefix,
        "outputs": outputs,
        "counts": counts,
        "csv": csv_str,
        "returncode": p.returncode,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
