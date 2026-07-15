# PINGMapper runner

The HTTP service the Flow calls to run PINGMapper. Wraps the `pingmapper`
Python package behind FastAPI and exposes:

| Route | Purpose |
|-------|---------|
| `GET /health` | liveness (`{"status":"ok"}`) |
| `POST /process` | run the pipeline, return images inline (base64) |
| `GET /process/stream` | run with a live `text/event-stream` of console + image progress |
| `POST /process/ingest` | run **and stage outputs by reference** — returns `{run_id, bucket, prefix, outputs[], csv, returncode}`; `csv` is one row per output, ready for a Lakehouse `ingestData` into `main.sonar.documents` |

`POST /process/ingest` is what the demo flow (`flows/pingmapper-ingest.yaml`) uses:
the inline `csv` feeds the Lakehouse store node.

## Deployment (host 192.168.0.148, microk8s cluster, namespace `pixelrag`)

This runner is **not** deployed by the Flow platform — it lives on the .148 GPU
host's own microk8s cluster. `server.py` is delivered via a configMap
(`pingmapper-server`) mounted at `/app/server.py`.

```
# server code (configMap)
microk8s kubectl -n pixelrag create configmap pingmapper-server \
  --from-file=server.py=runner/server.py \
  --dry-run=client -o yaml | microk8s kubectl apply -f -

# images (built with docker, imported into microk8s containerd)
docker build -t pingmapper-runner:tf     -f runner/Dockerfile     runner/   # CPU
docker build -t pingmapper-runner:tf-gpu -f runner/Dockerfile.gpu runner/   # GPU
docker save pingmapper-runner:tf-gpu | microk8s ctr images import -

# deployments + service
microk8s kubectl apply -f runner/k8s/runner-blue.yaml   # CPU (pingmapper-runner)
microk8s kubectl apply -f runner/k8s/runner-gpu.yaml    # GPU (pingmapper-runner-gpu, :30821)
```

Both Dockerfiles build `FROM pingmapper-runner:latest` (the base image carrying
the `pingmapper` package + FastAPI). The `:tf` / `:tf-gpu` layers add the
substrate-segmentation ML stack (`transformers`, `tf-keras`, `doodleverse_utils`)
on CPU or GPU TensorFlow respectively.

The stable service `pingmapper-runner` (NodePort **30820**) is what the Flow's
connection points at (`http://192.168.0.148:30820`). Blue-green cutover between
CPU and GPU is a selector flip on that service:

```
microk8s kubectl -n pixelrag patch svc pingmapper-runner \
  -p '{"spec":{"selector":{"app":"pingmapper-runner-gpu"}}}'   # -> GPU
# rollback: set the selector back to app: pingmapper-runner
```

## GPU

`Dockerfile.gpu` installs `tensorflow[and-cuda]` (bundled CUDA 12.x wheels) and
runs `ldconfig` over the pip-bundled `nvidia/*/lib` dirs — **without that step
TensorFlow logs "Cannot dlopen some GPU libraries" and silently falls back to
CPU** (`tf.config.list_physical_devices('GPU') == []`). The deployment requests
`nvidia.com/gpu: 1`.

Both physical GPUs on .148 are held by the always-on `llama` + `vl-embed` pods,
so the runner shares a card via **time-slicing** (`k8s/gpu-time-slicing.yaml`,
`replicas: 2` → 4 schedulable slots). Apply it and point the GPU-operator
ClusterPolicy at it:

```
microk8s kubectl apply -f runner/k8s/gpu-time-slicing.yaml
microk8s kubectl patch clusterpolicy cluster-policy --type merge \
  -p '{"spec":{"devicePlugin":{"config":{"name":"time-slicing-config","default":"any"}}}}'
```

**Note:** PINGMapper's built-in sample is *not* GPU-bound — its ~2-minute
runtime is CPU/georectification work, and the GPU stays idle. The GPU variant
exists for heavier real recordings where substrate segmentation dominates.

## Output staging + the MinIO endpoint

`POST /process/ingest` stages each output image to an S3/MinIO bucket and returns
a manifest by reference. Staging is **best-effort**: uploads use a short timeout
and short-circuit after the first failure, and every output is recorded in the
manifest/CSV **whether or not its bytes uploaded** (`"staged": true|false` per
output). So a run always returns promptly with a complete CSV — the Lakehouse
rows are the metadata manifest; the object bytes are a bonus.

This matters because the default `MINIO_ENDPOINT` (`192.168.0.118:32557`) was
decommissioned by the shared-MinIO consolidation; before the best-effort change,
each of ~48 uploads blocked up to 60s against the dead endpoint (~30 min total),
which timed out the calling Flow run. To actually stage bytes again, set:

| Env | Notes |
|-----|-------|
| `MINIO_ENDPOINT` | a **reachable** endpoint. The consolidated `minio` namespace is ClusterIP-only (not reachable from the .148 cluster); a reachable NodePort is `http://192.168.0.118:30902`. |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | per-app credentials for that store. |
| `INGEST_BUCKET` | destination bucket (default `uploads`). |
