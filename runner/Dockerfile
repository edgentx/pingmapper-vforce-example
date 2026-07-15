FROM pingmapper-runner:latest
USER root
# System X11/Tk runtime libs so matplotlib/tkinter import does not hard-fail
# (doodleverse_utils.prediction_imports pulls in tkinter via matplotlib).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libx11-6 libxext6 libxrender1 libsm6 \
 && rm -rf /var/lib/apt/lists/*
# Segmentation ML deps (CPU TensorFlow; segformer substrate models need transformers).
RUN /opt/conda/bin/pip install --no-cache-dir \
      "tensorflow-cpu>=2.20,<3" \
      "tf-keras>=2.20,<3" \
      "transformers>=4.57,<5" \
      "doodleverse_utils>=0.0.39"
# Durable runtime shims (Agg backend + matplotlib.cm.get_cmap restore).
COPY sitecustomize.py /opt/conda/lib/python3.11/site-packages/sitecustomize.py
ENV MPLBACKEND=Agg
# Verify the whole segmentation import chain at build time.
RUN /opt/conda/bin/python -c "import doodleverse_utils.prediction_imports as p; import tensorflow as tf; import transformers; import matplotlib.cm as cm; assert hasattr(cm, \"get_cmap\"), \"get_cmap shim missing\"; print(\"OK TF\", tf.__version__, \"transformers\", transformers.__version__, \"get_cmap restored\")"
USER mambauser
