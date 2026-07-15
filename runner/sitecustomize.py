# Durable runtime shims for PINGMapper segmentation in the headless runner.
import os
# Force a non-interactive matplotlib backend so importing matplotlib/pyplot
# never pulls in tkinter (which needs libX11) and never needs a display.
os.environ.setdefault("MPLBACKEND", "Agg")

# matplotlib >=3.9 removed matplotlib.cm.get_cmap; PINGMapper (class_sonObj.py,
# class_mapSubstrateObj.py) still calls plt.cm.get_cmap(name). Restore it as a
# thin alias over the modern matplotlib.colormaps registry.
def _install_get_cmap_shim():
    try:
        import matplotlib
        import matplotlib.cm as _cm
        if not hasattr(_cm, "get_cmap"):
            import matplotlib as _mpl
            def get_cmap(name=None, lut=None):
                if name is None:
                    name = _mpl.rcParams["image.cmap"]
                if isinstance(name, _mpl.colors.Colormap):
                    return name
                cmap = _mpl.colormaps[name]
                if lut is not None:
                    cmap = cmap.resampled(lut)
                return cmap
            _cm.get_cmap = get_cmap
    except Exception:
        pass

_install_get_cmap_shim()
