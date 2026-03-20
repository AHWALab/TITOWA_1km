import os
import shutil
from datetime import datetime as dt
from datetime import timedelta
from .gfs_downloader import download_GFS
import glob

def GFS_searcher(path_gfs, qpf_store_path, start_time, end_time, xmin, xmax, ymin, ymax):
    """
    Always download fresh GFS data for the requested window via the Herbie-backed
    download_GFS function, which automatically selects the latest available GFS cycle
    and falls back to previous cycles if needed.

    Downloaded files are written to qpf_store_path/gfs_data/ for EF5 to read, and
    a copy is kept in path_gfs (e.g. precip/GFS/) as a persistent archive.

    Parameters
    ----------
    path_gfs : str
        Persistent storage folder for GFS tif files (archive copy target).
        Recommended: "precip/GFS/"
    qpf_store_path : str
        EF5 working QPF folder; files are written here as qpf_store_path/gfs_data/.
    start_time : datetime
        Start time of requested data.
    end_time : datetime
        End time of requested data.
    xmin, xmax, ymin, ymax : float
        Spatial domain for clipping.
    """

    # EF5 working folder — cleared each cycle so stale data never accumulates
    download_folder = os.path.join(qpf_store_path, "gfs_data/")
    os.makedirs(download_folder, exist_ok=True)
    os.makedirs(path_gfs, exist_ok=True)

    for f in glob.glob(os.path.join(download_folder, "*.tif")):
        try:
            os.remove(f)
        except Exception:
            pass

    # Always download fresh — download_GFS picks the latest released GFS cycle
    # and falls back to previous cycles automatically if a cycle isn't ready yet.
    print(f"Downloading fresh GFS data from {start_time} to {end_time}...")
    result = download_GFS(start_time, end_time, xmin, xmax, ymin, ymax, download_folder)
    num_written = len(result) if result else 0
    print(f"GFS download completed. Files written: {num_written}")

    if num_written == 0:
        raise RuntimeError("No GFS data available after downloader fallback attempts.")

    # Archive a copy to path_gfs so future diagnostic/hindcast runs can reuse them
    for f in result:
        dest = os.path.join(path_gfs, os.path.basename(f))
        try:
            shutil.copy2(f, dest)
        except Exception as e:
            print(f"Warning: could not archive {os.path.basename(f)} to {path_gfs}: {e}")
        