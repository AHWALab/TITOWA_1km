import os            
import re
import shutil        
from datetime import datetime, timedelta, timezone  
from tito_utils.file_utils.datetime_utils import get_geotiff_datetime


def _get_hsaf_datetime(filename):
    """Extract datetime from an HSAF filename like h40_YYYYMMDD_HHMM_fdk.tif."""
    m = re.match(r"h40_(\d{8})_(\d{4})_fdk\.tif$", filename)
    if m:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")
    return None

def cleanup_precip(current_datetime, precipFolder, qpf_store_path):
    """Function that cleans up the precip folder for the current EF5 run

    Arguments:
        current_datetime {datetime} -- datetime object for the current time step
        failTime {datetime} -- datetime object representing the maximum datetime in the past
        precipFolder {str} -- path to the geotiff precipitation folder
        qpf_store_path {str} -- path to the folder where QPF files are stored
    """
    # Normalize timezone handling: compare naive UTC datetimes to avoid
    # "can't compare offset-naive and offset-aware datetimes" errors.
    def _to_naive_utc(dt):
        try:
            if getattr(dt, "tzinfo", None) is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            # If anything unexpected, fall back to original value
            return dt

    current_naive_utc = _to_naive_utc(current_datetime)

    qpes = []
    qpfs = []
    older_QPE = current_naive_utc - timedelta(hours=9.5)
    imerg_Latency = current_naive_utc - timedelta(hours=4)
    
    try:
        # List all precip files
        precip_files = os.listdir(precipFolder)

        # Segregate between QPEs and QPFs
        for file in precip_files:
            if "qpe" in file:
                qpes.append(file)
            elif "qpf" in file:
                qpfs.append(file)

        print("    Deleting all QPE files older than Fail Time: ", older_QPE)
        for qpe in qpes:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpe)
                if geotiff_datetime < older_QPE:
                    os.remove(precipFolder + qpe)
            except Exception as e:
                print(f"Error processing QPE file {qpe}: {e}")

        print("    Deleting all QPF files older than Current Time: ", current_naive_utc)
        print("    Copying all QPF files older than Current Time: ", current_naive_utc, " into qpf_store folder.")
        for qpf in qpfs:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpf)
                if geotiff_datetime < current_naive_utc:
                    shutil.copy2(precipFolder + qpf, qpf_store_path)
                os.remove(precipFolder + qpf)
            except Exception as e:
                print(f"Error processing QPF file {qpf}: {e}")

        print(f"    Deleting all QPE files newer than Imerg Latency Time: {imerg_Latency} because it might be duplicated files")
        for qpedup in qpes:
            try:
                geotiff_datetime = get_geotiff_datetime(precipFolder + qpedup)
                if geotiff_datetime > current_naive_utc - timedelta(hours=4):
                    os.remove(precipFolder + qpedup)
            except Exception as e:
                print(f"Error processing QPE duplicate file {qpedup}: {e}")

        print(f"    Deleting all QPF files in store folder older than: {imerg_Latency}")
        qpf_stored_files = os.listdir(qpf_store_path)
        qpf_stored_files = [f for f in qpf_stored_files if f.endswith('.tif')]
        max_qpf = current_naive_utc - timedelta(hours=4)
        for qpf_stored in qpf_stored_files:
            try:
                qpf_datetime = get_geotiff_datetime(qpf_store_path + qpf_stored)
                if qpf_datetime < max_qpf:
                    os.remove(qpf_store_path + qpf_stored)
            except Exception as e:
                print(f"Error processing stored QPF file {qpf_stored}: {e}")
        # --- HSAF file cleanup (h40_*_fdk.tif in precipFolder and _hsaf_raw/) ---
        hsaf_raw_dir = os.path.join(precipFolder, "_hsaf_raw")
        for search_dir in [precipFolder, hsaf_raw_dir]:
            if not os.path.isdir(search_dir):
                continue
            for fname in os.listdir(search_dir):
                if not fname.startswith("h40_") or not fname.endswith(".tif"):
                    continue
                fdt = _get_hsaf_datetime(fname)
                if fdt is not None and fdt < older_QPE:
                    try:
                        os.remove(os.path.join(search_dir, fname))
                        print(f"    Deleted old HSAF file: {fname}")
                    except Exception as e:
                        print(f"Error deleting HSAF file {fname}: {e}")

    except Exception as e:
        print(f"General error in cleanup_precip function: {e}")
