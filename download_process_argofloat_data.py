"""
===========================================================
ARGO FLOAT DATA PROCESSING PIPELINE
===========================================================

Author: Md. Zuhaib Kabir

DESCRIPTION:
This script downloads, filters, processes, and combines 
Argo float oceanographic data into a single NetCDF file.

Main Steps:
1. Mount Google Drive (Colab)
2. Download Argo index file
3. Filter data (region + time)
4. Download .nc files in batches
5. Zip files for storage
6. Reprocess and merge into a clean dataset
7. Apply QC filtering and interpolation
8. Save final gridded NetCDF file

Output:
- Zipped raw data
- Cleaned + gridded NetCDF dataset
"""



# 0. MOUNT GOOGLE DRIVE
# Make sure you mount Google Drive in Colab so that the .nc files are downloaded to your specified folder.
from google.colab import drive
drive.mount('/content/drive')



# 1. USER INPUT SETTINGS

import os, pandas as pd
# Study Area (replace with your study area)
LAT_MIN, LAT_MAX = x, y
LON_MIN, LON_MAX = x, y

# Time Range (replace with your study time)
# will be auto-fixed to actual availability
DATE_START_REQ = "yyyy-mm-dd"
DATE_END_REQ   = "yyyy-mm-dd"

# Output folder
ZIP_OUT = "YourPath/argo_zip_batches"
os.makedirs(ZIP_OUT, exist_ok=True)

print("Region:", LAT_MIN, LAT_MAX, "|", LON_MIN, LON_MAX)
print("Time:", DATE_START_REQ, "to", DATE_END_REQ)




# 2. DOWNLOAD ARGO INDEX FILE

import requests

INDEX_URL = "https://data-argo.ifremer.fr/ar_index_global_prof.txt"
INDEX_LOCAL = "/content/ar_index_global_prof.txt"

if not os.path.exists(INDEX_LOCAL) or os.path.getsize(INDEX_LOCAL) < 1000000:
    r = requests.get(INDEX_URL, stream=True, timeout=60)
    r.raise_for_status()
    with open(INDEX_LOCAL, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)

print("Index saved:", INDEX_LOCAL, "size:", os.path.getsize(INDEX_LOCAL))

df = pd.read_csv(INDEX_LOCAL, comment="#", header=None, low_memory=False)

base_cols = ["file","date","latitude","longitude","ocean","profiler_type","institution","date_update"]
df.columns = base_cols[:df.shape[1]] + [f"extra_{i}" for i in range(df.shape[1]-len(base_cols))]

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df = df.dropna(subset=["file","date","latitude","longitude"])

print("Total index rows:", len(df))




#3. To determine the available date from Argo at those locations

print("Earliest date:", df["date"].min())
print("Latest date:", df["date"].max())
print(df[["file","date","latitude","longitude"]].head(3))




#4 Check available date range, adjust time window, filter by lat/lon/time, select unique .nc files, and verify ranges

t0_req = pd.to_datetime(DATE_START_REQ)
t1_req = pd.to_datetime(DATE_END_REQ)

earliest_available = df["date"].min()
latest_available   = df["date"].max()

# Auto-fix time window to what Argo actually has
t0 = max(t0_req, earliest_available)
t1 = min(t1_req, latest_available)

print("Earliest available in index:", earliest_available)
print("Latest available in index:", latest_available)
print("Using time window:", t0, "to", t1)

m = (
    (df["latitude"] >= LAT_MIN) & (df["latitude"] <= LAT_MAX) &
    (df["longitude"] >= LON_MIN) & (df["longitude"] <= LON_MAX) &
    (df["date"] >= t0) & (df["date"] <= t1) &
    (df["file"].str.endswith(".nc"))
)

sub = df.loc[m, ["file","date","latitude","longitude"]].drop_duplicates("file").reset_index(drop=True)

print("\nFiltered files (sub rows):", len(sub))
print(sub.head())

print("\nCHECK RANGES:")
print("Lat min/max:", sub["latitude"].min(), sub["latitude"].max())
print("Lon min/max:", sub["longitude"].min(), sub["longitude"].max())
print("Date min/max:", sub["date"].min(), sub["date"].max())




#5. Download the files to the specified path

import math, zipfile, requests
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://data-argo.ifremer.fr/dac/"   #  correct (dac/ is required)
TMP_DIR = "/content/argo_tmp_batch"
os.makedirs(TMP_DIR, exist_ok=True)

paths = sub["file"].tolist()
print("Total files to download:", len(paths))

BATCH = 1000   # safe; you can try 2000 later
num_batches = math.ceil(len(paths) / BATCH)
print("Batches:", num_batches, "Batch size:", BATCH)

session = requests.Session()
retries = Retry(
    total=8,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))

FAILED_LOG = os.path.join(ZIP_OUT, "failed.log")

def clear_tmp():
    for f in os.listdir(TMP_DIR):
        fp = os.path.join(TMP_DIR, f)
        if os.path.isfile(fp):
            os.remove(fp)

def download_to_tmp(rel_path):
    url = BASE_URL + rel_path
    fname = os.path.basename(rel_path)
    out_path = os.path.join(TMP_DIR, fname)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return

    with session.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        tmp_part = out_path + ".part"
        with open(tmp_part, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        os.replace(tmp_part, out_path)

for bi in range(num_batches):
    start = bi * BATCH
    end = min((bi + 1) * BATCH, len(paths))
    batch_paths = paths[start:end]

    zip_name = os.path.join(ZIP_OUT, f"argo_profiles_{start+1:06d}_{end:06d}.zip")

    # Resume: if zip exists, skip
    if os.path.exists(zip_name) and os.path.getsize(zip_name) > 0:
        print("Skip existing:", os.path.basename(zip_name))
        continue

    clear_tmp()

    failed = 0
    for p in tqdm(batch_paths, desc=f"Batch {bi+1}/{num_batches}"):
        try:
            download_to_tmp(p)
        except Exception as e:
            failed += 1
            with open(FAILED_LOG, "a") as log:
                log.write(f"{p}\t{BASE_URL+p}\t{repr(e)}\n")

    with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in os.listdir(TMP_DIR):
            fp = os.path.join(TMP_DIR, f)
            if f.endswith(".nc") and os.path.getsize(fp) > 0:
                z.write(fp, arcname=f)

    print(f"✅ Saved: {os.path.basename(zip_name)} | files={len([f for f in os.listdir(TMP_DIR) if f.endswith('.nc')])} | failed={failed}")

print("\nDONE. ZIP folder:", ZIP_OUT)
print("If any failed, see:", FAILED_LOG)




#6. setup and file-checking script for processing 

import os
import glob

ZIP_DIR = "YourPath/argo_zip_batches"

# Output directory for processed data
OUT_DIR = "YourPath/Practice"
os.makedirs(OUT_DIR, exist_ok=True)

# Final NetCDF output file
OUT_NC = os.path.join(OUT_DIR, "argo_SO_profiles_cleaned_gridded.nc")

# Temporary folder for extracting ZIP files
TMP_EXTRACT = "/content/argo_extract_tmp"
os.makedirs(TMP_EXTRACT, exist_ok=True)

# Get list of all ZIP files
zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

# Display info
print("Total ZIP files found:", len(zip_files))

if len(zip_files) == 0:
    print("⚠️ No ZIP files found! Check your folder path.")
else:
    print("Sample files:")
    for f in zip_files[:3]:
        print("-", os.path.basename(f))

print("Output file will be saved at:", OUT_NC)




#7. to know the number of .nc files in zip's

import numpy as np

# Pressure grid (dbar): choose what you want
# Common: 0–2000 dbar every 10 dbar
P_MIN, P_MAX, P_STEP = 0.0, 2000.0, 10.0
PGRID = np.arange(P_MIN, P_MAX + P_STEP, P_STEP).astype("f4")
NLEV = len(PGRID)

print("Pressure grid levels:", NLEV, "from", PGRID[0], "to", PGRID[-1])




#8. data audit / schema inspection step in an Argo float data processing pipeline

zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))
print("ZIP files:", len(zip_files))

rows = []
for zpath in zip_files:
    zsize_mb = os.path.getsize(zpath) / (1024*1024)
    with zipfile.ZipFile(zpath, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".nc")]
        rows.append({
            "zip_name": os.path.basename(zpath),
            "zip_size_mb": round(zsize_mb, 3),
            "nc_count": len(names),
            "first_nc": names[0] if names else ""
        })

inv = pd.DataFrame(rows)
inv_path = os.path.join(OUT_DIR, "zip_inventory.csv")
inv.to_csv(inv_path, index=False)

print("Saved:", inv_path)
print(inv.head(10))
print("Total nc in zips (sum):", inv["nc_count"].sum())




#9. for final process, NetCDF schema inspection + quality audit step for Argo float data (re-check)

!pip -q install netCDF4
import os, glob, zipfile, random, pandas as pd
from netCDF4 import Dataset

ZIP_DIR = "YourPath/argo_zip_batches"
OUT_DIR = "/YourPath/Practice"
TMP_EXTRACT = "/content/audit_tmp"
os.makedirs(TMP_EXTRACT, exist_ok=True)

zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

SAMPLE_PER_ZIP = 2   # keep small; increase to 5 if you want
records = []

def safe_dim_sizes(ds):
    return {k: ds.dimensions[k].size for k in ds.dimensions.keys()}

for zpath in zip_files:
    # clear tmp
    for f in os.listdir(TMP_EXTRACT):
        fp = os.path.join(TMP_EXTRACT, f)
        if os.path.isfile(fp):
            os.remove(fp)

    with zipfile.ZipFile(zpath, "r") as z:
        nc_names = [n for n in z.namelist() if n.lower().endswith(".nc")]
        if not nc_names:
            continue

        pick = random.sample(nc_names, min(SAMPLE_PER_ZIP, len(nc_names)))
        z.extractall(TMP_EXTRACT, members=pick)

    for n in pick:
        fpath = os.path.join(TMP_EXTRACT, os.path.basename(n))
        try:
            ds = Dataset(fpath, "r")
            vars_ = set(ds.variables.keys())
            dims_ = safe_dim_sizes(ds)

            records.append({
                "zip": os.path.basename(zpath),
                "file": os.path.basename(n),
                "has_PRES": "PRES" in vars_,
                "has_TEMP": "TEMP" in vars_,
                "has_PSAL": "PSAL" in vars_,
                "has_PRES_ADJ": "PRES_ADJUSTED" in vars_,
                "has_TEMP_ADJ": "TEMP_ADJUSTED" in vars_,
                "has_PSAL_ADJ": "PSAL_ADJUSTED" in vars_,
                "has_TEMP_QC": "TEMP_QC" in vars_,
                "has_PSAL_QC": "PSAL_QC" in vars_,
                "dims": str(dims_)[:250]
            })
            ds.close()
        except Exception as e:
            records.append({
                "zip": os.path.basename(zpath),
                "file": os.path.basename(n),
                "error": repr(e)
            })

audit = pd.DataFrame(records)
audit_path = os.path.join(OUT_DIR, "nc_schema_sample.csv")
audit.to_csv(audit_path, index=False)

print("Saved:", audit_path)
print(audit.head(15))

# quick frequency summary
for col in ["has_PRES","has_TEMP","has_PSAL","has_TEMP_ADJ","has_PSAL_ADJ","has_TEMP_QC","has_PSAL_QC"]:
    if col in audit.columns:
        print(col, "=", audit[col].value_counts(dropna=False).to_dict())





#10. re-check 

import os, glob

ZIP_DIR = "YourPath/argo_zip_batches"   
OUT_DIR = "YourPath/Practice"                    

os.makedirs(OUT_DIR, exist_ok=True)

OUT_NC = os.path.join(OUT_DIR, "argo_combined.nc")
FAILED_LOG = os.path.join(OUT_DIR, "process_failed.log")
PROGRESS_LOG = os.path.join(OUT_DIR, "processed_zips.txt")

TMP_EXTRACT = "/content/argo_extract_tmp"
os.makedirs(TMP_EXTRACT, exist_ok=True)

zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

print("ZIP files found:", len(zip_files))
print("Output file:", OUT_NC)




#11. fixed pressure grid

import numpy as np

P_MIN, P_MAX, P_STEP = 0.0, 2000.0, 10.0
PGRID = np.arange(P_MIN, P_MAX + P_STEP, P_STEP).astype("f4")
NLEV = len(PGRID)

print("Grid:", NLEV, "levels")




#12. Create NetCDF

from netCDF4 import Dataset

if os.path.exists(OUT_NC):
    os.remove(OUT_NC)

nc = Dataset(OUT_NC, "w", format="NETCDF4")

nc.createDimension("N_PROF", None)
nc.createDimension("N_LEVELS", NLEV)

nc.createVariable("PRES_GRID", "f4", ("N_LEVELS",))[:] = PGRID

nc.createVariable("JULD", "f8", ("N_PROF",))
nc.createVariable("LATITUDE", "f4", ("N_PROF",))
nc.createVariable("LONGITUDE", "f4", ("N_PROF",))
nc.createVariable("CYCLE_NUMBER", "i4", ("N_PROF",))

nc.createVariable("PLATFORM_NUMBER", str, ("N_PROF",))
nc.createVariable("SOURCE_FILE", str, ("N_PROF",))

fill = np.float32(np.nan)
nc.createVariable("TEMP", "f4", ("N_PROF","N_LEVELS"), fill_value=fill)
nc.createVariable("PSAL", "f4", ("N_PROF","N_LEVELS"), fill_value=fill)

nc.close()

print("✅ NetCDF created")





#12. Processing Functions

import numpy as np
from netCDF4 import Dataset

GOOD_QC = {"1","2","3"}   # relaxed QC (important)

def clear_tmp():
    for f in os.listdir(TMP_EXTRACT):
        fp = os.path.join(TMP_EXTRACT, f)
        if os.path.isfile(fp):
            os.remove(fp)

def as_array(a):
    a = np.array(a).squeeze()
    if np.ma.isMaskedArray(a):
        a = a.filled(np.nan)
    return a

def get_row(var, i):
    a = as_array(var[:])
    if a.ndim == 0:
        return a
    elif a.ndim == 1:
        return a[i] if len(a) > 1 else a
    else:
        return a[i, ...]

def get_var(ds, name, i):
    if name not in ds.variables:
        return None
    return get_row(ds.variables[name], i)

def qc_mask(values, qc):
    v = np.array(values, dtype=float)

    if qc is None:
        return v

    q = np.array(qc)

    if q.dtype.kind == "S":
        q = np.char.decode(q)

    q = np.array([str(x)[-1] for x in q])

    good = np.isin(q, list(GOOD_QC))
    v[~good] = np.nan

    return v

def interp_to_grid(pres, var):
    pres = np.array(pres, dtype=float)
    var  = np.array(var, dtype=float)

    mask = np.isfinite(pres) & np.isfinite(var)

    # relaxed condition
    if mask.sum() < 2:
        return np.full(NLEV, np.nan)

    pres = pres[mask]
    var  = var[mask]

    # sort by pressure
    idx = np.argsort(pres)
    pres = pres[idx]
    var  = var[idx]

    # remove duplicates
    _, unique_idx = np.unique(pres, return_index=True)
    pres = pres[unique_idx]
    var  = var[unique_idx]

    return np.interp(PGRID, pres, var, left=np.nan, right=np.nan)




#13. Final product (combined .nc file)

from tqdm import tqdm
import zipfile, glob

done = set()

if os.path.exists(PROGRESS_LOG):
    with open(PROGRESS_LOG) as f:
        done = set(x.strip() for x in f)

out = Dataset(OUT_NC, "a")

total_profiles = 0
valid_profiles = 0

for zpath in tqdm(zip_files):
    name = os.path.basename(zpath)

    if name in done:
        continue

    clear_tmp()

    with zipfile.ZipFile(zpath) as z:
        z.extractall(TMP_EXTRACT)

    nc_files = glob.glob(os.path.join(TMP_EXTRACT, "*.nc"))

    for fpath in nc_files:
        try:
            ds = Dataset(fpath)

            nprof = ds.dimensions["N_PROF"].size if "N_PROF" in ds.dimensions else 1

            for i in range(nprof):

                total_profiles += 1

                # FIXED variable selection (NO "or")
                pres = get_var(ds, "PRES_ADJUSTED", i)
                if pres is None:
                    pres = get_var(ds, "PRES", i)

                temp = get_var(ds, "TEMP_ADJUSTED", i)
                if temp is None:
                    temp = get_var(ds, "TEMP", i)

                psal = get_var(ds, "PSAL_ADJUSTED", i)
                if psal is None:
                    psal = get_var(ds, "PSAL", i)

                if pres is None or temp is None or psal is None:
                    continue

                # QC filtering
                temp = qc_mask(temp, get_var(ds, "TEMP_QC", i))
                psal = qc_mask(psal, get_var(ds, "PSAL_QC", i))

                # DEBUG (important)
                if total_profiles % 500 == 0:
                    print("DEBUG profile:", total_profiles)
                    print("Valid TEMP:", np.isfinite(temp).sum())
                    print("Valid PSAL:", np.isfinite(psal).sum())

                temp_g = interp_to_grid(pres, temp)
                psal_g = interp_to_grid(pres, psal)

                # skip empty profiles
                if np.all(np.isnan(temp_g)) and np.all(np.isnan(psal_g)):
                    continue

                valid_profiles += 1

                idx = out.dimensions["N_PROF"].size

                # metadata
                out["JULD"][idx] = float(get_var(ds, "JULD", i) or np.nan)
                out["LATITUDE"][idx] = float(get_var(ds, "LATITUDE", i) or np.nan)
                out["LONGITUDE"][idx] = float(get_var(ds, "LONGITUDE", i) or np.nan)
                out["CYCLE_NUMBER"][idx] = int(get_var(ds, "CYCLE_NUMBER", i) or -1)

                # optional metadata
                try:
                    plat = get_var(ds, "PLATFORM_NUMBER", i)
                    if plat is not None:
                        if isinstance(plat, bytes):
                            plat = plat.decode()
                        out["PLATFORM_NUMBER"][idx] = str(plat)
                    else:
                        out["PLATFORM_NUMBER"][idx] = ""
                except:
                    out["PLATFORM_NUMBER"][idx] = ""

                out["SOURCE_FILE"][idx] = os.path.basename(fpath)

                # data
                out["TEMP"][idx, :] = temp_g
                out["PSAL"][idx, :] = psal_g

            ds.close()

        except Exception as e:
            with open(FAILED_LOG, "a") as f:
                f.write(f"{fpath} | {e}\n")

    with open(PROGRESS_LOG, "a") as f:
        f.write(name + "\n")

out.close()

print("\n✅ DONE SUCCESSFULLY")
print("Total profiles read:", total_profiles)
print("Valid profiles saved:", valid_profiles)




#14. check total & valid value

from netCDF4 import Dataset
import numpy as np

ds = Dataset(OUT_NC)

temp = ds["TEMP"][:]

print("TOTAL values:", temp.size)
print("VALID values:", np.isfinite(temp).sum())




# validity proof 
from netCDF4 import Dataset

ds = Dataset(OUT_NC)
print("N_PROF:", ds.dimensions["N_PROF"].size)
print("TEMP shape:", ds.variables["TEMP"].shape)
ds.close()






