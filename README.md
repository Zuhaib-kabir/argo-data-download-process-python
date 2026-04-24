# 🌊 Argo Float Data Processing Pipeline

A complete end-to-end Python pipeline for downloading, filtering, processing, and analyzing **Argo float oceanographic data**. This project converts raw Argo NetCDF profiles into a clean, gridded dataset ready for scientific analysis and machine learning.

---

## 📌 Overview

This project automates the full workflow of Argo data handling:

* 📥 Download global Argo index
* 🌍 Filter data by **region & time**
* 📦 Batch download NetCDF profile files
* 🧹 Apply quality control (QC) filtering
* 📊 Interpolate profiles to a uniform pressure grid
* 🧬 Combine all profiles into a single dataset
* 📈 Generate vertical profiles and mean ocean structure

---

## ⚙️ Features

* Robust batch downloading with retry mechanism
* Handles large-scale datasets efficiently
* Supports flexible spatial and temporal filtering
* QC-based filtering for reliable scientific results
* Interpolation to standard pressure levels (0–2000 dbar)
* Output stored in a clean NetCDF format
* Ready for visualization and ML applications

---

## 🗂️ Project Structure

```
argo-data-download-process-python/
│
├── argo_processing_pipeline.py   # Main pipeline script
├── argo_combined.nc              # Final processed dataset
├── zip_inventory.csv             # ZIP file summary
├── nc_schema_sample.csv          # Sample structure check
├── processed_zips.txt            # Progress log
├── process_failed.log            # Failed file log
└── README.md                     # Project documentation
```

---

## 🚀 How It Works

### 1. Define Study Area

### 2. Set Time Range

### 3. Run Pipeline

* Download Argo index
* Filter profiles
* Download `.nc` files in batches
* Process and interpolate data
* Save combined dataset

---

## 📊 Output Data

Final NetCDF file contains:

| Variable     | Description            |
| ------------ | ---------------------- |
| TEMP         | Temperature (°C)       |
| PSAL         | Salinity (psu)         |
| PRES_GRID    | Pressure levels (dbar) |
| LATITUDE     | Profile latitude       |
| LONGITUDE    | Profile longitude      |
| JULD         | Time (Julian date)     |
| CYCLE_NUMBER | Float cycle number     |

---



## 🧪 Applications

* Oceanographic analysis
* Climate variability studies
* Mixed Layer Depth (MLD) estimation
* Coral reef suitability modeling
* Machine learning dataset preparation
* Satellite–in situ data comparison

---

## ⚠️ Notes

* Argo data coverage may be sparse in short time windows
* QC filtering may reduce usable data significantly
* Use longer time ranges for better spatial coverage

---

## 🔮 Future Improvements

* Add density (sigma-theta) calculation
* Monthly and seasonal climatology
* Integration with satellite datasets (SST, chlorophyll)
* Machine learning models for ocean prediction

---

## 👨‍💻 Author

**Md. Zuhaib Kabir**
BSc in Oceanography

---

## 📜 License

This project is open-source and available for academic and research use.

---

## ⭐ Acknowledgment

Data provided by the global Argo program:
https://argo.ucsd.edu
