# 🧬 CENMIG Data Management Pipeline

This repository provides a Python-based pipeline for managing, processing, and updating metadata and sequence data in CENMIGDB.
It supports automatic data fetching from NCBI, processing in-house data, and updating local databases for resistance gene and MLST tools.

## 🚀 Features

✅ Automated metadata download from NCBI

🧩 Processing of in-house metadata

🗃️ Integration with CENMIGDB (update & delete records)

🔁 Automatic update of MLST and resistance gene databases

⚙️ Setup script for installing external dependencies

## 🧱 Requirements

- Python ≥3.8
- Linux environment (recommended)
- Internet access for NCBI metadata downloads
- Installed tools (will be auto-downloaded if missing):
- esearch (Entrez Direct)
- sratoolkit
- krocus
- stringMLST
- docker

## 🧩 Installation
```
Clone this repository
git clone https://github.com/Thanakron1997/CENMIGDB-Pipeline.git
cd CENMIGDB-Pipeline

# Create and activate a virtual environment
python3 -m venv env
source env/bin/activate

# Install required Python packages
pip install -r requirements.txt
```