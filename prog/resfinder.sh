#!/bin/bash
set -e

# ==============================
# Config
# ==============================
HOME_DIR=$(pwd)
BLAST_URL="https://ftp.ncbi.nlm.nih.gov/blast/executables/blast+/2.17.0"
INSTALL_DIR="prog/"
mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR"

# ==============================
# Detect OS and Architecture
# ==============================
osname=$(uname -s)
cputype=$(uname -m)

echo "Detected OS: $osname"
echo "Detected CPU: $cputype"

case "$osname-$cputype" in
  Linux-x86_64 )
    FILE="ncbi-blast-2.17.0+-x64-linux.tar.gz"
    ;;
  Linux-aarch64 )
    FILE="ncbi-blast-2.17.0+-aarch64-linux.tar.gz"
    ;;
  Darwin-x86_64 )
    FILE="ncbi-blast-2.17.0+-x64-macosx.tar.gz"
    ;;
  Darwin-arm64 | Darwin-*arm* )
    FILE="ncbi-blast-2.17.0+-aarch64-macosx.tar.gz"
    ;;
  MINGW*-x86_64 | CYGWIN_NT*-x86_64 )
    FILE="ncbi-blast-2.17.0+-win64.exe"
    ;;
  * )
    echo "❌ Unrecognized platform: $osname-$cputype"
    exit 1
    ;;
esac

# ==============================
# Download BLAST
# ==============================
echo "Downloading $FILE ..."
wget -q --show-progress "$BLAST_URL/$FILE"

# ==============================
# Verify MD5 checksum (optional)
# ==============================
MD5_FILE="${FILE}.md5"
echo "Downloading checksum..."
wget -q "$BLAST_URL/$MD5_FILE"

if command -v md5sum >/dev/null 2>&1; then
    echo "Verifying checksum..."
    md5sum -c "$MD5_FILE" || {
        echo "❌ Checksum verification failed!"
        exit 1
    }
else
    echo "⚠️ md5sum not found, skipping checksum verification."
fi

# ==============================
# Extract / Install
# ==============================
if [[ "$FILE" == *.tar.gz ]]; then
    echo "Extracting $FILE ..."
    tar -xzf "$FILE"
    BLAST_DIR=$(tar -tf "$FILE" | head -1 | cut -f1 -d"/")
elif [[ "$FILE" == *.exe ]]; then
    echo "Windows executable downloaded: $FILE"
else
    echo "Unknown format: $FILE"
fi
mv "$BLAST_DIR" blast
rm "$MD5_FILE"
rm "$FILE"

# ==============================
# Install KMA
# ==============================
echo "Cloning and building KMA..."
git clone https://bitbucket.org/genomicepidemiology/kma.git || true
cd kma
make
cd $HOME_DIR
export PATH="prog/kma/:$PATH"
export PATH="prog/blast/bin:$PATH"
mkdir -p "src/resfinder_db"
# Clone databases
echo "Cloning ResFinder DB..."
git clone https://bitbucket.org/genomicepidemiology/resfinder_db.git src/resfinder_db/resfinder_db
cd src/resfinder_db/resfinder_db
python INSTALL.py
cd $HOME_DIR
echo "Cloning PointFinder DB..."
git clone https://bitbucket.org/genomicepidemiology/pointfinder_db.git src/resfinder_db/pointfinder_db
cd src/resfinder_db/pointfinder_db
python INSTALL.py
cd $HOME_DIR
echo "Cloning DisinFinder DB..."
git clone https://bitbucket.org/genomicepidemiology/disinfinder_db.git src/resfinder_db/disinfinder_db
cd src/resfinder_db/disinfinder_db
python INSTALL.py
cd $HOME_DIR
