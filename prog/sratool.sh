#!/bin/bash
set -e
set -o pipefail

# ==============================================
# CONFIGURATION
# ==============================================
INSTALL_DIR="prog"
SRA_BASE="https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current"
DATASETS_BASE="https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2"

mkdir -p "$INSTALL_DIR"
cd "$INSTALL_DIR" || { echo "Failed to enter prog directory"; exit 1; }

# ==============================================
# HELPER: Download safely with wget/curl
# ==============================================

download_file() {
    local url="$1"
    local output="$2"
    echo "Downloading: $url"
    if command -v wget >/dev/null 2>&1; then
        wget --quiet -O "$output" "$url"
    elif command -v curl >/dev/null 2>&1; then
        curl -s -L -o "$output" "$url"
    else
        echo "Neither wget nor curl is available."
        exit 1
    fi
}

# ==============================================
# DETECT PLATFORM
# ==============================================
osname=$(uname -s)
cputype=$(uname -m)
echo "Detecting platform: $osname $cputype"

case "$osname-$cputype" in
  Linux-x86_64 )
    SRA_FILE="sratoolkit.current-ubuntu64.tar.gz"
    DATASETS_PLATFORM="linux-amd64"
    ;;
  Linux-aarch64 )
    SRA_FILE="sratoolkit.current-alma_linux64.tar.gz"  # ARM64 variant (works on most ARM distros)
    DATASETS_PLATFORM="linux-arm64"
    ;;
  Linux-*arm* )
    SRA_FILE="sratoolkit.current-alma_linux64.tar.gz"  # fallback
    DATASETS_PLATFORM="linux-arm"
    ;;
  Darwin-x86_64 )
    SRA_FILE="sratoolkit.current-mac64.tar.gz"
    DATASETS_PLATFORM="mac"
    ;;
  Darwin-*arm* )
    SRA_FILE="sratoolkit.current-mac-arm64.tar.gz"
    DATASETS_PLATFORM="mac"
    ;;
  MINGW* | CYGWIN* | MSYS* )
    SRA_FILE="sratoolkit.current-win64.zip"
    DATASETS_PLATFORM="win64"
    ;;
  * )
    echo "Unrecognized platform:  $osname-$cputype"
    exit 1 ;;
esac

# ==============================================
# DOWNLOAD AND EXTRACT SRA TOOLKIT
# ==============================================
echo "Downloading SRA Toolkit for $SRA_FILE"
download_file "$SRA_BASE/$SRA_FILE" "$SRA_FILE"

if [[ "$SRA_FILE" == *.zip ]]; then
    unzip -q "$SRA_FILE"
else
    tar -xzf "$SRA_FILE"
fi
rm -f "$SRA_FILE"

SRA_DIR=$(ls -d sratoolkit.* | head -n1)
mv "$SRA_DIR" sratoolkit
# ==============================================
# DOWNLOAD DATASETS AND DATAFORMAT
# ==============================================
DATASETS_URL="$DATASETS_BASE/$DATASETS_PLATFORM/datasets"
DATAFORMAT_URL="$DATASETS_BASE/$DATASETS_PLATFORM/dataformat"

download_file "$DATASETS_URL" "datasets"
download_file "$DATAFORMAT_URL" "dataformat"

chmod +x datasets dataformat
