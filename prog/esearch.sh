#!/bin/bash

# Public domain notice for all NCBI EDirect scripts is located at:
# https://www.ncbi.nlm.nih.gov/books/NBK179288/#chapter6.Public_Domain_Notice

# Base URL for EDirect
base="https://ftp.ncbi.nlm.nih.gov/entrez/entrezdirect"

# Function to fetch a single file, passed as an argument
FetchFile() {
  fl="$1"

  if [ -x "$(command -v curl)" ]; then
    curl -s "${base}/${fl}" -o "${fl}"
  elif [ -x "$(command -v wget)" ]; then
    wget -q "${base}/${fl}"
  else
    echo "Missing curl and wget commands, unable to download EDirect archive" >&2
    exit 1
  fi
}

# Create and move into prog directory
mkdir -p prog
cd prog || { echo "Failed to enter prog directory"; exit 1; }

# Download and extract EDirect archive
FetchFile "edirect.tar.gz"
if [ -s "edirect.tar.gz" ]; then
  gunzip -c edirect.tar.gz | tar xf -
  rm edirect.tar.gz
fi

if [ ! -d "edirect" ]; then
  echo "Unable to download or extract EDirect archive" >&2
  exit 1
fi

# Move into edirect directory
cd edirect || { echo "Failed to enter edirect directory"; exit 1; }

# Get current working directory
DIR=$(pwd)

plt=""
alt=""

# Detect platform
osname=$(uname -s)
cputype=$(uname -m)
case "$osname-$cputype" in
  Linux-x86_64 )
    plt=Linux ;;
  Darwin-x86_64 )
    plt=Darwin
    alt=Silicon ;;
  Darwin-*arm* )
    plt=Silicon
    alt=Darwin ;;
  CYGWIN_NT-* | MINGW*-* )
    plt=CYGWIN_NT ;;
  Linux-*arm* )
    plt=ARM ;;
  Linux-aarch64 )
    plt=ARM64 ;;
  * )
    echo "Unrecognized platform: $osname-$cputype"
    exit 1 ;;
esac

# Fetch precompiled binaries (xtract, rchive, transmute)
if [ -n "$plt" ]; then
  for exc in xtract rchive transmute; do
    FetchFile "$exc.$plt.gz"
    gunzip -f "$exc.$plt.gz"
    chmod +x "$exc.$plt"
    if [ -n "$alt" ]; then
      # For Apple Silicon, download both versions
      FetchFile "$exc.$alt.gz"
      gunzip -f "$exc.$alt.gz"
      chmod +x "$exc.$alt"
    fi
  done
fi
