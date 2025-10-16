#!/bin/bash
set -e
pip3 install git+https://github.com/jodyphelan/TBProfiler.git
pip3 install git+https://github.com/jodyphelan/pathogen-profiler.git

git clone https://github.com/timflutre/trimmomatic.git