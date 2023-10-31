#!/bin/bash

SLICER_VERSION="2.0.0"
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

DOWNLOAD_PLATFORMS=("arm64" "amd64")

for PLATFORM_SUFFIX in "${DOWNLOAD_PLATFORMS[@]}"; do
  DOWNLOAD_FILE_PATH="$SCRIPT_DIR/slicer_linux_$PLATFORM_SUFFIX"
  echo "https://github.com/keboola/processor-split-table/releases/download/v$SLICER_VERSION/cli_linux_$PLATFORM_SUFFIX"
  curl -fL -o $DOWNLOAD_FILE_PATH "https://github.com/keboola/processor-split-table/releases/download/v$SLICER_VERSION/cli_linux_$PLATFORM_SUFFIX"
  chmod +x $DOWNLOAD_FILE_PATH
done
