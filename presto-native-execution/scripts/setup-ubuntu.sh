#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Minimal setup for Ubuntu 20.04.
set -eufx -o pipefail

# Run the velox setup script first.
source "$(dirname "${BASH_SOURCE[0]}")/../velox/scripts/setup-ubuntu.sh"
SUDO="${SUDO:-"sudo --preserve-env"}"
DATASKETCHES_VERSION="5.2.0"
OPENTELEMETRY_CPP_VERSION="1.27.0"
XXHASH_VERSION="0.8.3"

function install_proxygen {
  # proxygen requires python and gperf
  ${SUDO} apt update
  ${SUDO} apt install -y gperf python3
  wget_and_untar https://github.com/facebook/proxygen/archive/refs/tags/${FB_OS_VERSION}.tar.gz proxygen
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, lin) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  cmake_install_dir proxygen -DBUILD_TESTS=OFF
}

function install_datasketches {
  wget_and_untar https://github.com/apache/datasketches-cpp/archive/refs/tags/${DATASKETCHES_VERSION}.tar.gz datasketches-cpp
  cmake_install_dir datasketches-cpp -DBUILD_TESTS=OFF
}

function install_opentelemetry_cpp {
  # The version and build configuration must stay in sync with the
  # opentelemetry-cpp dependency of CLP (taskfiles/deps/main.yaml in
  # y-scope/clp), which links against it in clp_s::search.
  wget_and_untar https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v${OPENTELEMETRY_CPP_VERSION}.tar.gz opentelemetry-cpp
  cmake_install_dir opentelemetry-cpp \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=20 \
    -DOPENTELEMETRY_INSTALL=ON \
    -DWITH_BENCHMARK=OFF \
    -DWITH_EXAMPLES=OFF \
    -DWITH_FUNC_TESTS=OFF \
    -DWITH_OTLP_GRPC=OFF \
    -DWITH_OTLP_HTTP=ON
}

function install_xxhash {
  # The version and build configuration must stay in sync with the xxHash
  # dependency of CLP (taskfiles/deps/main.yaml in y-scope/clp), which
  # requires xxHash's CMake package config.
  wget_and_untar https://github.com/Cyan4973/xxHash/archive/refs/tags/v${XXHASH_VERSION}.tar.gz xxHash
  cmake_install_dir xxHash/cmake_unofficial \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=OFF \
    -DXXHASH_BUILD_XXHSUM=OFF
}

function install_libarchive {
  # CLP requires LibArchive's development files at configure time.
  ${SUDO} apt update
  ${SUDO} apt install -y libarchive-dev
}

function install_presto_deps {
  run_and_time install_proxygen
  run_and_time install_datasketches
  run_and_time install_opentelemetry_cpp
  run_and_time install_xxhash
  run_and_time install_libarchive
}

if [[ $# -ne 0 ]]; then
  for cmd in "$@"; do
    run_and_time "${cmd}"
  done
  echo "All specified dependencies installed!"
else
  if [ "${INSTALL_PREREQUISITES:-Y}" == "Y" ]; then
    echo "Installing build dependencies"
    run_and_time install_build_prerequisites
  else
    echo "Skipping installation of build dependencies since INSTALL_PREREQUISITES is not set"
  fi
  install_velox_deps
  install_presto_deps
  echo "All dependencies for Prestissimo installed!"
fi
