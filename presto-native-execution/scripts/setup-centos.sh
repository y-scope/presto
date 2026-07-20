#!/bin/bash
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

set -e
set -x

export CC=/opt/rh/gcc-toolset-12/root/bin/gcc
export CXX=/opt/rh/gcc-toolset-12/root/bin/g++

GPERF_VERSION="3.1"
DATASKETCHES_VERSION="5.2.0"
OPENTELEMETRY_CPP_VERSION="1.27.0"
XXHASH_VERSION="0.8.3"

CPU_TARGET="${CPU_TARGET:-avx}"
SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
if [ -f "${SCRIPT_DIR}/setup-centos9.sh" ]; then
  source "${SCRIPT_DIR}/setup-centos9.sh"
else
  source "${SCRIPT_DIR}/../velox/scripts/setup-centos9.sh"
fi

# NPROC is normally sourced from the Velox setup scripts.
export NPROC=${NPROC:-$(getconf _NPROCESSORS_ONLN)}

function install_presto_deps_from_package_managers {
  dnf install -y maven java clang-tools-extra jq perl-XML-XPath
  # This python version is installed by the Velox setup scripts
  pip install regex pyyaml chevron black ptsd-jbroll
}

function install_gperf {
  wget_and_untar https://mirrors.ocf.berkeley.edu/gnu/gperf/gperf-${GPERF_VERSION}.tar.gz gperf
  (
    cd ${DEPENDENCY_DIR}/gperf || exit &&
      ./configure --prefix=/usr/local/gperf/3_1 &&
      make "-j${NPROC}" &&
      make install
    if [ -f /usr/local/bin/gperf ]; then
      echo "Did not create '/usr/local/bin/gperf' symlink as file already exists."
    else
      ln -s /usr/local/gperf/3_1/bin/gperf /usr/local/bin/
    fi
  )
}

function install_proxygen {
  wget_and_untar https://github.com/facebook/proxygen/archive/refs/tags/${FB_OS_VERSION}.tar.gz proxygen
  # Folly Portability.h being used to decide whether or not support coroutines
  # causes issues (build, lin) if the selection is not consistent across users of folly.
  EXTRA_PKG_CXXFLAGS=" -DFOLLY_CFG_NO_COROUTINES"
  cmake_install_dir proxygen -DBUILD_TESTS=OFF -DBUILD_SHARED_LIBS=ON
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
  dnf install -y libarchive-devel
}

function install_presto_deps {
  run_and_time install_presto_deps_from_package_managers
  run_and_time install_gperf
  run_and_time install_proxygen
  run_and_time install_datasketches
  run_and_time install_opentelemetry_cpp
  run_and_time install_xxhash
  run_and_time install_libarchive
}

if [[ $# -ne 0 ]]; then
  # Activate gcc12; enable errors on unset variables afterwards.
  source /opt/rh/gcc-toolset-12/enable || exit 1
  set -u
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
  # Activate gcc12; enable errors on unset variables afterwards.
  source /opt/rh/gcc-toolset-12/enable || exit 1
  set -u
  install_velox_deps
  install_presto_deps
  echo "All dependencies for Prestissimo installed!"
fi

dnf clean all
