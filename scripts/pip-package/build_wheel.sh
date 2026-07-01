#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

MODE="manylinux"
PYTHON_VERSION="3.13"
MANYLINUX_ARCH="x86_64"
CLEAN_ONLY="false"
BUILD_JOBS=""

usage() {
    cat <<'EOF'
用法:
  scripts/pip-package/build_wheel.sh [选项]

清理旧打包产物，重新生成源码包并构建 Ladybug Python wheel。
默认使用 Docker 和 cibuildwheel 构建 CPython 3.13、x86_64 manylinux wheel。

选项:
  --manylinux          使用 Docker/cibuildwheel 构建 manylinux wheel（默认）
  --native             直接在当前系统构建，不使用 Docker
  --clean              删除打包产物后退出，不执行构建
  --python VERSION     指定 Python 版本（默认: 3.13）
  --arch ARCH          指定 manylinux 架构（默认: x86_64）
  --jobs N             指定 C++ 并行编译线程数（默认: 使用全部可用 CPU）
  -h, --help           显示此帮助并退出

清理范围:
  scripts/pip-package/*.tar.gz
  scripts/pip-package/cibw-source/
  scripts/pip-package/wheelhouse/

依赖:
  所有模式: uv、C/C++ 编译工具链、CMake、Make
  manylinux 模式: Docker，并且 Docker daemon 必须正在运行

输出:
  wheel 写入 scripts/pip-package/wheelhouse/，构建成功后打印绝对路径。

示例:
  # 默认构建 CPython 3.13 x86_64 manylinux wheel
  scripts/pip-package/build_wheel.sh

  # 直接在当前系统构建
  scripts/pip-package/build_wheel.sh --native

  # 仅清理打包产物
  scripts/pip-package/build_wheel.sh --clean

  # 使用 8 个线程构建
  scripts/pip-package/build_wheel.sh --jobs 8

  # 指定 Python 版本和 manylinux 架构
  scripts/pip-package/build_wheel.sh --python 3.13 --arch x86_64
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --manylinux)
            MODE="manylinux"
            shift
            ;;
        --native)
            MODE="native"
            shift
            ;;
        --clean)
            CLEAN_ONLY="true"
            shift
            ;;
        --python)
            [[ $# -ge 2 ]] || { echo "error: --python requires a value" >&2; exit 2; }
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --arch)
            [[ $# -ge 2 ]] || { echo "error: --arch requires a value" >&2; exit 2; }
            MANYLINUX_ARCH="$2"
            shift 2
            ;;
        --jobs)
            [[ $# -ge 2 ]] || { echo "error: --jobs requires a value" >&2; exit 2; }
            BUILD_JOBS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -n "${BUILD_JOBS}" && ! "${BUILD_JOBS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --jobs must be a positive integer" >&2
    exit 2
fi

PYTHON_TAG="cp${PYTHON_VERSION//./}"
SDIST_DIR="${SCRIPT_DIR}/cibw-source"
SDIST_ROOT="${SDIST_DIR}/sdist"
WHEELHOUSE="${SCRIPT_DIR}/wheelhouse"

clean_artifacts() {
    rm -rf -- "${SDIST_DIR}" "${WHEELHOUSE}"
    find "${SCRIPT_DIR}" -maxdepth 1 -type f -name '*.tar.gz' -delete
}

echo "Cleaning packaging artifacts..."
clean_artifacts

if [[ "${CLEAN_ONLY}" == "true" ]]; then
    echo "Packaging artifacts removed."
    exit 0
fi

command -v uv >/dev/null 2>&1 || {
    echo "error: uv is required but was not found in PATH" >&2
    exit 1
}

mkdir -p -- "${SDIST_DIR}" "${WHEELHOUSE}"

echo "Generating source distribution..."
(
    cd -- "${SCRIPT_DIR}"
    uv run --python "${PYTHON_VERSION}" --with setuptools --with wheel package_tar.py
)

mapfile -t SDISTS < <(find "${SCRIPT_DIR}" -maxdepth 1 -type f -name '*.tar.gz' -print)
if [[ ${#SDISTS[@]} -ne 1 ]]; then
    echo "error: expected one source archive, found ${#SDISTS[@]}" >&2
    exit 1
fi

tar -xzf "${SDISTS[0]}" -C "${SDIST_DIR}"

if [[ "${MODE}" == "manylinux" ]]; then
    command -v docker >/dev/null 2>&1 || {
        echo "error: Docker is required for a manylinux build" >&2
        exit 1
    }
    docker info >/dev/null 2>&1 || {
        echo "error: the Docker daemon is not available" >&2
        exit 1
    }

    echo "Building ${PYTHON_TAG}-manylinux_${MANYLINUX_ARCH} wheel..."
    if [[ -n "${BUILD_JOBS}" ]]; then
        CIBW_ENVIRONMENT="${CIBW_ENVIRONMENT:+${CIBW_ENVIRONMENT} }NUM_THREADS=${BUILD_JOBS}" \
            uv tool run --from cibuildwheel \
            cibuildwheel "${SDIST_ROOT}" \
            --only "${PYTHON_TAG}-manylinux_${MANYLINUX_ARCH}" \
            --output-dir "${WHEELHOUSE}"
    else
        uv tool run --from cibuildwheel \
            cibuildwheel "${SDIST_ROOT}" \
            --only "${PYTHON_TAG}-manylinux_${MANYLINUX_ARCH}" \
            --output-dir "${WHEELHOUSE}"
    fi
else
    echo "Building native Python ${PYTHON_VERSION} wheel..."
    if [[ -n "${BUILD_JOBS}" ]]; then
        NUM_THREADS="${BUILD_JOBS}" \
            uv run --python "${PYTHON_VERSION}" --with build --with setuptools --with wheel \
            python -m build --wheel "${SDIST_ROOT}" --outdir "${WHEELHOUSE}"
    else
        uv run --python "${PYTHON_VERSION}" --with build --with setuptools --with wheel \
            python -m build --wheel "${SDIST_ROOT}" --outdir "${WHEELHOUSE}"
    fi
fi

mapfile -t WHEELS < <(find "${WHEELHOUSE}" -maxdepth 1 -type f -name '*.whl' -print)
if [[ ${#WHEELS[@]} -eq 0 ]]; then
    echo "error: build completed without producing a wheel" >&2
    exit 1
fi

echo "Wheel output:"
for wheel in "${WHEELS[@]}"; do
    realpath -- "${wheel}"
done
