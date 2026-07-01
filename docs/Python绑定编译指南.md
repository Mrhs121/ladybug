# Python 绑定、Wheel 与 lbug 编译指南

Ladybug 的 Python 导入包名是 `ladybug`，发行包名是 `ladybugdb`。Python 绑定通过
pybind11 调用 C++ 核心库，因此 wheel 和 `_lbug` 共享库都与 Python 版本及操作系统平台相关。

本文说明三种构建目标：

1. 构建可安装的 Python wheel；
2. 为本地开发直接编译 Python 绑定；
3. 编译 `lbug` Shell 可执行程序。

## 环境准备

基础依赖：

- CMake；
- 支持 C++20 的 GCC 或 Clang；
- Make；
- [uv](https://docs.astral.sh/uv/)；
- Docker，仅 manylinux wheel 构建需要。

当前 Python 包要求 Python 3.13：

```bash
uv python install 3.13
uv python find 3.13
```

以下命令均在 Ladybug 仓库根目录执行：

```bash
cd /path/to/ladybug
```

## 构建 Python wheel

仓库提供统一打包脚本：

```bash
./scripts/pip-package/build_wheel.sh --help
```

脚本会自动清理旧打包产物、生成 sdist、编译原生扩展并输出 wheel 的绝对路径。

### manylinux wheel（推荐）

默认使用 Docker 和 cibuildwheel 构建 CPython 3.13、x86_64 manylinux wheel：

```bash
./scripts/pip-package/build_wheel.sh
```

指定并行编译线程数：

```bash
./scripts/pip-package/build_wheel.sh --jobs 8
```

指定 Python 版本和架构：

```bash
./scripts/pip-package/build_wheel.sh \
    --python 3.13 \
    --arch x86_64 \
    --jobs 8
```

manylinux 模式适合跨 Linux 发行版部署或发布到包仓库。首次构建需要下载 manylinux
Docker 镜像；后续构建会复用本机 Docker 镜像缓存。

生成的文件位于：

```text
scripts/pip-package/wheelhouse/
```

文件名示例：

```text
ladybugdb-0.17.1-cp313-cp313-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl
```

多个 manylinux 平台标签是 `auditwheel` 根据动态库依赖生成的兼容性标签，不表示重复打包。

### 本机 wheel

只在当前机器或相同运行环境中使用时，可以跳过 Docker，直接构建本机 wheel：

```bash
./scripts/pip-package/build_wheel.sh --native --jobs 8
```

本机构建的 wheel 通常带有 `linux_x86_64` 等平台标签，并可能依赖当前系统的 glibc、
libstdc++ 或其他动态库，因此不建议作为通用 Linux wheel 发布。

### 清理 wheel 打包产物

```bash
./scripts/pip-package/build_wheel.sh --clean
```

该命令只删除打包流程生成的文件：

```text
scripts/pip-package/*.tar.gz
scripts/pip-package/cibw-source/
scripts/pip-package/wheelhouse/
```

### 安装和验证 wheel

建议使用全新虚拟环境验证：

```bash
rm -rf /tmp/ladybug-wheel-test
uv venv --python 3.13 /tmp/ladybug-wheel-test

uv pip install \
    --python /tmp/ladybug-wheel-test/bin/python \
    scripts/pip-package/wheelhouse/*.whl

/tmp/ladybug-wheel-test/bin/python - <<'PY'
import ladybug

print(ladybug)
print(ladybug.__file__)
PY
```

## 为本地开发编译 Python 绑定

如果只需要在源码目录中开发或运行测试，可以直接通过 CMake 编译，不必生成 wheel。

先清除可能记录了其他 Python 版本的 CMake 缓存：

```bash
rm -rf build/release/CMakeCache.txt build/release/CMakeFiles
```

配置并编译：

```bash
cmake -S . -B build/release -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_PYTHON=TRUE \
    -DBUILD_SHELL=FALSE \
    -DPYTHON_EXECUTABLE="$(uv python find 3.13)"

cmake --build build/release --parallel 8
```

`PYTHON_EXECUTABLE` 必须指向实际使用的 Python。不要改用 `Python3_EXECUTABLE`；项目中的
pybind11 查找逻辑读取的是 `PYTHON_EXECUTABLE`。

Python 包和原生扩展输出到：

```text
tools/python_api/build/ladybug/
tools/python_api/build/ladybug/_lbug.cpython-313-x86_64-linux-gnu.so
```

验证本地构建：

```bash
PYTHONPATH=tools/python_api/build \
    "$(uv python find 3.13)" -c 'import ladybug; print(ladybug.__file__)'
```

也可以使用 Makefile 的快捷目标。通过环境变量指定解释器和线程数：

```bash
PYTHON_EXECUTABLE="$(uv python find 3.13)" NUM_THREADS=8 make python
```

## 编译 lbug 可执行程序

`lbug` 是 Ladybug 的交互式 Shell，与 Python wheel 是两个独立产物。

### 使用 Makefile

推荐使用快捷目标：

```bash
NUM_THREADS=8 make shell
```

Release 可执行文件生成在：

```text
build/release/tools/shell/lbug
```

运行：

```bash
./build/release/tools/shell/lbug
```

Debug 构建：

```bash
NUM_THREADS=8 make shell-debug
./build/debug/tools/shell/lbug
```

### 使用 CMake

```bash
cmake -S . -B build/release -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHELL=TRUE \
    -DBUILD_PYTHON=FALSE

cmake --build build/release --target lbug_shell --parallel 8
```

CMake 目标名是 `lbug_shell`，最终可执行文件名是 `lbug`。

## 同时编译本地 Python 绑定和 lbug

如果本地开发同时需要两个产物，可以共享同一个 CMake 构建目录：

```bash
rm -rf build/release/CMakeCache.txt build/release/CMakeFiles

cmake -S . -B build/release -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_PYTHON=TRUE \
    -DBUILD_SHELL=TRUE \
    -DPYTHON_EXECUTABLE="$(uv python find 3.13)"

cmake --build build/release --parallel 8
```

这不会生成 wheel。需要 wheel 时仍应使用 `scripts/pip-package/build_wheel.sh`。

## 常见问题

### `_lbug` 文件名是其他 Python 版本

CMake 缓存中保存了旧解释器路径。删除缓存后重新配置：

```bash
rm -rf build/release/CMakeCache.txt build/release/CMakeFiles
```

确认配置参数使用：

```bash
-DPYTHON_EXECUTABLE="$(uv python find 3.13)"
```

### `ImportError` 或原生模块无法加载

确认构建和运行使用相同的 Python 主次版本：

```bash
uv python find 3.13
```

本地源码构建还需要设置正确的 `PYTHONPATH`：

```bash
export PYTHONPATH="$PWD/tools/python_api/build"
```

### wheel 无法安装到当前 Python

`cp313` wheel 只能安装到 CPython 3.13。当前项目的 `requires-python` 也限制为：

```toml
requires-python = ">=3.13,<3.14"
```

请使用 Python 3.13 创建环境，或者在正式支持其他 Python 版本后重新构建对应 wheel。

### manylinux 构建反复下载镜像

Docker 会缓存已下载的镜像层。不要执行会删除镜像缓存的命令，例如：

```bash
docker system prune -a
docker image prune -a
```
