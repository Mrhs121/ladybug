# Python 绑定编译指南

## 背景

Ladybug 的 Python 绑定（`real_ladybug`）通过 pybind11 将 C++ 核心库暴露给 Python。编译产物是一个平台相关的共享库，文件名包含 Python 版本信息，例如：

```
_lbug.cpython-313-x86_64-linux-gnu.so
```

**.so 文件与 Python 版本强绑定**，必须与实际使用的 Python 解释器版本完全一致。

---

## 前提条件

- 系统已安装 CMake、GCC/Clang（支持 C++20）
- 已安装 [uv](https://docs.astral.sh/uv/)
- 目标 Python 版本需与 Nexus 仓库中的一致（当前为 **Python 3.13**）

---

## 编译步骤

### 第一步：安装目标 Python 版本

uv 可以独立管理 Python 版本，无需系统安装：

```bash
uv python install 3.13
```

安装完成后，查询可执行文件路径：

```bash
uv python find 3.13
# 输出示例：/home/zcy/.local/share/uv/python/cpython-3.13.x-linux-x86_64-gnu/bin/python3.13
```

### 第二步：进入 Ladybug 根目录

```bash
cd /path/to/ladybug
```

### 第三步：配置 CMake

同时编译 Python 绑定和 Shell 工具：

```bash
cmake -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_PYTHON=TRUE \
    -DBUILD_SHELL=TRUE \
    -DPYTHON_EXECUTABLE=$(uv python find 3.13)
```

> **注意：变量名是 `PYTHON_EXECUTABLE`，不是 `Python3_EXECUTABLE`。**
> 项目使用的是旧版 pybind11，它通过 `FindPythonLibsNew.cmake` 查找解释器，
> 读取的变量名是 `PYTHON_EXECUTABLE`。传入 `Python3_EXECUTABLE` 会被忽略，
> 导致 CMake 退回到系统默认 Python（可能是 3.12），编译出错误版本的 .so。

如果之前编译过其他版本，需要先清除 CMake 缓存：

```bash
rm -rf build/release/CMakeCache.txt build/release/CMakeFiles
```

然后重新执行上述 `cmake -B` 命令。

### 第四步：编译

```bash
cmake --build build/release --parallel 8
```

### 第五步：拷贝 Python 源文件

CMakeLists.txt 通过 `file(COPY ...)` 自动将 `src_py/` 拷贝到输出目录，通常不需要手动操作。如果发现 Python 源文件未同步，可手动执行：

```bash
cp tools/python_api/src_py/*.py tools/python_api/build/real_ladybug/
```

---

## 验证结果

```bash
ls tools/python_api/build/real_ladybug/_lbug*.so
```

输出应包含目标 Python 版本号，例如：

```
tools/python_api/build/real_ladybug/_lbug.cpython-313-x86_64-linux-gnu.so
```

---

## 在 uv 项目中使用

在你的 uv 项目的 `pyproject.toml` 中配置：

```toml
[project]
dependencies = ["real_ladybug"]

[tool.uv.sources]
real_ladybug = { path = "/path/to/ladybug/tools/python_api", editable = true }
```

然后同步依赖：

```bash
uv sync
```

editable 模式下，重新编译 `.so` 后无需重新 `uv sync`，直接生效。

---

## 常见问题

### 编译后 .so 仍是 3.12 版本

原因：CMake 缓存中保存了旧的 Python 路径。

解决：

```bash
rm -rf build/release/CMakeCache.txt build/release/CMakeFiles
cmake -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_PYTHON=TRUE \
    -DBUILD_SHELL=TRUE \
    -DPYTHON_EXECUTABLE=$(uv python find 3.13)
cmake --build build/release --parallel 8
```

### uv 项目安装时报 Python 版本不满足

`tools/python_api/pyproject.toml` 中的 `requires-python` 需与编译版本匹配：

```toml
requires-python = ">=3.13,<3.14"
```

如编译的是其他版本，相应修改此字段。

### ImportError: Python version mismatch

`.so` 文件与运行时 Python 版本不一致。确保 uv 项目 pin 的 Python 版本与编译版本相同：

```bash
uv python pin 3.13
```
