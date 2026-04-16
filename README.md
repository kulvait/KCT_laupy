# laupy

**laupy** is a lightweight workflow orchestration framework for high-throughput scientific data processing. Written in Python, it enables scalable, automated pipelines with a focus on dependency-aware execution, HPC integration, and minimal user intervention.

It is primarily developed for tomographic reconstruction at synchrotron beamlines, supporting industrial-scale datasets and GPU-accelerated environments. laupy provides command-line tools for dataset management, SLURM job submission, pipeline orchestration, and real-time status inspection, using open-source technologies such as Python, BASH, OpenCL/C++, and SLURM.

## Motivation

Modern scientific experiments, such as high-throughput tomography at synchrotron facilities, generate large volumes of data that must be processed through complex, multi-step workflows. These workflows often involve heterogeneous tools, multiple execution environments, and dependencies between processing stages, making them difficult to manage, reproduce, and scale.

In practice, reconstruction workflows are:
- fragmented across scripts and tools, leading to inefficient resource utilization
- difficult to automate, scale, and reproduce
- hard to monitor, inspect, and debug

**laupy** addresses these challenges by providing:
- automated, dependency-aware execution, allowing existing scripts and tools to be integrated into structured pipelines
- seamless integration with SLURM for HPC job scheduling and resource management
- tools for dataset management and real-time status inspection

## Installation

SSH clone:

```bash
git clone git@github.com:kulvait/KCT_laupy.git
```

To install the package, execute the following command

```bash
pip install git+https://github.com/kulvait/KCT_laupy.git
```

For an editable local install from the git directory, use the following command

```bash
git clone https://github.com/kulvait/KCT_laupy.git
cd KCT_laupy
pip install --user --upgrade -e .
```


### Upgrading the Package
To update the package, use

```bash
pip install --upgrade git+https://github.com/kulvait/KCT_laupy.git
```

For a local upgrade from the git directory:

```bash
pip install --user --upgrade .
```

For a local development editable upgrade from the git directory:

```bash
pip install --user --upgrade --editable .
``` 

## Command-Line Tools

The **laupy** package installs several command‑line utilities via `console_scripts`.  
These tools become available to the user automatically after installation.

The package provides:

- **`submitslurm`** – submit tomographic reconstruction jobs to a SLURM cluster  
- **`listnodes`** – list available compute nodes on the Maxwell cluster at DESY, including status and GPU capabilities


### `submitslurm`

`submitslurm` is the primary command for submitting reconstruction or preprocessing tasks to a SLURM scheduler.  
It is designed for large‑scale or batch reconstructions and aims to simplify automation at PETRA III beamlines.

Key features:

- automated generation of `sbatch` scripts  
- dependency‑aware pipeline execution  
- filtering and selecting datasets by pattern  
- support for multiple working directories  
- optional targeting of GPU, “excellent”, or “strong” nodes  
- automatic partition selection for GPU jobs  

#### Commonly Used Parameters

Below is an overview of the most frequently used options  
(see the full help output with `submitslurm -h`):

- **`-w / --working-dir`**  
  Working subdirectory where the script is executed.  
  Default: `wd`.  
  Accepts:
  - a single directory (`-w wd`)
  - comma‑separated list (`-w wd1,wd2`)
  - repeated flags (`-w wd1 -w wd2 -w wd3`)

- **`-p / --pattern`**  
  Restrict selection to subdirectories whose names contain the given substring  
  (e.g. `--pattern 001`).

- **`--pipeline-step`**  
  Run only the specified pipeline step (e.g. 1, 2, 3…).  
  The step is executed only if all previous steps completed.

- **`-a / --slurmargs`**  
  Pass additional arguments directly to the generated `sbatch` script.

- **`-f / --partition`**  
  Select a SLURM partition (for example `com`, `comgpu`, etc.).

- **`-g / --gpu-nodes`**  
  Restrict allocation to GPU nodes.

- **`-e / --excellent-nodes`** and **`-s / --strong-nodes`**  
  Select special node categories (A100‑tier or strong CPU nodes).

## Acknowledgements

*laupy* is designed for high-throughput scientific workflows and has been applied to tomographic reconstruction pipelines at synchrotron beamlines, including large-scale and industrial datasets.

The development of this package was supported by [Hi ACTS Use Case Initiatives 2026](https://www.hi-acts.de/en/use-case-initiatives) within the project ***Advanced reconstruction pipeline for tomography experiments at PETRA III***.

## Target Environment

laupy is designed to run on HPC systems with a SLURM scheduler and a standard Python installation. These are the core requirements for deployment.

Development, testing, and validation are primarily performed on the Maxwell Cluster at DESY, which provides GPU-accelerated resources well suited for large-scale tomographic reconstruction and production-like workloads.

## Licensing

Unless otherwise specified in the source files, this project is licensed under the GNU General Public License v3.0.

Copyright (C) 2025-2026 Vojtěch Kulvait

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, version 3 of the License.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
