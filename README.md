
# laupy

**laupy** is a Python package designed to simplify and automate tomographic reconstruction workflows, with a strong focus on industrial‑scale datasets and high‑throughput processing on GPU‑accelerated HPC systems. It provides command‑line tools for dataset management, job submission, reconstruction orchestration, and diagnostics — all using open‑source technologies such as Python, BASH, OpenCL/C++, and SLURM.

---

## Motivation

The primary motivation behind **laupy** is to streamline and automate tomographic reconstruction pipelines for **industrial clients** at the **PETRA III synchrotron**, especially at **beamlines P05 and P07**, which are operated by **Helmholtz‑Zentrum Hereon**. These beamlines serve a broad set of industrial sectors, including automotive, aerospace, energy storage, additive manufacturing, and advanced materials R&D.

Industrial users typically require:

- **Fast and reliable reconstructions**
- **Minimal manual intervention**
- **Simple, reproducible workflows**
- **Automation that does not require deep scientific or computational expertise**

However, real-world tomography experiments often rely on fragmented or proprietary software, making the processing pipeline difficult to maintain, automate, and scale.

**laupy** addresses this by offering a fully open-source, modular pipeline that integrates seamlessly with existing beamline workflows and supports end‑to‑end automation — from raw data ingestion to reconstruction and reporting.


## Target Environment

While the package is fully portable and can run on any SLURM‑based HPC system, development and validation are primarily carried out on the:

- **Maxwell Cluster at DESY**

This environment is well suited for GPU‑accelerated tomographic reconstruction and allows extensive testing at production scale. Nevertheless, **laupy is not tied to Maxwell**, and it is intentionally designed to run on other clusters, Linux workstations, cloud environments, or laboratory computing facilities without modification.


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
cd KCT_denpy
pip install --user --upgrade -e .
```


### Upgrading the Package
To update the package, use

```bash
pip install --upgrade git+https://github.com/kulvait/KCT_denpy.git
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

The development of this package was supported by [Hi ACTS Use Case Initiatives 2026](https://www.hi-acts.de/en/use-case-initiatives) within the project ***Advanced reconstruction pipeline for tomography experiments at PETRA III***.

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
