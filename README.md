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
## Design Concepts
 
### Execution Units and Dependency Semantics

`laupy` tracks concrete **execution units** (entries), which currently correspond to SLURM jobs in the system.  
Each entry has its own scheduler identity (`job_id`) and execution metadata.

Dependencies are evaluated at two levels:

1. **Numeric hierarchy** (`0 < 1 < 2 < ...`) for simple staged priority ordering.
2. **Full DAG hierarchy** for logical entities requiring non-linear dependency structure.

A logical graph node may produce **multiple entries** (e.g., retries or resubmissions).  
This preserves dependency correctness at the node level while keeping full observability of individual executions at the entry level.

### Package Structure

- **`flow.py`**  
  Core workflow state and DAG persistence logic.  
  Important functions include:
  - `load_dag(...)` – load DAG/workflow state from persistent representation
  - `store_dag(...)` – store updated DAG/workflow state back to disk

- **`slurm.py`**  
  SLURM-facing execution integration layer (submission/tracking helpers used by pipeline execution units).

- **`graph_utils.py`**  
  Internal DAG utilities used by orchestration logic (validation/order/traversal), implemented without `networkx` to keep startup and command responsiveness fast.


**DAG ordering and tie-breaking**
`graph_utils.py` provides uses an internal lightweight directed-graph implementation for DAG validation, ancestor traversal, and topological sorting.

Topological sorting follows dependency constraints first.  
If multiple nodes are simultaneously ready (no direct dependency relation), `laupy` applies deterministic tie-breaking:

1. earlier node timestamp first (minimum timestamp across entries of the same node),
2. named nodes before numeric nodes on equal timestamp,
3. first appearance order in `dag.json` as final fallback.

Notes:
- Numeric graph nodes may exist without corresponding DAG entries (graph-only nodes).
- A node may have multiple entries (e.g., resubmissions); tie-breaking uses the earliest timestamp for node priority stability.
- This allows non-blocking helper steps (such as `warmup`) to remain dependency-correct while being consistently ordered at the top when eligible.

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

### `pipeline`

`pipeline` is the operational CLI for managing and inspecting workflow execution state stored in each dataset’s `pipeline/dag.json`, with SLURM-aware status updates.

It is designed for day-to-day pipeline operations around **existing execution entries** (job records), not only submission.  
In the current model, entries correspond to SLURM jobs and carry fields such as `job_id`, optional `slurm_id`, state metadata, and node/step information.

#### What `pipeline` does

- loads DAG/entry state from disk (`load_dag`)
- refreshes entry status from SLURM (`update_dag_entries`)
- persists state changes (`save_dag`)
- can clean DAG bookkeeping (`clean_dag`)
- supports selective operations across one or more working directories

#### Key behavior from implementation

- Works per dataset subdirectory (optionally filtered by `--pattern`).
- Supports multiple working directories via repeated `-w` or comma-separated lists.
- Uses `pipeline` folder convention under each dataset:
  - `pipeline/dag.json` (state)
  - `pipeline/log/` (logs)
- For dependency safety, active upstream jobs can be checked (pending/running/requeued/completing) before dependent work proceeds.

#### Main modes

- `--create`  
  Create pipeline artifacts (ensures `pipeline/log` exists) without submitting work.

- `--delete`  
  Remove pipeline artifacts for a dataset (pipeline files and `pipeline/log`, with safe directory handling).

- `--clean-dag`  
  Clean DAG entries in `pipeline/dag.json` to resolve stale/failed dependency bookkeeping.

#### Subcommands

##### `pipeline status`
Show status of all pipeline entries with SLURM state coloring and runtime context.

- optional `--show-completed`
- optional `--show-retired`
- prints node/step/job identifiers, state, and for running jobs also elapsed/time-limit/node list
- includes pending reason when available

##### `pipeline log <id>`
Show stored SLURM log files for a specific DAG job identifier.

- `<id>` matches either `job_id` or `slurm_id`
- `--stdout` and/or `--stderr` to control output
- if neither flag is set, both are shown by default

##### `pipeline retire`
Mark selected entries as retired in DAG metadata (typically failed terminal jobs).

Selection:
- `--all` (default if no selector given)
- `--job <ids...>`
- `--step <steps...>`

Filtering:
- `--state ...` with defaults: `FAILED CANCELLED TIMEOUT`

Effect:
- sets `retired: true` for matching entries and saves DAG

##### `pipeline cancel`
Cancel active SLURM jobs represented by DAG entries.

Selection:
- `--all` (default if no selector given)
- `--job <ids...>`
- `--step <steps...>`

Filtering:
- `--state ...` with defaults: `PENDING RUNNING REQUEUED COMPLETING`

Effect:
- calls `scancel` on matching `slurm_id`/resolved job id
- writes updated state back to DAG

#### Common options

- `-d, --root-dir` root path (defaults to current directory)
- `-w, --working-dir` one or more working directories (default `wd`)
- `--slurm-dir` directory containing SLURM scripts (default `sbatch`)
- `-p, --pattern` process only matching dataset subdirectories
- `--pipeline-step` target specific pipeline step number
- `--dry-run` simulate actions (where applicable)
- `--verbose` print parsed arguments and detailed processing info

#### Example commands

```bash
# Show active pipeline status for all datasets in wd
pipeline status -w wd

# Show status including completed and retired entries
pipeline status -w wd --show-completed --show-retired

# Show both stdout/stderr logs for job id 10452
pipeline log 10452 -w wd

# Retire all failed/cancelled/timeout jobs (default states)
pipeline retire -w wd --all

# Retire only specific pipeline steps
pipeline retire -w wd --step 3 4 --state FAILED TIMEOUT

# Cancel all currently active jobs
pipeline cancel -w wd --all

# Cancel selected jobs only
pipeline cancel -w wd --job 12345 12346

# Clean DAG metadata, logs will be preserved but no longer referenced
pipeline --clean-dag -w wd

# Initialize pipeline directory structure
pipeline --create -w wd

# Remove pipeline data and logs for a dataset, execution history will be lost
pipeline --delete -w wd
```


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
