"""
Command line helpers shared by the `pipeline` and `submitslurm` scripts.
"""
import argparse
import os
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping

from laupy.flow import list_processing_unit_subdirs


def parse_comma_separated(value):
    """
    Parse a comma-separated string into a list of strings.

    Example:
    "dir1,dir2,dir3" -> ["dir1", "dir2", "dir3"]
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def add_common_arguments(parser, suppress_defaults=False, dry_run_help="Only print what would be done"):
    """Options selecting the root, working directories and processing units.

    With suppress_defaults=True (for subcommand parsers) the options only
    override values given before the subcommand when given explicitly.
    """
    def default(value):
        return argparse.SUPPRESS if suppress_defaults else value
    parser.add_argument("-d", "--root-dir", type=str, help="Root directory (defaults to current directory)", default=default(None))
    parser.add_argument("-w", "--working-dir", type=parse_comma_separated, action="append", required=False, help="One or more working directories. Can be comma-separated or repeated. Defaults to wd.", default=default(None))
    parser.add_argument("--slurm-dir", type=str, help="Directory with SLURM scripts, relative to the root dir (defaults to 'sbatch')", default=default("sbatch"))
    parser.add_argument("-p", "--pattern", type=str, help="Process only processing units whose name contains PATTERN", default=default(None))
    parser.add_argument("--dry-run", action="store_true", help=dry_run_help, default=default(False))
    parser.add_argument("--verbose", action="store_true", help="Print parsed arguments and detailed processing information", default=default(False))


@dataclass
class ProcessingUnitSubdir:
    """One processing unit: a subdirectory of a working directory (also called processing unit)."""
    name: str       # directory name, e.g. 17k_sample
    abs: str        # resolved absolute path
    rel: str        # path relative to the root directory, e.g. wd/17k_sample


@dataclass
class DirectoryTree:
    """Directories a pipeline/submitslurm call works on:

    root/                     ROOT DIR, the current directory after resolve_directory_tree()
        sbatch/               SLURM scripts (--slurm-dir)
        wd/                   working directories (-w)
            <subdir>/         processing unit subdirectories (filtered by -p)
    """
    root: Path
    sbatch_abs: Path
    sbatch_rel: str
    working_dirs_rel: List[str]
    processing_unit_subdirs: List[ProcessingUnitSubdir]


def resolve_directory_tree(ARG) -> DirectoryTree:
    """Change to the root directory and resolve the working directories, the SLURM
    script directory and the processing unit subdirectories selected by -w/-p.

    Exits with an error message when a directory is missing or no subdirectory matches.
    """
    root = Path.cwd() if ARG.root_dir is None else Path(ARG.root_dir)
    root = root.expanduser().resolve()
    os.chdir(root)
    working_dirs = ["wd"] if ARG.working_dir is None else [d for group in ARG.working_dir for d in group]
    wd_abs = [(Path(d) if Path(d).is_absolute() else root / d).resolve() for d in working_dirs]
    wd_rel = [os.path.relpath(path, root) for path in wd_abs]
    for path, rel in zip(wd_abs, wd_rel):
        if not path.is_dir():
            print(f"Working directory {rel} does not exist or is not a directory.", file=sys.stderr)
            sys.exit(1)
    sbatch = Path(ARG.slurm_dir)
    sbatch_abs = sbatch if sbatch.is_absolute() else root / sbatch
    sbatch_rel = os.path.relpath(sbatch_abs, root)
    sbatch_abs = sbatch_abs.resolve()
    if not sbatch_abs.is_dir():
        print(f"SLURM script directory {sbatch_rel} does not exist or is not a directory.", file=sys.stderr)
        sys.exit(1)
    processing_unit_subdirs = []
    for path in wd_abs:
        for subdir in list_processing_unit_subdirs(path, pattern=ARG.pattern):
            resolved = subdir.resolve()
            processing_unit_subdirs.append(ProcessingUnitSubdir(name=subdir.name, abs=str(resolved), rel=os.path.relpath(resolved, root)))
    if not processing_unit_subdirs:
        if ARG.pattern is not None:
            print("No subdirectories of %s match the specified pattern '%s'." % (", ".join(wd_rel), ARG.pattern), file=sys.stderr)
        else:
            print("No subdirectories of %s found." % (", ".join(wd_rel)), file=sys.stderr)
        sys.exit(1)
    return DirectoryTree(root=root, sbatch_abs=sbatch_abs, sbatch_rel=sbatch_rel, working_dirs_rel=wd_rel,
                         processing_unit_subdirs=processing_unit_subdirs)


# Utility functions for printing environment variables in a readable way, and for parsing --env NAME=VALUE arguments.
# Used to construct the --export=ALL,NAME=VALUE option for sbatch.
# Printed in verbose or normal mode of e.g. submitslurm
def environment_lines(env: Mapping[str, str], indent: str = "    ") -> List[str]:
    """CONDA_PREFIX and the PATH components of env, one per line.

    Components below CONDA_PREFIX are abbreviated, duplicates and missing
    directories are flagged.
    """
    conda_prefix = (env.get("CONDA_PREFIX") or "").rstrip("/")
    lines = [f"{indent}CONDA_PREFIX: {conda_prefix or '(not set)'}", f"{indent}PATH:"]
    seen = set()
    for component in env.get("PATH", "").split(os.pathsep):
        if not component:
            continue
        shown = component
        if conda_prefix and (component == conda_prefix or component.startswith(conda_prefix + "/")):
            shown = "$CONDA_PREFIX" + component[len(conda_prefix):]
        notes = []
        if component in seen:
            notes.append("duplicate")
        elif not os.path.isdir(component):
            notes.append("missing")
        seen.add(component)
        lines.append(f"{indent}    {shown}" + (f"  ({', '.join(notes)})" if notes else ""))
    return lines


_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_env_assignment(value: str):
    """argparse type for --env NAME=VALUE."""
    name, sep, val = value.partition("=")
    if not sep or not _ENV_NAME_RE.match(name):
        raise argparse.ArgumentTypeError(f"expected NAME=VALUE, got {value!r}")
    if "," in val:
        # sbatch --export separates variables by commas
        raise argparse.ArgumentTypeError(f"value of {name} must not contain a comma (sbatch --export uses commas as separator)")
    return name, val


@dataclass
class SubmissionEnvironment:
    """Environment of submitted jobs, in two layers.

    global_env: environment sbatch runs with; every job inherits it
        (--export=ALL). Defaults to the environment of this process.
    job_vars(): variables set per job on top of it: PIPELINE_SCRIPTNAME,
        PIPELINE_JOBNAME, LD_PRELOAD (cleared) and the `extra` ones
        (submitslurm --env NAME=VALUE), which may override the defaults.
    """
    global_env: Dict[str, str] = field(default_factory=lambda: dict(os.environ))
    extra: Dict[str, str] = field(default_factory=dict)

    def job_vars(self, script_name: str, job_name: str) -> Dict[str, str]:
        variables = {"PIPELINE_SCRIPTNAME": script_name, "PIPELINE_JOBNAME": job_name, "LD_PRELOAD": ""}
        variables.update(self.extra)
        return variables

    @staticmethod
    def export_option(job_vars: Mapping[str, str]) -> str:
        return "--export=ALL," + ",".join(f"{name}={value}" for name, value in job_vars.items())

    @staticmethod
    def describe_job_vars(job_vars: Mapping[str, str]) -> str:
        return " ".join(f"{name}={shlex.quote(value) if value else ''}" for name, value in job_vars.items())

    def global_lines(self) -> List[str]:
        return ["GLOBAL ENV (inherited by every job via --export=ALL):"] + environment_lines(self.global_env)


def print_arguments(ARG):
    print("Parsed arguments:")
    for arg_name, arg_value in vars(ARG).items():
        print(f"  {arg_name}: {arg_value}")
