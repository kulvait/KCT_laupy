"""
Command line helpers shared by the `pipeline` and `submitslurm` scripts.
"""
import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

from laupy.flow import execution_unit_dirs


def parse_comma_separated(value):
    """
    Parse a comma-separated string into a list of strings.

    Example:
    "dir1,dir2,dir3" -> ["dir1", "dir2", "dir3"]
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def add_common_arguments(parser, suppress_defaults=False, dry_run_help="Only print what would be done"):
    """Options selecting the root, working directories and execution units.

    With suppress_defaults=True (for subcommand parsers) the options only
    override values given before the subcommand when given explicitly.
    """
    def default(value):
        return argparse.SUPPRESS if suppress_defaults else value
    parser.add_argument("-d", "--root-dir", type=str, help="Root directory (defaults to current directory)", default=default(None))
    parser.add_argument("-w", "--working-dir", type=parse_comma_separated, action="append", required=False, help="One or more working directories. Can be comma-separated or repeated. Defaults to wd.", default=default(None))
    parser.add_argument("--slurm-dir", type=str, help="Directory with SLURM scripts, relative to the root dir (defaults to 'sbatch')", default=default("sbatch"))
    parser.add_argument("-p", "--pattern", type=str, help="Process only execution units whose name contains PATTERN", default=default(None))
    parser.add_argument("--dry-run", action="store_true", help=dry_run_help, default=default(False))
    parser.add_argument("--verbose", action="store_true", help="Print parsed arguments and detailed processing information", default=default(False))


@dataclass
class ExecutionUnit:
    name: str       # directory name, e.g. 17k_sample
    abs: str        # resolved absolute path
    rel: str        # path relative to the root directory, e.g. wd/17k_sample


@dataclass
class Layout:
    root: Path
    sbatch_abs: Path
    sbatch_rel: str
    working_dirs_rel: List[str]
    units: List[ExecutionUnit]


def resolve_layout(ARG) -> Layout:
    """Change to the root directory and resolve working directories, SLURM script directory and execution units.

    Exits with an error message when a directory is missing or no execution unit matches.
    """
    root = Path.cwd() if ARG.root_dir is None else Path(ARG.root_dir)
    root = root.expanduser().resolve()
    print(f"Changing to root directory: {root}")
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
    units = []
    for path in wd_abs:
        for unit in execution_unit_dirs(path, pattern=ARG.pattern):
            resolved = unit.resolve()
            units.append(ExecutionUnit(name=unit.name, abs=str(resolved), rel=os.path.relpath(resolved, root)))
    if not units:
        if ARG.pattern is not None:
            print("No subdirectories of %s match the specified pattern '%s'." % (", ".join(wd_rel), ARG.pattern), file=sys.stderr)
        else:
            print("No subdirectories of %s found." % (", ".join(wd_rel)), file=sys.stderr)
        sys.exit(1)
    return Layout(root=root, sbatch_abs=sbatch_abs, sbatch_rel=sbatch_rel, working_dirs_rel=wd_rel, units=units)


def print_arguments(ARG):
    print("Parsed arguments:")
    for arg_name, arg_value in vars(ARG).items():
        print(f"  {arg_name}: {arg_value}")
