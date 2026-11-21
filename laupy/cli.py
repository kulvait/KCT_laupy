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


# Options whose value may itself start with "-" (script arguments, environment values)
DASH_VALUE_OPTIONS = {"-a": "--slurmargs", "--slurmargs": "--slurmargs", "--env": "--env"}


def join_dash_values(argv: List[str], options: Mapping[str, str] = DASH_VALUE_OPTIONS) -> List[str]:
    """Join each option of `options` with its following value as --long=VALUE.

    argparse takes a value starting with "-" (e.g. -a "--verbose") for an option and fails;
    in the --long=VALUE form the value is never parsed. Tokens after "--" are left alone.
    """
    result, i = [], 0
    while i < len(argv):
        token = argv[i]
        if token == "--":
            result.extend(argv[i:])
            break
        if token in options and i + 1 < len(argv):
            result.append(f"{options[token]}={argv[i + 1]}")
            i += 2
            continue
        result.append(token)
        i += 1
    return result


def parse_env_assignment(value: str):
    """argparse type for --env NAME=VALUE."""
    name, sep, val = value.partition("=")
    if not sep or not _ENV_NAME_RE.match(name):
        raise argparse.ArgumentTypeError(f"expected NAME=VALUE, got {value!r}")
    if "," in val:
        # sbatch --export separates variables by commas
        raise argparse.ArgumentTypeError(f"value of {name} must not contain a comma (sbatch --export uses commas as separator)")
    return name, val


LAUPY_PYTHON = "LAUPY_PYTHON"


def check_python_environment(value: str) -> str:
    """Absolute path of a Conda/Mamba environment (a directory with bin/python) for LAUPY_PYTHON.

    Raises ValueError with the reason if the path is not usable.
    """
    path = os.path.abspath(os.path.expanduser(value))
    if "," in path:
        # sbatch --export separates variables by commas
        raise ValueError(f"path must not contain a comma (sbatch --export uses commas as separator): {path}")
    if not os.path.isdir(path):
        raise ValueError(f"not a directory: {path}")
    if not os.access(os.path.join(path, "bin", "python"), os.X_OK):
        raise ValueError(f"no executable bin/python in {path}")
    return path


def parse_python_environment(value: str) -> str:
    """argparse type for submitslurm --python PATH."""
    try:
        return check_python_environment(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e))


@dataclass
class SubmissionEnvironment:
    """Environment of submitted jobs, in two layers.

    global_env: environment sbatch runs with; every job inherits it
        (--export=ALL). Defaults to the environment of this process.
    job_vars(): variables set per job on top of it: PIPELINE_SCRIPTNAME,
        PIPELINE_JOBNAME, LD_PRELOAD (cleared) and the `extra` ones
        (submitslurm --env NAME=VALUE), which may override the defaults.
    vanilla: the environment of this process is not propagated (--export
        without ALL): a job gets only SLURM_* and the variables listed
        explicitly, the `base` ones (see vanilla_base_variables()) followed
        by the per-job ones (submitslurm --vanilla).
    """
    global_env: Dict[str, str] = field(default_factory=lambda: dict(os.environ))
    extra: Dict[str, str] = field(default_factory=dict)
    vanilla: bool = False
    base: Dict[str, str] = field(default_factory=dict)

    def job_vars(self, script_name: str, job_name: str) -> Dict[str, str]:
        variables = dict(self.base) if self.vanilla else {}
        variables.update({"PIPELINE_SCRIPTNAME": script_name, "PIPELINE_JOBNAME": job_name, "LD_PRELOAD": ""})
        variables.update(self.extra)
        return variables

    def export_option(self, job_vars: Mapping[str, str]) -> str:
        listed = [f"{name}={value}" for name, value in job_vars.items()]
        return "--export=" + ",".join(listed if self.vanilla else ["ALL"] + listed)

    @staticmethod
    def describe_job_vars(job_vars: Mapping[str, str]) -> str:
        return " ".join(f"{name}={shlex.quote(value) if value else ''}" for name, value in job_vars.items())

    def global_lines(self) -> List[str]:
        if self.vanilla:
            return ["GLOBAL ENV: not propagated to the jobs (--vanilla); they get only the listed variables"]
        return ["GLOBAL ENV (inherited by every job via --export=ALL):"] + environment_lines(self.global_env)


VANILLA_SYSTEM_PATH = ["/usr/local/bin", "/usr/bin", "/bin"]


def parse_job_path(value: str) -> List[str]:
    """argparse type for submitslurm --job-path DIR[:DIR...]: absolute directories, in order."""
    directories = []
    for directory in value.split(os.pathsep):
        if not directory:
            continue
        directory = os.path.abspath(os.path.expanduser(directory))
        if "," in directory:
            # sbatch --export separates variables by commas
            raise argparse.ArgumentTypeError(f"directory must not contain a comma (sbatch --export uses commas as separator): {directory}")
        directories.append(directory)
    return directories


def job_path_variable(job_path: List[str], global_env: Mapping[str, str] = None) -> str:
    """PATH of a non-vanilla job with --job-path: the job path directories before the inherited PATH.

    Raises ValueError if the inherited PATH contains a comma (the --export separator).
    """
    inherited = (global_env if global_env is not None else os.environ).get("PATH", "")
    if "," in inherited:
        raise ValueError("the inherited PATH contains a comma (sbatch --export uses commas as separator)")
    return os.pathsep.join(dict.fromkeys(job_path + [d for d in inherited.split(os.pathsep) if d]))


def vanilla_base_variables(root, sbatch_dir, global_env: Mapping[str, str] = None, job_path: List[str] = None) -> Dict[str, str]:
    """Variables a --vanilla job gets besides the per-job ones.

    PATH: root/bin, the SLURM script directory, the job_path directories (--job-path), the
    directory of initGanrec (looked up on the PATH of global_env, so the recipes can
    `source initGanrec`) and the system directories.
    HOME, USER, LOGNAME: from the password database of the submitting user (if the UID has no
    entry, from Path.home() and getpass.getuser()).
    Raises ValueError if a value contains a comma (the --export separator).
    """
    import pwd
    import shutil
    path = [str(Path(root) / "bin"), str(sbatch_dir)] + list(job_path or [])
    init_ganrec = shutil.which("initGanrec", path=(global_env if global_env is not None else os.environ).get("PATH", ""))
    if init_ganrec:
        path.append(os.path.dirname(os.path.abspath(init_ganrec)))
    path += VANILLA_SYSTEM_PATH
    try:
        account = pwd.getpwuid(os.getuid())
        home, user = account.pw_dir, account.pw_name
    except KeyError:
        # UID without a password database entry (e.g. a container)
        import getpass
        home, user = str(Path.home()), getpass.getuser()
    variables = {"PATH": os.pathsep.join(dict.fromkeys(path)), "HOME": home, "USER": user, "LOGNAME": user}
    for name, value in variables.items():
        if "," in value:
            raise ValueError(f"{name} must not contain a comma (sbatch --export uses commas as separator): {value}")
    return variables


def print_arguments(ARG):
    print("Parsed arguments:")
    for arg_name, arg_value in vars(ARG).items():
        print(f"  {arg_name}: {arg_value}")


def add_node_source_arguments(parser, offline=False):
    """--list FILE, --no-list, --feature-provider (and --offline): where laupy.maxwell takes the nodes from."""
    from laupy import maxwell
    parser.add_argument("--list", metavar="FILE", dest="node_table",
                        help="Node table (maxwell.tsv format) to use as it is instead of the app-managed one "
                             f"(${maxwell.NODE_TABLE_ENV} or {maxwell.DEFAULT_NODE_TABLE})")
    parser.add_argument("--no-list", action="store_true",
                        help="Do not use any node table: the hardware comes from the SLURM node features only "
                             "(implies --feature-provider slurm; a slower query)")
    parser.add_argument("--feature-provider", choices=maxwell.FEATURE_PROVIDERS, default=None,
                        help="Where the hardware (CPU/GPU models, classes) comes from. 'app-managed-list' (default): "
                             f"the node table {maxwell.DEFAULT_NODE_TABLE}, unpacked from the laupy package or created "
                             f"from SLURM when missing and refreshed from SLURM when older than "
                             f"{maxwell.MANAGED_LIST_MAX_AGE_DAYS} days; SLURM is only asked for node states (fast). "
                             "'list': the node table as it is, SLURM only for node states. 'slurm': the SLURM node "
                             "features on every call (seconds), completed from the node table")
    if offline:
        parser.add_argument("--offline", action="store_true",
                            help="Do not query SLURM: describe the node table alone")

def apply_node_source_arguments(parser, args):
    """Configure laupy.maxwell from add_node_source_arguments() options (parser.error on conflicts)."""
    from laupy import maxwell
    offline = getattr(args, "offline", False)
    provider = args.feature_provider or maxwell.DEFAULT_FEATURE_PROVIDER
    if args.no_list:
        if args.node_table or offline or args.feature_provider not in (None, "slurm"):
            others = "--list, --offline or a table feature provider" if hasattr(args, "offline") else "--list or a table feature provider"
            parser.error(f"--no-list ignores node tables, it cannot be combined with {others}")
        provider = "slurm"
    if args.node_table and not os.path.isfile(os.path.expanduser(args.node_table)):
        parser.error(f"--list: node table {args.node_table} does not exist")
    maxwell.set_node_table(args.node_table, live=not offline, use_table=not args.no_list, feature_provider=provider)

def print_node_notes(file=None):
    """Print and clear laupy.maxwell.NODE_NOTES (node table installed, created, refreshed)."""
    from laupy import maxwell
    for note in maxwell.NODE_NOTES:
        print(note, file=file)
    maxwell.NODE_NOTES.clear()
