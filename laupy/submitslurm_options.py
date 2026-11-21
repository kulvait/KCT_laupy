"""The submitslurm option set as a reusable component.

A helper for programs that compose submitslurm calls (the lautools submission context
editor): the parser, a description of every option, splitting a call into options,
merging a call with context options, and a preview of the job environment.

submitslurm (laupy/scripts/submitslurm.py) parses its command line with build_parser(), so
options are added here and every consumer sees the same option set.
"""
from __future__ import annotations

import argparse
import os
import shlex
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from laupy.cli import (
    DASH_VALUE_OPTIONS,
    LAUPY_PYTHON,
    SubmissionEnvironment,
    VANILLA_SYSTEM_PATH,
    WORKSPACE_PATH_LABELS,
    add_common_arguments,
    add_node_source_arguments,
    check_python_environment,
    join_dash_values,
    job_path_variable,
    parse_comma_separated,
    parse_env_assignment,
    parse_job_path,
    parse_python_environment,
    prepend_path_directories,
    vanilla_base_variables,
    workspace_path_directories,
)

STATE_CHOICES = ["NONE", "PENDING", "RUNNING", "REQUEUED", "COMPLETING", "FAILED", "CANCELLED", "TIMEOUT", "COMPLETED"]

# Options that belong to one submission (the script and how it is placed in the DAG)
CALL_DESTS = {"slurmargs", "pipeline_step", "dependencies", "scriptName"}
# Pipeline directory maintenance actions (no submission)
ACTION_DESTS = {"create_pipeline_only", "clean_dag", "delete_pipeline_only"}
# Options whose values from the context and from the call are combined instead of replaced
# (--env: the call's value of the same NAME wins, as the later --env)
MERGED_DESTS = {"env"}


def build_parser() -> argparse.ArgumentParser:
    """The submitslurm parser, used by submitslurm itself."""
    parser = argparse.ArgumentParser(prog="submitslurm", description="Submit jobs to SLURM using laupy")
    add_common_arguments(parser, dry_run_help="Simulate the submission without actually submitting jobs or creating files")
    parser.add_argument("-a", "--slurmargs", type=str, help="Additional SLURM arguments")
    parser.add_argument("-f", "--partition", type=str, help="Partition to use (defaults to 'allcpu,allgpu')", default="allcpu,allgpu")
    parser.add_argument("-n", "--nodelist", type=str, help="Specify node list")
    parser.add_argument("-o", "--oversubscribe", action="store_true", help="Allow oversubscription")
    parser.add_argument("-t", "--delaytime", type=int, help="Delay time in seconds before the next submit", default=0)
    parser.add_argument('-g', '--gpu-nodes', action='store_true', help="Select only nodes with GPUs")
    parser.add_argument("-e", "--excellent-nodes", action="store_true", help="Use excellent nodes only")
    parser.add_argument("-s", "--strong-nodes", action="store_true", help="Use strong nodes only")
    parser.add_argument("-x", "--exclude-weak-nodes", action="store_true", help="Exclude weak nodes")
    add_node_source_arguments(parser)
    parser.add_argument("--vanilla", action="store_true", help=f"Do not propagate the submitting environment to the jobs (--export without ALL). A job gets only SLURM_* and explicit variables: PATH (ROOT/bin, ROOT/script, the SLURM script directory, the directory of initGanrec, /usr/local/bin:/usr/bin:/bin), HOME, USER, LOGNAME, PIPELINE_SCRIPTNAME, PIPELINE_JOBNAME, LD_PRELOAD=, --env and --python ({LAUPY_PYTHON} of the submitting shell is not used). To test submissions independently of the user environment.")
    parser.add_argument("--job-path", type=parse_job_path, metavar="DIR[:DIR...]", help="Directories for the PATH of the jobs, after ROOT/bin, ROOT/script and the SLURM script directory (which always lead the job PATH) and, without --vanilla, before the inherited PATH.")
    parser.add_argument("--python", type=parse_python_environment, metavar="PATH", help=f"Python (Conda/Mamba) environment of the jobs: exported as {LAUPY_PYTHON}=PATH, which initGanrec activates instead of its default, and recorded in the pipeline entry so resubmissions use it too. Without --python, {LAUPY_PYTHON} of the submitting shell is used if set.")
    parser.add_argument("--env", type=parse_env_assignment, action="append", default=[], metavar="NAME=VALUE", help="Set an environment variable for every submitted job (repeatable); overrides the defaults PIPELINE_SCRIPTNAME, PIPELINE_JOBNAME and LD_PRELOAD=. PATH=... replaces the computed job PATH, ROOT/bin, ROOT/script and the SLURM script directory are still put first. Shown with --verbose.")
    scheduling_group = parser.add_mutually_exclusive_group()
    scheduling_group.add_argument("--pipeline-step", type=str, default="-1", help="Pipeline step number or DAG node name. Uses only explicit DAG dependencies. Default is -1, which are not put or evaluated against DAG.")
    parser.add_argument("--dependencies", type=parse_comma_separated, action="append", default=None, metavar="NODE[,NODE...]", help="Dependencies for non numeric --pipeline-step/DAG node, comma-separated or repeated. Can also be numeric pipeline steps (e.g., 0,1,2), numeric pipeline steps can not have explicit --dependencies. Default is None.")
    parser.add_argument("--skip-range", type=str, choices=["none", "cmd", "jobname", "step"], default="cmd", help="Scope of entries of the same node that make the new submission be skipped: 'none' (never skip), 'cmd' (same command), 'jobname' (same SLURM job name), 'step' (any entry of the node). Default is 'cmd'.")
    parser.add_argument("--skip-state", nargs="+", choices=STATE_CHOICES, default=["PENDING", "RUNNING", "REQUEUED"], help="SLURM job states to yield to command submitted in the same step with coresponging state. Default is PENDING, RUNNING, REQUEUED.")
    parser.add_argument("--retire-range", type=str, choices=["none", "cmd", "step"], default="cmd", help="Scope of retiring entries on new submission: 'none' (retire nothing), 'cmd' (retire only entries with the same command and step), 'step' (retire all entries with the same step regardless of command). Default is 'cmd'.")
    parser.add_argument("--retire-state", nargs="+", choices=STATE_CHOICES, default=["FAILED", "CANCELLED", "TIMEOUT"], help="SLURM job states that trigger retiring of entries in the same step on new submission with --retire-range set to 'cmd' or 'step'. Default is FAILED, CANCELLED, TIMEOUT.")
    parser.add_argument("--cancel-range", type=str, choices=["none", "cmd", "step"], default="none", help="Scope of cancelling active jobs on new submission: 'none' (cancel nothing), 'cmd' (cancel only jobs with the same command and step), 'step' (cancel all jobs with the same step regardless of command). Default is 'none'.")
    parser.add_argument("--cancel-state", nargs="+", choices=STATE_CHOICES, default=["NONE"], help="SLURM job states that trigger cancellation of active jobs in the same step on new submission with --cancel-range set to 'cmd' or 'step'. Default is NONE (no cancellations).")
    pipeline_group = parser.add_mutually_exclusive_group()
    pipeline_group.add_argument("--create-pipeline-only", action="store_true", help="Create only the pipeline directory and exit (scriptName not required); same as `pipeline create`.")
    pipeline_group.add_argument("--clean-dag", action="store_true", help="Forget the pipeline entries and exit (scriptName not required); same as `pipeline clean`.")
    pipeline_group.add_argument("--delete-pipeline-only", action="store_true", help="Delete the pipeline directory and exit (scriptName not required); same as `pipeline delete`.")
    parser.add_argument("scriptName", type=str, nargs="?", help="Script to execute in each working directory")
    return parser


# ---------------------------------------------------------------- option descriptions

@dataclass(frozen=True)
class OptionSpec:
    """One submitslurm option, described for building an editor.

    kind: "flag" (no value), "value" (one value), "choice" (one of choices),
          "multi_choice" (one or more of choices), "append" (repeatable, one value each).
    category: "context" (applies to every submission), "call" (per submission),
              "action" (pipeline maintenance, no submission).
    """
    dest: str
    option_strings: Tuple[str, ...]
    kind: str
    category: str
    help: str = ""
    metavar: Optional[str] = None
    choices: Tuple[str, ...] = ()
    default: object = None

    @property
    def name(self) -> str:
        """The option string used when composing calls (the long one)."""
        longs = [s for s in self.option_strings if s.startswith("--")]
        return longs[0] if longs else self.option_strings[0]


def _kind(action: argparse.Action) -> str:
    if action.nargs == 0:
        return "flag"
    if isinstance(action, argparse._AppendAction):
        return "append"
    if action.choices and action.nargs in ("+", "*"):
        return "multi_choice"
    if action.choices:
        return "choice"
    return "value"


def option_specs(parser: argparse.ArgumentParser = None) -> List[OptionSpec]:
    """All optional arguments of the parser (no help, no positional), in definition order."""
    parser = parser or build_parser()
    specs = []
    for action in parser._actions:
        if not action.option_strings or isinstance(action, argparse._HelpAction):
            continue
        category = "call" if action.dest in CALL_DESTS else "action" if action.dest in ACTION_DESTS else "context"
        specs.append(OptionSpec(
            dest=action.dest, option_strings=tuple(action.option_strings), kind=_kind(action), category=category,
            help=(action.help or "").replace("%%", "%"), metavar=action.metavar if isinstance(action.metavar, str) else None,
            choices=tuple(action.choices or ()), default=action.default))
    return specs


def context_option_specs(parser: argparse.ArgumentParser = None) -> List[OptionSpec]:
    """The options a context may set for every submission."""
    return [spec for spec in option_specs(parser) if spec.category == "context"]


# ---------------------------------------------------------------- splitting and merging calls

@dataclass
class ArgumentGroup:
    """An option with its value tokens as written in the call, or a positional token (dest None)."""
    dest: Optional[str]
    tokens: List[str]


def split_arguments(argv: Sequence[str], parser: argparse.ArgumentParser = None) -> List[ArgumentGroup]:
    """Split a submitslurm argument list into options with their values, like argparse reads it.

    Values of -a/--slurmargs/--env are taken even when they start with "-" (as submitslurm
    does, see laupy.cli.join_dash_values). Unknown options are kept as positional groups.
    """
    parser = parser or build_parser()
    actions = parser._option_string_actions
    groups: List[ArgumentGroup] = []
    i = 0
    argv = list(argv)
    while i < len(argv):
        token = argv[i]
        if token == "--":
            groups.extend(ArgumentGroup(None, [t]) for t in argv[i:])
            break
        name, _, attached = token.partition("=") if token.startswith("--") else (token, "", "")
        action = actions.get(name)
        if action is None and token.startswith("-") and not token.startswith("--") and len(token) > 2:
            # -fallgpu: short option with the value attached
            action = actions.get(token[:2])
            if action is not None and action.nargs != 0:
                groups.append(ArgumentGroup(action.dest, [token]))
                i += 1
                continue
            action = None
        if action is None or not token.startswith("-"):
            groups.append(ArgumentGroup(None, [token]))
            i += 1
            continue
        if action.nargs == 0 or "=" in token:
            groups.append(ArgumentGroup(action.dest, [token]))
            i += 1
            continue
        values = []
        j = i + 1
        if name in DASH_VALUE_OPTIONS:
            values = argv[j:j + 1]
            j += len(values)
        elif action.nargs in ("+", "*"):
            while j < len(argv) and not argv[j].startswith("-"):
                values.append(argv[j])
                j += 1
        elif j < len(argv) and not argv[j].startswith("-"):
            values = [argv[j]]
            j += 1
        groups.append(ArgumentGroup(action.dest, [token] + values))
        i = j
    return groups


def merge_arguments(call: Sequence[str], defaults: Mapping[str, List[str]] = None, forced: Set[str] = frozenset(),
                    extra: Sequence[str] = (), parser: argparse.ArgumentParser = None) -> List[str]:
    """A call merged with context options.

    defaults: dest -> tokens (option and values, e.g. ["--partition", "allgpu"]); used when
        the call does not give that option. For MERGED_DESTS (--env) they are always used and
        the call's own values come later, so they win for the same NAME.
    forced: dests whose context tokens replace the call's.
    extra: free tokens added after the context options (before the call's own).
    Context tokens come first, then the call's options in their order, then the call's
    positional arguments (the script) after "--", so that an option with several values
    (e.g. --skip-state FAILED) cannot take the script name as a value.
    """
    parser = parser or build_parser()
    defaults = defaults or {}
    groups = split_arguments(call, parser)
    present = {group.dest for group in groups if group.dest is not None}
    kept = [group for group in groups if not (group.dest in forced and group.dest in defaults)]
    options = [token for group in kept if group.dest is not None for token in group.tokens]
    positionals = [token for group in kept if group.dest is None for token in group.tokens]
    if positionals and positionals[0] != "--":
        positionals = ["--"] + positionals
    context: List[str] = []
    for dest, tokens in defaults.items():
        if dest in forced or dest in MERGED_DESTS or dest not in present:
            context.extend(tokens)
    return context + list(extra) + options + positionals


def parse_merged(argv: Sequence[str], parser: argparse.ArgumentParser = None) -> argparse.Namespace:
    """Parse arguments as submitslurm does; raises ValueError instead of exiting."""
    parser = parser or build_parser()

    def error(message):
        raise ValueError(message)
    parser.error = error
    return parser.parse_args(join_dash_values(list(argv)))


# ---------------------------------------------------------------- job environment preview

@dataclass
class EnvironmentPreview:
    """What a job of a submission would get (for display; computed like submitslurm does).

    variables: name -> (value, source); path: [(directory, source)]; export: the --export option.
    inherits: True when the submitting environment is propagated (--export=ALL).
    """
    variables: Dict[str, Tuple[str, str]] = field(default_factory=dict)
    path: List[Tuple[str, str]] = field(default_factory=list)
    export: str = ""
    inherits: bool = True
    warnings: List[str] = field(default_factory=list)


def preview_environment(args: argparse.Namespace, root, global_env: Mapping[str, str] = None,
                        script_name: str = "SCRIPT.sbatch", job_name: str = "SCRIPT_UNIT") -> EnvironmentPreview:
    """The environment a job would get with these submitslurm arguments, with the source of each part."""
    global_env = dict(os.environ if global_env is None else global_env)
    root = Path(root)
    sbatch = Path(args.slurm_dir)
    sbatch_abs = sbatch if sbatch.is_absolute() else root / sbatch
    preview = EnvironmentPreview(inherits=not args.vanilla)
    extra = dict(args.env or [])
    environment = SubmissionEnvironment(global_env=global_env, extra=dict(extra), vanilla=args.vanilla)

    # LAUPY_PYTHON as submitslurm resolves it
    python, python_source = None, None
    if args.python is not None:
        python, python_source = args.python, "--python"
    elif extra.get(LAUPY_PYTHON):
        python, python_source = extra[LAUPY_PYTHON], f"--env {LAUPY_PYTHON}"
    elif not args.vanilla and global_env.get(LAUPY_PYTHON):
        python, python_source = global_env[LAUPY_PYTHON], "submitting shell"
    if python is not None:
        try:
            python = check_python_environment(python)
        except ValueError as e:
            preview.warnings.append(f"{LAUPY_PYTHON} from {python_source}: {e}")
        environment.extra[LAUPY_PYTHON] = python

    sources: Dict[str, str] = {}
    path_sources: List[Tuple[str, str]] = []
    if args.vanilla:
        environment.base = vanilla_base_variables(root, sbatch_abs, global_env, args.job_path)
        sources.update({name: "vanilla default" for name in environment.base})
        init_dir = shutil.which("initGanrec", path=global_env.get("PATH", ""))
        init_dir = os.path.dirname(os.path.abspath(init_dir)) if init_dir else None
        labels = dict(zip(workspace_path_directories(root, sbatch_abs), WORKSPACE_PATH_LABELS))
        labels.update({d: "--job-path" for d in args.job_path or []})
        if init_dir:
            labels.setdefault(init_dir, "initGanrec")
        labels.update({d: "system" for d in VANILLA_SYSTEM_PATH if d not in labels})
        path_sources = [(d, labels.get(d, "")) for d in environment.base["PATH"].split(os.pathsep)]
        if not init_dir:
            preview.warnings.append("initGanrec is not on the submitting PATH; recipes that source it will fail")
    leading = workspace_path_directories(root, sbatch_abs)
    leading_labels = list(zip(leading, WORKSPACE_PATH_LABELS))
    if "PATH" in extra:
        environment.extra["PATH"] = prepend_path_directories(leading, extra["PATH"])
        path_sources = leading_labels + [(d, "--env PATH") for d in extra["PATH"].split(os.pathsep) if d and d not in leading]
    elif not args.vanilla:
        job_path = args.job_path or []
        environment.extra["PATH"] = job_path_variable(job_path, global_env, leading)
        labels = dict(leading_labels)
        for d in job_path:
            labels.setdefault(d, "--job-path")
        path_sources = [(d, labels.get(d, "inherited")) for d in environment.extra["PATH"].split(os.pathsep)]
    for directory in args.job_path or []:
        if not os.path.isdir(directory):
            preview.warnings.append(f"--job-path directory does not exist: {directory}")

    job_vars = environment.job_vars(script_name, job_name)
    for name, value in job_vars.items():
        if name in extra and name != "PATH":
            source = f"--env {name}"
        elif name == LAUPY_PYTHON:
            source = python_source or ""
        elif name in ("PIPELINE_SCRIPTNAME", "PIPELINE_JOBNAME", "LD_PRELOAD"):
            source = "per job"
        elif name == "PATH" and "PATH" in extra:
            source = "ROOT/bin, ROOT/script, SLURM script directory + --env PATH"
        elif name == "PATH" and not args.vanilla:
            source = "ROOT/bin, ROOT/script, SLURM script directory" + (", --job-path" if args.job_path else "") + " + inherited"
        else:
            source = sources.get(name, "")
        preview.variables[name] = (value, source)
    preview.path = path_sources
    preview.export = environment.export_option(job_vars)
    return preview


def quote_arguments(argv: Sequence[str]) -> str:
    return " ".join(shlex.quote(token) for token in argv)
