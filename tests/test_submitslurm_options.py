"""laupy.submitslurm_options: the submitslurm parser, splitting and merging calls.

Run with: python -m pytest tests/test_submitslurm_options.py  (or python tests/test_submitslurm_options.py)
"""
import argparse
import os
import sys
from unittest import mock

from laupy import cli
from laupy import submitslurm_options as so


class _Captured(Exception):
    pass


def submitslurm_parser():
    """The parser submitslurm.main() builds (stopped at its parse_args call)."""
    from laupy.scripts import submitslurm
    captured = {}

    def parse_args(self, args=None, namespace=None):
        captured["parser"] = self
        raise _Captured()
    with mock.patch.object(argparse.ArgumentParser, "parse_args", parse_args), mock.patch.object(sys, "argv", ["submitslurm"]):
        try:
            submitslurm.main()
        except _Captured:
            pass
    return captured["parser"]


def describe(parser):
    return [(tuple(a.option_strings), a.dest, a.nargs, a.default, tuple(a.choices or ()), type(a).__name__, a.help)
            for a in parser._actions]


def test_submitslurm_uses_build_parser():
    # submitslurm must not add options of its own outside build_parser()
    assert describe(so.build_parser()) == describe(submitslurm_parser())


def test_categories():
    specs = {spec.dest: spec for spec in so.option_specs()}
    assert specs["slurmargs"].category == "call"
    assert specs["clean_dag"].category == "action"
    assert specs["vanilla"].category == "context" and specs["vanilla"].kind == "flag"
    assert specs["skip_state"].kind == "multi_choice"
    assert specs["env"].kind == "append"
    assert specs["partition"].name == "--partition"


def test_split_arguments():
    groups = so.split_arguments(["--pipeline-step", "7", "-a", "--bin 2 a b", "--skip-state", "FAILED", "RUNNING",
                                 "--partition=allgpu", "-fcpu", "--env", "A=-x", "--vanilla", "00bin.sbatch"])
    assert [(g.dest, g.tokens) for g in groups] == [
        ("pipeline_step", ["--pipeline-step", "7"]), ("slurmargs", ["-a", "--bin 2 a b"]),
        ("skip_state", ["--skip-state", "FAILED", "RUNNING"]), ("partition", ["--partition=allgpu"]),
        ("partition", ["-fcpu"]), ("env", ["--env", "A=-x"]), ("vanilla", ["--vanilla"]), (None, ["00bin.sbatch"])]


def test_merge_arguments():
    defaults = {"partition": ["--partition", "allgpu"], "vanilla": ["--vanilla"], "env": ["--env", "A=1"],
                "working_dir": ["--working-dir", "wd"]}
    call = ["-f", "cpu", "-w", "wd2", "--env", "A=2", "s.sbatch"]
    merged = so.merge_arguments(call, defaults, forced={"partition"}, extra=["--verbose"])
    assert merged == ["--partition", "allgpu", "--vanilla", "--env", "A=1", "--verbose", "-w", "wd2", "--env", "A=2", "--", "s.sbatch"]
    args = so.parse_merged(merged)
    assert args.partition == "allgpu" and args.vanilla and dict(args.env)["A"] == "2" and args.working_dir == [["wd2"]]


def test_merge_multi_value_before_script():
    merged = so.merge_arguments(["s.sbatch", "--pipeline-step", "3"], {"skip_state": ["--skip-state", "FAILED"]})
    args = so.parse_merged(merged)
    assert args.skip_state == ["FAILED"] and args.scriptName == "s.sbatch" and args.pipeline_step == "3"


def test_parse_merged_raises():
    try:
        so.parse_merged(["--skip-range", "bad"])
    except ValueError as e:
        assert "skip-range" in str(e)
    else:
        raise AssertionError("no error")


def test_preview_vanilla(tmp_path):
    (tmp_path / "sbatch").mkdir()
    args = so.parse_merged(["--vanilla", "--job-path", "/opt/tools", "--env", "OMP_NUM_THREADS=8", "s.sbatch"])
    preview = so.preview_environment(args, tmp_path, global_env={"PATH": "/usr/bin", "LAUPY_PYTHON": "/x"})
    assert not preview.inherits
    assert [d for d, _ in preview.path][:4] == [str(tmp_path / "bin"), str(tmp_path / "script"), str(tmp_path / "sbatch"), "/opt/tools"]
    assert "LAUPY_PYTHON" not in preview.variables          # the shell's value is not used with --vanilla
    assert preview.variables["OMP_NUM_THREADS"] == ("8", "--env OMP_NUM_THREADS")
    assert preview.export.startswith("--export=PATH=")


def _commands(directory, *names):
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_text("#!/bin/sh\n")
        (directory / name).chmod(0o755)
    return str(directory)


def test_vanilla_keeps_command_directories(tmp_path):
    group = _commands(tmp_path / "group", "initEnv", "initPython")
    shadow = _commands(tmp_path / "shadow", "initEnv")
    syscripts = _commands(tmp_path / "syscripts", "rawTiffStructureToZarr.py")
    kct = _commands(tmp_path / "kct", "kct-pb2d-krylov")
    other = _commands(tmp_path / "other", "tool")
    # first occurrence of each command, in the order of the submitting PATH
    env = {"PATH": os.pathsep.join([other, kct, shadow, group, syscripts, "/usr/bin"])}
    kept, missing = cli.vanilla_kept_directories(env)
    assert list(kept) == [kct, shadow, syscripts] and kept[shadow] == ["initEnv"] and missing == []
    args = so.parse_merged(["--vanilla", "s.sbatch"])
    preview = so.preview_environment(args, tmp_path, global_env=env)
    assert [d for d, _ in preview.path][3:6] == [kct, shadow, syscripts]
    assert dict(preview.path)[kct] == "submitting PATH (kct-pb2d-krylov)"
    assert other not in preview.variables["PATH"][0] and group not in preview.variables["PATH"][0]
    assert preview.warnings == []
    # a missing command is reported
    preview = so.preview_environment(args, tmp_path, global_env={"PATH": group})
    assert any("rawTiffStructureToZarr.py, kct-pb2d-krylov" in w for w in preview.warnings)
    # configurable list
    assert list(cli.vanilla_kept_directories(env, ["tool"])[0]) == [other]


def test_vanilla_strict(tmp_path):
    group = _commands(tmp_path / "group", "initEnv")
    args = so.parse_merged(["--vanilla-strict", "--job-path", "/opt/tools", "s.sbatch"])
    preview = so.preview_environment(args, tmp_path, global_env={"PATH": group, "LAUPY_PYTHON": "/x"})
    assert not preview.inherits and not preview.export.startswith("--export=ALL")
    assert [d for d, _ in preview.path] == [str(tmp_path / "bin"), str(tmp_path / "script"), str(tmp_path / "sbatch"),
                                            "/opt/tools"] + cli.VANILLA_SYSTEM_PATH
    assert "LAUPY_PYTHON" not in preview.variables
    assert not any("not on the submitting PATH" in w for w in preview.warnings)


def test_preview_inherited(tmp_path):
    args = so.parse_merged(["--job-path", "/opt/tools", "s.sbatch"])
    preview = so.preview_environment(args, tmp_path, global_env={"PATH": "/usr/bin:/bin"})
    assert preview.inherits
    assert preview.path == [(str(tmp_path / "bin"), "ROOT/bin"), (str(tmp_path / "script"), "ROOT/script"),
                            (str(tmp_path / "sbatch"), "SLURM script directory"),
                            ("/opt/tools", "--job-path"), ("/usr/bin", "inherited"), ("/bin", "inherited")]
    assert preview.export.startswith("--export=ALL,")


def test_preview_root_bin_always_first(tmp_path):
    # without --job-path, and with an explicit --env PATH (also with --vanilla)
    root_bin, script, sbatch = str(tmp_path / "bin"), str(tmp_path / "script"), str(tmp_path / "sbatch")
    preview = so.preview_environment(so.parse_merged(["s.sbatch"]), tmp_path, global_env={"PATH": "/usr/bin"})
    assert preview.variables["PATH"][0] == f"{root_bin}:{script}:{sbatch}:/usr/bin"
    for vanilla in ([], ["--vanilla"]):
        args = so.parse_merged(vanilla + ["--env", f"PATH=/a:{root_bin}", "s.sbatch"])
        preview = so.preview_environment(args, tmp_path, global_env={"PATH": "/usr/bin"})
        assert preview.variables["PATH"][0] == f"{root_bin}:{script}:{sbatch}:/a"
        assert [s for _, s in preview.path] == ["ROOT/bin", "ROOT/script", "SLURM script directory", "--env PATH"]


if __name__ == "__main__":
    import tempfile
    from pathlib import Path
    for name, test in sorted(globals().items()):
        if name.startswith("test_"):
            if "tmp_path" in test.__code__.co_varnames[:test.__code__.co_argcount]:
                with tempfile.TemporaryDirectory() as d:
                    test(Path(d))
            else:
                test()
            print("ok", name)
