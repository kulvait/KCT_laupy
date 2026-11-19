"""
Management of PIPELINE_ENTRIES, the scheduled SLURM jobs of pipelines.

PIPELINE_ENTRIES of a processing unit subdir are stored in <processing_unit_subdir>/pipeline/dag.json
(see laupy.flow). Each entry is one SLURM job attached to a DAG node; this
module covers their lifecycle after submission:

    submitted -> (refresh from SLURM) -> cancelled / finished -> retired

Shared by the `pipeline` and `submitslurm` CLIs and by lautools-browser.

Batched SLURM queries
---------------------
PipelineEntriesStore holds the PIPELINE_ENTRIES of many processing unit subdirs.
refresh() collects the entries that need fresh SLURM info from all subdirs and
queries them together, so the number of sacct/squeue calls does not grow with
the number of processing unit subdirs (see laupy.slurm.QUERY_CHUNK_SIZE). Entries
whose stored state is final are not queried again, and store() writes only
subdirs that changed.

Typical use:

    pipeline_entries_store = PipelineEntriesStore(subdirs)  # load_pipeline_entries() of every subdir
    pipeline_entries_store.refresh()                          # one batched SLURM query for all subdirs
    for subdir, PIPELINE_ENTRIES in pipeline_entries_store.items():
        ...                                                   # read or modify PIPELINE_ENTRIES in place
    pipeline_entries_store.store()                            # store_pipeline_entries() of the changed subdirs

Concurrent writers
------------------
store() merges with the file on disk: it reloads it, applies only the fields
changed since loading, and appends new entries. Entries another process
appended in the meantime (e.g. submitslurm while lautools-browser refreshes)
are therefore kept. The same field changed by two writers: the last one wins.
"""
import copy
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from laupy.flow import pipeline_entries_to_refresh, load_pipeline_entries, refresh_pipeline_entries, store_pipeline_entries, sort_pipeline_entries_topologically
from laupy.slurm import ACTIVE_STATES, FAILED_STATES, scancel

log = logging.getLogger(__name__)

RETIRABLE_STATES = FAILED_STATES
CANCELLABLE_STATES = ACTIVE_STATES

PathLike = Union[str, Path]


# ----------------------------------------------------------------------
# Operations on a loaded PIPELINE_ENTRIES list (in place; nothing is written)
# ----------------------------------------------------------------------

def entry_slurm_id(entry: dict):
    """SLURM ID of an entry, falling back to the ID reported by SLURM."""
    return entry.get("slurm_id") or entry.get("slurm_info", {}).get("JobID")


def entry_matches_id(entry: dict, ids) -> bool:
    """True if the entry's job_id (laupy UUID) or its SLURM ID is in ids (compared as strings)."""
    slurm_id = entry_slurm_id(entry)
    return str(entry.get("job_id", "")) in ids or (slurm_id is not None and str(slurm_id) in ids)


def select_pipeline_entries(PIPELINE_ENTRIES: list, states, job_ids: list = None, steps: list = None) -> list:
    """Non-retired entries whose slurm_info State is in `states`, optionally limited to IDs or steps.

    job_ids may mix entry job_ids (laupy UUIDs) and SLURM job IDs.
    """
    states = set(states)
    selected = [entry for entry in PIPELINE_ENTRIES if not entry.get("retired", False) and entry.get("slurm_info", {}).get("State") in states]
    if job_ids:
        job_ids = {str(jid) for jid in job_ids}
        selected = [entry for entry in selected if entry_matches_id(entry, job_ids)]
    elif steps:
        steps = {str(step) for step in steps}
        selected = [entry for entry in selected if str(entry.get("step", "")) in steps]
    return selected


def retire_pipeline_entries(PIPELINE_ENTRIES: list, job_ids: list, reason: str = None, retired_by: str = None) -> list:
    """Retire the entries of PIPELINE_ENTRIES with the given job IDs. Returns the entries retired now.

    Retired entries are kept for history but ignored by scheduling and status.
    retire_timestamp, and optionally retired_reason / retired_by (job_id of the
    entry that replaced them), are recorded.
    """
    job_ids = {str(jid) for jid in job_ids}
    retired = []
    for entry in PIPELINE_ENTRIES:
        if str(entry.get("job_id")) in job_ids and not entry.get("retired", False):
            entry["retired"] = True
            entry["retire_timestamp"] = time.time()
            if reason:
                entry["retired_reason"] = reason
            if retired_by:
                entry["retired_by"] = retired_by
            retired.append(entry)
    return retired


def cancel_pipeline_entries(PIPELINE_ENTRIES: list, job_ids: list, refresh: bool = True) -> list:
    """scancel the entries of PIPELINE_ENTRIES with the given job IDs and set their slurm_info State to CANCELLED.

    Entries are not retired. With refresh=True their SLURM info is queried
    first (one batched query); jobs no longer in an active state are
    reported, not cancelled. Each job is cancelled separately so the outcome
    is known per job.
    Returns a list of {"entry": entry, "slurm_id": id, "state": state before
    cancelling, "error": message or None}.
    """
    job_ids = {str(jid) for jid in job_ids}
    selected = [entry for entry in PIPELINE_ENTRIES if str(entry.get("job_id")) in job_ids]
    if refresh:
        refresh_pipeline_entries(selected)
    results = []
    for entry in selected:
        slurm_id = entry_slurm_id(entry)
        state = entry.get("slurm_info", {}).get("State", "UNKNOWN")
        if slurm_id is None:
            error = "No SLURM ID found, cannot cancel"
        elif state not in CANCELLABLE_STATES:
            error = f"Job is no longer active ({state})"
        else:
            error = scancel(slurm_id)
            if error is None:
                entry.setdefault("slurm_info", {})["State"] = "CANCELLED"
        results.append({"entry": entry, "slurm_id": slurm_id, "state": state, "error": error})
    return results


# ----------------------------------------------------------------------
# PIPELINE_ENTRIES of many processing unit subdirs
# ----------------------------------------------------------------------

def _snapshot(entry: dict) -> str:
    return json.dumps(entry, sort_keys=True, default=str)


class PipelineEntriesStore:
    """PIPELINE_ENTRIES of several processing unit subdirs with batched SLURM refresh and merging store()."""

    def __init__(self, subdirs: Iterable[PathLike] = (), topological_sort: bool = False):
        self.topological_sort = topological_sort
        self._entries: Dict[str, list] = {}
        self._snapshots: Dict[str, Dict[str, str]] = {}  # subdir -> {job_id: entry as loaded}
        for subdir in subdirs:
            self.load(subdir)

    @staticmethod
    def _key(subdir: PathLike) -> str:
        return str(Path(subdir).resolve())

    # -- access ------------------------------------------------------------

    def load(self, subdir: PathLike) -> list:
        """(Re)load PIPELINE_ENTRIES of a processing unit subdir from disk, discarding unsaved changes."""
        subdir = self._key(subdir)
        PIPELINE_ENTRIES = load_pipeline_entries(subdir, topological_sort=self.topological_sort)
        name = os.path.basename(subdir)
        for entry in PIPELINE_ENTRIES:
            # Derived location; not a reason to rewrite the file on its own.
            entry.setdefault("processing_unit_subdir", subdir)
            entry.setdefault("processing_unit_name", name)
        self._entries[subdir] = PIPELINE_ENTRIES
        self._snapshots[subdir] = {str(e["job_id"]): _snapshot(e) for e in PIPELINE_ENTRIES if "job_id" in e}
        return PIPELINE_ENTRIES

    def subdirs(self) -> List[str]:
        return list(self._entries)

    def entries(self, subdir: PathLike) -> list:
        """PIPELINE_ENTRIES of a subdir (the loaded list: modify it in place, then store())."""
        return self._entries[self._key(subdir)]

    __getitem__ = entries

    def items(self) -> Iterator[Tuple[str, list]]:
        return iter(self._entries.items())

    def all_entries(self) -> List[dict]:
        return [entry for PIPELINE_ENTRIES in self._entries.values() for entry in PIPELINE_ENTRIES]

    def find(self, job_id) -> Optional[dict]:
        job_id = str(job_id)
        return next((e for e in self.all_entries() if str(e.get("job_id")) == job_id), None)

    # -- SLURM refresh -----------------------------------------------------

    def refresh(self, subdirs: Iterable[PathLike] = None, select: Callable[[str, list], list] = None, **selection) -> int:
        """Refresh slurm_info of entries from all (or the given) subdirs with one batched query.

        Which entries are queried: select(subdir, PIPELINE_ENTRIES) if given, otherwise
        laupy.flow.entries_to_refresh(PIPELINE_ENTRIES, **selection), e.g.
        update_retired=True, filter_terminal_states=False,
        limit_to_ancestors_including_node="3". Returns the number of entries queried.
        """
        subdir_keys = [self._key(u) for u in subdirs] if subdirs is not None else self.subdirs()
        to_query = []
        for subdir in subdir_keys:
            PIPELINE_ENTRIES = self._entries[subdir]
            to_query.extend(select(subdir, PIPELINE_ENTRIES) if select is not None else pipeline_entries_to_refresh(PIPELINE_ENTRIES, **selection))
        refresh_pipeline_entries(to_query)
        return len(to_query)

    # -- saving ------------------------------------------------------------

    def modified_subdirs(self) -> List[str]:
        result = []
        for subdir, PIPELINE_ENTRIES in self._entries.items():
            snapshots = self._snapshots[subdir]
            if any("job_id" in e and snapshots.get(str(e["job_id"])) != _snapshot(e) for e in PIPELINE_ENTRIES):
                result.append(subdir)
        return result

    def store(self, subdirs: Iterable[PathLike] = None) -> List[str]:
        """Write changed subdirs (all or the given ones), merged with the current file. Returns saved subdirs.

        Entries removed from a list are not removed from the file; use
        laupy.flow.clean_entries() to forget entries.
        """
        wanted = {self._key(u) for u in subdirs} if subdirs is not None else None
        saved = []
        for subdir in self.modified_subdirs():
            if wanted is not None and subdir not in wanted:
                continue
            self._store_subdir(subdir)
            saved.append(subdir)
        return saved

    def _store_subdir(self, subdir: str):
        PIPELINE_ENTRIES = self._entries[subdir]
        snapshots = self._snapshots[subdir]
        on_disk = load_pipeline_entries(subdir)
        index = {str(e["job_id"]): i for i, e in enumerate(on_disk) if "job_id" in e}
        for entry in PIPELINE_ENTRIES:
            if "job_id" not in entry:
                continue
            key = str(entry["job_id"])
            loaded = snapshots.get(key)
            if loaded == _snapshot(entry):
                continue  # unchanged here; keep whatever is on disk
            if key not in index:
                on_disk.append(copy.deepcopy(entry))  # new entry
                continue
            target = on_disk[index[key]]
            if loaded is None:
                target.clear()
                target.update(copy.deepcopy(entry))
                continue
            original = json.loads(loaded)
            for field in set(original) | set(entry):
                if field not in entry:
                    target.pop(field, None)
                elif original.get(field) != entry[field]:
                    target[field] = copy.deepcopy(entry[field])
        store_pipeline_entries(subdir, on_disk)
        # Continue from the merged state, keeping the identity of the list.
        name = os.path.basename(subdir)
        for entry in on_disk:
            entry.setdefault("processing_unit_subdir", subdir)
            entry.setdefault("processing_unit_name", name)
        PIPELINE_ENTRIES[:] = sort_pipeline_entries_topologically(on_disk) if self.topological_sort else on_disk
        self._snapshots[subdir] = {str(e["job_id"]): _snapshot(e) for e in PIPELINE_ENTRIES if "job_id" in e}


# ----------------------------------------------------------------------
# Convenience: operate on entries chosen elsewhere (e.g. in a dialog)
# ----------------------------------------------------------------------

def _group_by_subdir(selection) -> Dict[str, list]:
    """Accept {subdir: [job_id, ...]} or a list of entries carrying processing_unit_subdir."""
    if isinstance(selection, dict):
        return {str(subdir): list(job_ids) for subdir, job_ids in selection.items()}
    grouped: Dict[str, list] = {}
    for entry in selection:
        grouped.setdefault(entry["processing_unit_subdir"], []).append(entry["job_id"])
    return grouped


def retire_selected(selection, reason: str = None) -> Dict[str, list]:
    """Retire chosen entries: PIPELINE_ENTRIES are loaded fresh, changed and saved. Returns {subdir: retired entries}."""
    grouped = _group_by_subdir(selection)
    pipeline_entries_store = PipelineEntriesStore(grouped)
    result = {pipeline_entries_store._key(u): retire_pipeline_entries(pipeline_entries_store[u], job_ids, reason=reason) for u, job_ids in grouped.items()}
    pipeline_entries_store.store()
    return result


def cancel_selected(selection) -> Dict[str, list]:
    """Cancel chosen entries of any number of subdirs with one SLURM query. Returns {subdir: cancel results}."""
    grouped = _group_by_subdir(selection)
    pipeline_entries_store = PipelineEntriesStore(grouped)
    wanted = {pipeline_entries_store._key(u): {str(j) for j in job_ids} for u, job_ids in grouped.items()}
    # One batched query for all chosen entries, then cancel without querying again.
    pipeline_entries_store.refresh(select=lambda subdir, PIPELINE_ENTRIES: [e for e in PIPELINE_ENTRIES if str(e.get("job_id")) in wanted[subdir]])
    result = {subdir: cancel_pipeline_entries(pipeline_entries_store[subdir], job_ids, refresh=False) for subdir, job_ids in wanted.items()}
    pipeline_entries_store.store()  # also persists the refreshed slurm_info
    return result
