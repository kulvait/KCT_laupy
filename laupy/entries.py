"""
Management of ENTRIES, the scheduled SLURM jobs of pipelines.

ENTRIES of an execution unit are stored in <execution_unit>/pipeline/dag.json
(see laupy.flow). Each entry is one SLURM job attached to a DAG node; this
module covers their lifecycle after submission:

    submitted -> (refresh from SLURM) -> cancelled / finished -> retired

Shared by the `pipeline` and `submitslurm` CLIs and by lautools-browser.

Batched SLURM queries
---------------------
EntriesStore holds the ENTRIES of many execution units. refresh() collects the
entries that need fresh SLURM info from all units and queries them together,
so the number of sacct/squeue calls does not grow with the number of
execution units (see laupy.slurm.QUERY_CHUNK_SIZE). Entries whose stored state
is final are not queried again, and save() writes only units that changed.

Typical use:

    store = EntriesStore(unit_dirs)   # ENTRIES = load_entries() of every unit
    store.refresh()                   # one batched SLURM query for all units
    for unit_dir, ENTRIES in store.items():
        ...                           # read or modify ENTRIES in place
    store.save()                      # save_entries() of the changed units

Concurrent writers
------------------
save() merges with the file on disk: it reloads it, applies only the fields
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

from laupy.flow import entries_to_refresh, load_entries, refresh_entries, save_entries, sort_entries_topologically
from laupy.slurm import ACTIVE_STATES, FAILED_STATES, scancel

log = logging.getLogger(__name__)

RETIRABLE_STATES = FAILED_STATES
CANCELLABLE_STATES = ACTIVE_STATES

PathLike = Union[str, Path]


# ----------------------------------------------------------------------
# Operations on a loaded ENTRIES list (in place; nothing is written)
# ----------------------------------------------------------------------

def entry_slurm_id(entry: dict):
    """SLURM ID of an entry, falling back to the ID reported by SLURM."""
    return entry.get("slurm_id") or entry.get("slurm_info", {}).get("JobID")


def select_entries(ENTRIES: list, states, job_ids: list = None, steps: list = None) -> list:
    """Non-retired entries whose slurm_info State is in `states`, optionally limited to job IDs or steps."""
    states = set(states)
    selected = [entry for entry in ENTRIES if not entry.get("retired", False) and entry.get("slurm_info", {}).get("State") in states]
    if job_ids:
        job_ids = {str(jid) for jid in job_ids}
        selected = [entry for entry in selected if str(entry.get("job_id", "")) in job_ids]
    elif steps:
        steps = {str(step) for step in steps}
        selected = [entry for entry in selected if str(entry.get("step", "")) in steps]
    return selected


def retire_entries(ENTRIES: list, job_ids: list, reason: str = None, retired_by: str = None) -> list:
    """Retire the entries of ENTRIES with the given job IDs. Returns the entries retired now.

    Retired entries are kept for history but ignored by scheduling and status.
    retire_timestamp, and optionally retired_reason / retired_by (job_id of the
    entry that replaced them), are recorded.
    """
    job_ids = {str(jid) for jid in job_ids}
    retired = []
    for entry in ENTRIES:
        if str(entry.get("job_id")) in job_ids and not entry.get("retired", False):
            entry["retired"] = True
            entry["retire_timestamp"] = time.time()
            if reason:
                entry["retired_reason"] = reason
            if retired_by:
                entry["retired_by"] = retired_by
            retired.append(entry)
    return retired


def cancel_entries(ENTRIES: list, job_ids: list, refresh: bool = True) -> list:
    """scancel the entries of ENTRIES with the given job IDs and set their slurm_info State to CANCELLED.

    Entries are not retired. With refresh=True their SLURM info is queried
    first (one batched query); jobs no longer in an active state are
    reported, not cancelled. Each job is cancelled separately so the outcome
    is known per job.
    Returns a list of {"entry": entry, "slurm_id": id, "state": state before
    cancelling, "error": message or None}.
    """
    job_ids = {str(jid) for jid in job_ids}
    selected = [entry for entry in ENTRIES if str(entry.get("job_id")) in job_ids]
    if refresh:
        refresh_entries(selected)
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
# ENTRIES of many execution units
# ----------------------------------------------------------------------

def _snapshot(entry: dict) -> str:
    return json.dumps(entry, sort_keys=True, default=str)


class EntriesStore:
    """ENTRIES of several execution units with batched SLURM refresh and merging save."""

    def __init__(self, unit_dirs: Iterable[PathLike] = (), topological_sort: bool = False):
        self.topological_sort = topological_sort
        self._entries: Dict[str, list] = {}
        self._snapshots: Dict[str, Dict[str, str]] = {}  # unit -> {job_id: entry as loaded}
        for unit_dir in unit_dirs:
            self.load(unit_dir)

    @staticmethod
    def _key(unit_dir: PathLike) -> str:
        return str(Path(unit_dir).resolve())

    # -- access ------------------------------------------------------------

    def load(self, unit_dir: PathLike) -> list:
        """(Re)load ENTRIES of an execution unit from disk, discarding unsaved changes."""
        unit = self._key(unit_dir)
        ENTRIES = load_entries(unit, topological_sort=self.topological_sort)
        name = os.path.basename(unit)
        for entry in ENTRIES:
            # Derived location; not a reason to rewrite the file on its own.
            entry.setdefault("execution_unit_dir", unit)
            entry.setdefault("execution_unit_name", name)
        self._entries[unit] = ENTRIES
        self._snapshots[unit] = {str(e["job_id"]): _snapshot(e) for e in ENTRIES if "job_id" in e}
        return ENTRIES

    def units(self) -> List[str]:
        return list(self._entries)

    def entries(self, unit_dir: PathLike) -> list:
        """ENTRIES of a unit (the stored list: modify it in place, then save())."""
        return self._entries[self._key(unit_dir)]

    __getitem__ = entries

    def items(self) -> Iterator[Tuple[str, list]]:
        return iter(self._entries.items())

    def all_entries(self) -> List[dict]:
        return [entry for ENTRIES in self._entries.values() for entry in ENTRIES]

    def find(self, job_id) -> Optional[dict]:
        job_id = str(job_id)
        return next((e for e in self.all_entries() if str(e.get("job_id")) == job_id), None)

    # -- SLURM refresh -----------------------------------------------------

    def refresh(self, units: Iterable[PathLike] = None, select: Callable[[str, list], list] = None, **selection) -> int:
        """Refresh slurm_info of entries from all (or the given) units with one batched query.

        Which entries are queried: select(unit, ENTRIES) if given, otherwise
        laupy.flow.entries_to_refresh(ENTRIES, **selection), e.g.
        update_retired=True, filter_terminal_states=False,
        limit_to_ancestors_including_node="3". Returns the number of entries queried.
        """
        unit_keys = [self._key(u) for u in units] if units is not None else self.units()
        to_query = []
        for unit in unit_keys:
            ENTRIES = self._entries[unit]
            to_query.extend(select(unit, ENTRIES) if select is not None else entries_to_refresh(ENTRIES, **selection))
        refresh_entries(to_query)
        return len(to_query)

    # -- saving ------------------------------------------------------------

    def modified_units(self) -> List[str]:
        result = []
        for unit, ENTRIES in self._entries.items():
            snapshots = self._snapshots[unit]
            if any("job_id" in e and snapshots.get(str(e["job_id"])) != _snapshot(e) for e in ENTRIES):
                result.append(unit)
        return result

    def save(self, units: Iterable[PathLike] = None) -> List[str]:
        """Write changed units (all or the given ones), merged with the current file. Returns saved units.

        Entries removed from a list are not removed from the file; use
        laupy.flow.clean_entries() to forget entries.
        """
        wanted = {self._key(u) for u in units} if units is not None else None
        saved = []
        for unit in self.modified_units():
            if wanted is not None and unit not in wanted:
                continue
            self._save_unit(unit)
            saved.append(unit)
        return saved

    def _save_unit(self, unit: str):
        ENTRIES = self._entries[unit]
        snapshots = self._snapshots[unit]
        on_disk = load_entries(unit)
        index = {str(e["job_id"]): i for i, e in enumerate(on_disk) if "job_id" in e}
        for entry in ENTRIES:
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
        save_entries(unit, on_disk)
        # Continue from the merged state, keeping the identity of the list.
        name = os.path.basename(unit)
        for entry in on_disk:
            entry.setdefault("execution_unit_dir", unit)
            entry.setdefault("execution_unit_name", name)
        ENTRIES[:] = sort_entries_topologically(on_disk) if self.topological_sort else on_disk
        self._snapshots[unit] = {str(e["job_id"]): _snapshot(e) for e in ENTRIES if "job_id" in e}


# ----------------------------------------------------------------------
# Convenience: operate on entries chosen elsewhere (e.g. in a dialog)
# ----------------------------------------------------------------------

def _group_by_unit(selection) -> Dict[str, list]:
    """Accept {unit_dir: [job_id, ...]} or a list of entries carrying execution_unit_dir."""
    if isinstance(selection, dict):
        return {str(unit): list(job_ids) for unit, job_ids in selection.items()}
    grouped: Dict[str, list] = {}
    for entry in selection:
        grouped.setdefault(entry["execution_unit_dir"], []).append(entry["job_id"])
    return grouped


def retire_selected(selection, reason: str = None) -> Dict[str, list]:
    """Retire chosen entries: ENTRIES are loaded fresh, changed and saved. Returns {unit: retired entries}."""
    grouped = _group_by_unit(selection)
    store = EntriesStore(grouped)
    result = {store._key(u): retire_entries(store[u], job_ids, reason=reason) for u, job_ids in grouped.items()}
    store.save()
    return result


def cancel_selected(selection) -> Dict[str, list]:
    """Cancel chosen entries of any number of units with one SLURM query. Returns {unit: cancel results}."""
    grouped = _group_by_unit(selection)
    store = EntriesStore(grouped)
    wanted = {store._key(u): {str(j) for j in job_ids} for u, job_ids in grouped.items()}
    # One batched query for all chosen entries, then cancel without querying again.
    store.refresh(select=lambda unit, ENTRIES: [e for e in ENTRIES if str(e.get("job_id")) in wanted[unit]])
    result = {unit: cancel_entries(store[unit], job_ids, refresh=False) for unit, job_ids in wanted.items()}
    store.save()  # also persists the refreshed slurm_info
    return result
