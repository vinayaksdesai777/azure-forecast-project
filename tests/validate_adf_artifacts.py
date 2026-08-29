"""
Static validation of the ADF artifacts under adf/.

Several defects this project hit were pure JSON-shape errors that no unit test
could catch and that only surfaced as a failed pipeline run minutes later:

  * a Copy activity typed SalesforceServiceCloudSource against a SalesforceV2
    linked service — mismatched connector families
  * batch_id read as @json(activity(...).output.runOutput).batch_id when
    runOutput is already an object
  * a Get Metadata dataset parameterised with fileName "*" so exists never matched
  * notebookPath pointing at a path that does not exist in the workspace
  * a trigger firing the medallion load instead of the extract that feeds it

Each check below corresponds to one of those. Run as a script (exit 1 on any
failure) or under pytest.
"""

import json
import sys
from pathlib import Path

ADF = Path(__file__).resolve().parents[1] / "adf"

# Connector families that must not be mixed. A linked service of one type needs
# a dataset and a copy source from the same row.
CONNECTOR_FAMILIES = {
    "SalesforceServiceCloudV2": {
        "dataset": "SalesforceServiceCloudV2Object",
        "source": "SalesforceServiceCloudV2Source",
    },
    "SalesforceV2": {
        "dataset": "SalesforceV2Object",
        "source": "SalesforceV2Source",
    },
}


def _load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _walk(node):
    """Yield every dict nested anywhere in the document."""
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _artifacts(kind):
    return sorted((ADF / kind).glob("*.json"))


# ── Checks ───────────────────────────────────────────────────────────────────

def check_json_parses():
    """Every artifact must be valid JSON. A malformed file fails deployment
    with a message that names the byte offset, not the mistake."""
    problems = []
    for path in sorted(ADF.rglob("*.json")):
        try:
            _load(path)
        except json.JSONDecodeError as exc:
            problems.append(f"{path.relative_to(ADF)}: {exc}")
    return problems


def check_no_json_wrapper_on_runoutput():
    """ADF parses a notebook's runOutput into an object before an expression
    sees it, so @json(...) on it raises "expects its parameter to be a string
    or an XML"."""
    problems = []
    for path in _artifacts("pipeline"):
        text = path.read_text(encoding="utf-8")
        if "json(activity(" in text and "runOutput" in text:
            problems.append(
                f"{path.name}: wraps runOutput in json(); read the property directly"
            )
    return problems


def check_connector_families_match():
    """A dataset and copy source must belong to the same connector family as
    their linked service."""
    problems = []
    ls_types = {}
    for path in _artifacts("linkedService"):
        doc = _load(path)
        ls_types[doc["name"]] = doc["properties"]["type"]

    ds_family = {}
    for path in _artifacts("dataset"):
        doc = _load(path)
        props = doc["properties"]
        ls_name = props.get("linkedServiceName", {}).get("referenceName")
        ls_type = ls_types.get(ls_name)
        ds_type = props.get("type")
        ds_family[doc["name"]] = (ls_name, ls_type, ds_type)

        expected = CONNECTOR_FAMILIES.get(ls_type)
        if expected and ds_type != expected["dataset"]:
            problems.append(
                f"{path.name}: dataset type {ds_type} does not match linked service "
                f"{ls_name} of type {ls_type} (expected {expected['dataset']})"
            )

    # copy activity sources
    for path in _artifacts("pipeline"):
        for node in _walk(_load(path)):
            if node.get("type") != "Copy":
                continue
            tp = node.get("typeProperties", {})
            src_type = tp.get("source", {}).get("type", "")
            for inp in node.get("inputs", []):
                ds_name = inp.get("referenceName")
                if ds_name not in ds_family:
                    continue
                _, ls_type, _ = ds_family[ds_name]
                expected = CONNECTOR_FAMILIES.get(ls_type)
                if expected and src_type != expected["source"]:
                    problems.append(
                        f"{path.name}/{node.get('name')}: source type {src_type} does not "
                        f"match linked service type {ls_type} (expected {expected['source']})"
                    )
    return problems


def check_references_resolve():
    """Every referenceName must point at an artifact that exists in the repo."""
    known = set()
    for kind in ("linkedService", "dataset", "pipeline", "integrationRuntime"):
        known.update(_load(p)["name"] for p in _artifacts(kind))

    problems = []
    for kind in ("pipeline", "dataset", "trigger"):
        for path in _artifacts(kind):
            for node in _walk(_load(path)):
                ref = node.get("referenceName")
                if isinstance(ref, str) and ref not in known:
                    problems.append(f"{path.name}: references unknown artifact {ref!r}")
    return problems


def check_triggers_fire_the_extract():
    """A trigger must start the extract, which chains into the medallion load
    itself. Firing the master pipeline directly reprocesses whatever Parquet is
    already in landing and never pulls new source rows."""
    problems = []
    for path in _artifacts("trigger"):
        doc = _load(path)
        for pl in doc["properties"].get("pipelines", []):
            name = pl.get("pipelineReference", {}).get("referenceName")
            if name != "pl_extract_to_landing":
                problems.append(
                    f"{path.name}: fires {name}; a schedule must start "
                    f"pl_extract_to_landing so the source is actually read"
                )
    return problems


def check_trigger_parameters_complete():
    """A trigger must supply every parameter its pipeline declares. A missing
    one silently falls back to the default — which is how a weekly run came to
    load from the daily landing folder."""
    problems = []
    declared = {}
    for path in _artifacts("pipeline"):
        doc = _load(path)
        declared[doc["name"]] = set(doc["properties"].get("parameters", {}))

    for path in _artifacts("trigger"):
        doc = _load(path)
        for pl in doc["properties"].get("pipelines", []):
            name = pl.get("pipelineReference", {}).get("referenceName")
            if name not in declared:
                continue
            passed = set(pl.get("parameters", {}))
            missing = declared[name] - passed
            if missing:
                problems.append(
                    f"{path.name}: does not pass {sorted(missing)} to {name}"
                )
    return problems


def check_notebook_paths_consistent():
    """Every notebookPath must share one root and name a notebook that exists
    under databricks/notebooks/."""
    repo_notebooks = {
        p.stem for p in (ADF.parent / "databricks" / "notebooks").glob("*.py")
    }
    problems = []
    roots = set()
    for path in _artifacts("pipeline"):
        for node in _walk(_load(path)):
            nb = node.get("typeProperties", {}).get("notebookPath")
            if not isinstance(nb, str):
                continue
            roots.add(nb.rsplit("/", 1)[0])
            name = nb.rsplit("/", 1)[-1]
            if name not in repo_notebooks:
                problems.append(
                    f"{path.name}: notebookPath names {name!r}, which is not in "
                    f"databricks/notebooks/"
                )
    if len(roots) > 1:
        problems.append(f"notebookPath roots disagree: {sorted(roots)}")
    return problems


def check_get_metadata_uses_childitems():
    """Get Metadata's "exists" field resolves one concrete path, so it is always
    false for a wildcard filename. Folder emptiness must be tested with
    childItems."""
    problems = []
    for path in _artifacts("pipeline"):
        for node in _walk(_load(path)):
            if node.get("type") != "GetMetadata":
                continue
            tp = node.get("typeProperties", {})
            fields = tp.get("fieldList", [])
            params = tp.get("dataset", {}).get("parameters", {})
            if params.get("fileName") == "*" and "exists" in fields:
                problems.append(
                    f"{path.name}/{node.get('name')}: asks for 'exists' with a wildcard "
                    f"fileName; use childItems against a folder dataset"
                )
    return problems


CHECKS = [
    check_json_parses,
    check_no_json_wrapper_on_runoutput,
    check_connector_families_match,
    check_references_resolve,
    check_triggers_fire_the_extract,
    check_trigger_parameters_complete,
    check_notebook_paths_consistent,
    check_get_metadata_uses_childitems,
]


def main():
    failed = 0
    for check in CHECKS:
        problems = check()
        label = check.__name__.replace("check_", "").replace("_", " ")
        if problems:
            failed += 1
            print(f"FAIL  {label}")
            for p in problems:
                print(f"        {p}")
        else:
            print(f"ok    {label}")
    print()
    if failed:
        print(f"{failed} of {len(CHECKS)} checks failed")
        return 1
    print(f"all {len(CHECKS)} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
