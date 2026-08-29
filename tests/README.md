# Tests

Two suites, 17 tests.

| File | Tests | What it covers |
|---|---|---|
| `test_data_quality.py` | 9 | DQ expressions written inline — a copy of the logic, not the shipped code |
| `test_pipeline_utilities.py` | 8 | imports `databricks/utilities` directly, so a regression in the real code fails the suite |

The second suite is the one that protects you. Each case corresponds to a defect
that reached a running load, and its docstring names the symptom so a
reintroduced bug fails with an explanation rather than a bare assertion error.

## Running them on a Databricks cluster

This is the reference environment: Spark and Delta are already configured, so
nothing needs installing beyond pytest itself.

```python
%pip install pytest
```

```python
import pytest, os, sys

repo = "/Workspace/Users/<your-upn>/azure-forecast-project"

os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
os.chdir("/tmp")            # Workspace paths are not writable
sys.path.insert(0, repo)

pytest.main([f"{repo}/tests/", "-v", "-rs",
             "-p", "no:cacheprovider", "--assert=plain"])
```

Three details matter, each of which fails the run if omitted:

- **`pytest.main()`, not a subprocess.** Databricks owns the SparkContext in the
  driver and rejects any attempt to construct a second one, so a subprocess
  cannot get a session. Running in-process lets the fixture reuse the driver's.
- **`-p no:cacheprovider` and `--assert=plain`.** Both pytest caching and
  assertion rewriting try to create directories under the Workspace mount, which
  returns `OSError: [Errno 95] Operation not supported`.
- **`os.chdir("/tmp")`.** Same reason: pytest needs a writable working directory.

Expect `17 passed`.

## Running them locally

```powershell
pip install pyspark pytest delta-spark
pytest tests/ -v -rs
```

The Spark-backed tests **skip rather than fail** when no local JVM starts, and
`-rs` prints why. On Windows the launcher scripts under `SPARK_HOME/bin` do not
quote paths, so an install path containing a space dies with "The system cannot
find the file C:\Users\First" — an environment fault that says nothing
about the pipeline. A local checkout on a path without spaces runs normally, as
does CI on Linux.
