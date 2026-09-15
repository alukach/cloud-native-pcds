import json

import pyarrow as pa
import pyarrow.fs as pafs

from pcds import metadata, paths
from pcds.schema import HISTORIES, NETWORKS, STATIONS, VARIABLES
from pcds.storage import Store

SCHEMAS = {
    "networks": NETWORKS,
    "variables": VARIABLES,
    "stations": STATIONS,
    "histories": HISTORIES,
}


def test_write_drops_the_unknown_frequency(tmp_path):
    """Upstream's frequencies list carries a null, which sorted() cannot order."""
    store = Store(pafs.LocalFileSystem(), str(tmp_path), tmp_path.as_uri())
    tables = {n: pa.Table.from_pylist([], schema=s) for n, s in SCHEMAS.items()}

    metadata.write(store, tables, ["daily", None, "1-hourly"])

    written = tmp_path / paths.VARIABLES / "frequencies.json"
    assert json.loads(written.read_text()) == ["1-hourly", "daily"]
