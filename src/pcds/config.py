"""Runtime configuration.

Everything is env-overridable so the same code runs locally and in GitHub Actions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

MIB = 1024 * 1024

# PCIC Meteorological Data Portal (PCDS instance).
DEFAULT_MDP_BASE = "https://services.pacificclimate.org/met-data-portal-pcds"


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    # ---- upstream -------------------------------------------------------
    mdp_base: str = os.environ.get("PCDS_MDP_BASE", DEFAULT_MDP_BASE)
    provinces: str = os.environ.get("PCDS_PROVINCES", "BC")

    # Politeness. PCIC runs this on modest academic infrastructure; the whole
    # point of publishing a Parquet mirror is that nobody has to hammer it.
    concurrency: int = _env_int("PCDS_CONCURRENCY", 4)
    max_rps: float = _env_float("PCDS_MAX_RPS", 2.0)
    request_timeout_s: float = _env_float("PCDS_TIMEOUT", 300.0)
    max_retries: int = _env_int("PCDS_MAX_RETRIES", 5)

    # ---- publication ----------------------------------------------------
    # The https base the catalog is served from, and the bucket-native equivalent.
    # Both appear in the STAC: https for asset hrefs (browsers cannot fetch s3://),
    # s3 for the partition glob and alternate assets.
    base_url: str = os.environ.get("PCDS_BASE_URL", "")
    public_s3_uri: str = os.environ.get("PCDS_PUBLIC_S3_URI", "")
    host_name: str = os.environ.get("PCDS_HOST_NAME", "Development Seed")
    host_url: str = os.environ.get("PCDS_HOST_URL", "https://developmentseed.org")
    host_email: str = os.environ.get("PCDS_HOST_EMAIL", "")

    # ---- destination ----------------------------------------------------
    # Local path, or s3://<bucket>/<prefix>. For Source Cooperative this is the
    # repository prefix, with PCDS_S3_ENDPOINT pointed at the data proxy.
    root: str = os.environ.get("PCDS_ROOT", "./data")
    s3_endpoint: str | None = os.environ.get("PCDS_S3_ENDPOINT") or None
    s3_region: str = os.environ.get("PCDS_S3_REGION", "us-west-2")

    # ---- packing --------------------------------------------------------
    # Object-storage readers do best with files large enough to amortize the
    # footer fetch but small enough to parallelize: 128-512 MiB.
    target_file_bytes: int = _env_int("PCDS_TARGET_FILE_BYTES", 256 * MIB)
    min_file_bytes: int = _env_int("PCDS_MIN_FILE_BYTES", 64 * MIB)
    # Row group sizing drives how much a range request must pull for a predicate
    # hit. 150,000 rows is ~450 KiB compressed in this schema, and it matches the
    # cap Portolan puts on GeoParquet row groups (PORTO-FMT-009), so the
    # observation files follow the same rule even though they are not GeoParquet.
    row_group_rows: int = _env_int("PCDS_ROW_GROUP_ROWS", 150_000)
    data_page_bytes: int = _env_int("PCDS_DATA_PAGE_BYTES", 1 * MIB)
    compression: str = os.environ.get("PCDS_COMPRESSION", "zstd")
    compression_level: int = _env_int("PCDS_COMPRESSION_LEVEL", 9)

    # ---- ingest semantics ----------------------------------------------
    # PCDS is explicitly a preliminary dataset: observations are revised and
    # late data arrives for weeks. Every incremental run re-reads this trailing
    # window and the compactor resolves duplicates last-write-wins.
    revision_window_days: int = _env_int("PCDS_REVISION_WINDOW_DAYS", 30)
    # Backfill is chunked so a failure costs one chunk, not one station-century.
    backfill_chunk_years: int = _env_int("PCDS_BACKFILL_CHUNK_YEARS", 5)

    extra_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": os.environ.get(
                "PCDS_USER_AGENT",
                "pcds-parquet/0.1 (+https://github.com/developmentseed/pcds-parquet)",
            )
        }
    )

    # ---- derived --------------------------------------------------------
    @property
    def metadata_base(self) -> str:
        return f"{self.mdp_base}/api/metadata"

    @property
    def data_base(self) -> str:
        return f"{self.mdp_base}/api/data"

    @property
    def lister_base(self) -> str:
        return f"{self.data_base}/lister"

    @property
    def agg_base(self) -> str:
        return f"{self.data_base}/pcds/agg/"

    def path(self, *parts: str) -> str:
        base = self.root.rstrip("/")
        return "/".join([base, *[p.strip("/") for p in parts]])


SETTINGS = Settings()
