import httpx
import pytest

from pcds.config import Settings
from pcds.ingest import RateLimiter, _get_with_retry


def _client(status: int, calls: list[int]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(status)
        return httpx.Response(status, content=b"nope")

    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)
    return Settings(max_retries=5, max_rps=0)


def test_a_500_gets_one_retry_not_five(settings):
    """Lister 500s are reproducible, so retrying them just hammers PCIC."""
    calls: list[int] = []
    with pytest.raises(RuntimeError, match="exhausted retries"):
        _get_with_retry(_client(500, calls), "http://x/s.rsql.csv", settings, RateLimiter(0))
    assert len(calls) == 2


def test_a_503_still_gets_the_full_budget(settings):
    calls: list[int] = []
    with pytest.raises(RuntimeError, match="exhausted retries"):
        _get_with_retry(_client(503, calls), "http://x/s.rsql.csv", settings, RateLimiter(0))
    assert len(calls) == settings.max_retries


def test_a_404_is_not_retried(settings):
    calls: list[int] = []
    with pytest.raises(httpx.HTTPStatusError):
        _get_with_retry(_client(404, calls), "http://x/s.rsql.csv", settings, RateLimiter(0))
    assert len(calls) == 1
