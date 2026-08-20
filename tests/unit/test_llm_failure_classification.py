"""Free-pool failure classification (plata.core.llm._classify_free_failure).

Regression anchor: 2026-08-20 strategist crash — AtlasCloud returned
`400 {'message': 'Provider returned error', ...}` on a json_schema call,
which matched no classification, so `complete()` burned its 3 same-model
retries and the BadRequestError escaped to the agent's consume loop.
"""
from plata.core.llm import _classify_free_failure


class _FakeStatusError(Exception):
    def __init__(self, msg: str, status_code: int | None = None):
        super().__init__(msg)
        self.status_code = status_code


ATLASCLOUD_400 = (
    "Error code: 400 - {'error': {'message': 'Provider returned error', "
    "'code': 400, 'metadata': {'raw': '{\"code\":400,\"msg\":\"bad request\","
    "\"request_id\":\"669133ae\"}', 'provider_name': 'AtlasCloud', "
    "'is_byok': False}}}"
)


def _classify(msg: str, status_code: int | None = None) -> str | None:
    return _classify_free_failure(_FakeStatusError(msg, status_code), msg)


def test_provider_returned_error_400_is_transient():
    assert _classify(ATLASCLOUD_400, status_code=400) == "transient"


def test_provider_returned_error_without_status_attr_is_transient():
    # Message-only match: exception types that don't carry .status_code.
    exc = RuntimeError(ATLASCLOUD_400)
    assert _classify_free_failure(exc, str(exc)) == "transient"


def test_bare_400_status_is_transient():
    assert _classify("Error code: 400 - bad request", status_code=400) == "transient"


def test_unavailable_for_free_is_permanent():
    assert _classify("model is unavailable for free") == "permanent"


def test_response_format_unsupported_is_permanent():
    assert _classify("response_format is not supported by this model") == "permanent"


def test_no_endpoints_found_is_transient():
    assert _classify("404 No endpoints found for x/y:free") == "transient"


def test_rate_limit_is_transient():
    assert _classify("Error code: 429 - rate limit exceeded") == "transient"


def test_provider_unreachable_is_transient():
    assert _classify("provider_unreachable: google_ai_studio — no key") == "transient"


def test_unrelated_error_is_unclassified():
    # e.g. a 500/timeout: keep the plain same-model retry path.
    assert _classify("Error code: 500 - internal server error", status_code=500) is None
