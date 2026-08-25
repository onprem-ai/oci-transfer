import pytest

from opai_oci_transfer.errors import extract_http_error_detail, sanitize_error_detail


def test_sanitize_retains_actionable_detail_and_redacts_secrets() -> None:
    value = sanitize_error_detail(
        "DNS lookup failed for registry.internal at https://registry/x?token=value "
        "Bearer abc password=hunter2 ONPRM-ABCDE-ABCDE-ABCDE-ABCDE-ABCDE"
    )
    assert "DNS lookup failed for registry.internal" in value
    assert "https://" not in value
    assert "Bearer abc" not in value
    assert "hunter2" not in value
    assert "ONPRM" not in value


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b'{"detail":"repository does not exist"}', "repository does not exist"),
        (b'{"message":"access denied"}', "access denied"),
        (b'{"error":"manifest unknown"}', "manifest unknown"),
        (b"plain diagnostic", "plain diagnostic"),
        (b"", None),
        (b"   ", None),
    ],
)
def test_extract_http_error_detail(body: bytes, expected: str | None) -> None:
    assert extract_http_error_detail(body) == expected


def test_error_detail_is_bounded_and_invalid_utf8_is_safe() -> None:
    assert len(sanitize_error_detail("x" * 100, 10)) == 10
    assert extract_http_error_detail(b"problem \xff") == "problem �"
