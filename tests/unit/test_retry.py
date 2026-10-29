import pytest

from app.consumer.retry import decide


@pytest.mark.parametrize(
    ("attempt", "max_retries", "expected"),
    [(0, 3, 1), (1, 3, 2), (2, 3, 3), (3, 3, None), (5, 3, None), (0, 0, None)],
)
def test_decide(attempt: int, max_retries: int, expected: int | None):
    assert decide(attempt, max_retries) == expected
