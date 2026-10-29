def present[T](value: T | None) -> T:
    """Проверка «найдено» с сужением типа для mypy: `present(await repo.get(id))`."""
    assert value is not None
    return value
