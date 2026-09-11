# src/utils/sanitize.py


_INVALID: frozenset = frozenset(" ,;{}\r\n\t=.?`()")


def sanitize_col(name: str) -> str:
    """
    Produce the Delta-safe lowercase column name that Bronze stores.

    Preserves a single leading underscore if the input starts with one.
    Strips all other leading/trailing underscores from the sanitized result.

    Examples
    --------
    >>> sanitize_col("Study Site")
    'study_site'
    >>> sanitize_col("_study_id")
    '_study_id'
    >>> sanitize_col("Disc. Rollback Approved?")
    'disc_rollback_approved'
    >>> sanitize_col("col with\r\nnewline")
    'col_with_newline'
    """
    leading = name.startswith("_")
    safe = "".join("_" if c in _INVALID else c for c in name)
    while "__" in safe:
        safe = safe.replace("__", "_")
    safe = safe.strip("_").lower()
    if leading and safe:
        safe = "_" + safe
    return safe if safe else "col"
