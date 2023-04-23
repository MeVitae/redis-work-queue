class KeyPrefix:
    """A string which should be prefixed to an identifier to generate a database key.

    ### Example

    ```python
    cv_key = KeyPrefix("cv:")
    # ...
    cv_id = "abcdef-123456"
    assert cv_key.of(cv_id) == "cv:abcdef-123456"
    # You could use this to fetch something from a database, for example:
    cv_info = db.get(cv_key.of(cv_id))
    ```
    """

    def __init__(self, prefix: str):
        self.prefix = prefix

    def of(self, name: str) -> str:
        """Returns the result of prefixing `self` onto `name`."""
        return self.prefix + name

    @classmethod
    def concat(cls, prefix, name: str):
        """Returns the result of prefixing `self` onto `name` as a new `KeyPrefix."""
        return cls(prefix.of(name))
