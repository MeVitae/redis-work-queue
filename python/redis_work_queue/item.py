import uuid
import json


class Item(dict):
    """An item for a work queue. Each item has an ID and associated data."""

    def __init__(self, data: bytes | str, id=None):
        """
        Args:
            data (bytes or str): Data to associate with this item, strings will be converted to
                                 bytes.
            id (str | None): ID of the Item, if None, a new (random UUID) ID is generated.
        """
        if isinstance(data, str):
            data = bytes(data, 'utf-8')
        elif not isinstance(data, bytes):
            data = bytes(data)

        if id is None:
            id = uuid.uuid4().hex
        elif type(id) != str:
            id = str(id)

        dict.__init__(self, data=data, id=id)

    @classmethod
    def from_dict(cls, loaded: dict):
        """Create an `Item` from a dictionary containing 'data' and, optionally, 'id'."""
        id = None
        if 'id' in loaded:
            id = loaded['id']
        return cls(loaded['data'], id=id)

    @classmethod
    def parse(cls, s: str):
        """Parse an `Item` from JSON. The JSON structure should be an object with a 'data' key and,
        optionally, an 'id' key."""
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_json_data(cls, data, id=None):
        """Generate an item where the associated data is the JSON string of `data`."""
        return cls(json.dumps(data, separators=(',', ':')), id=id)

    def data(self) -> bytes:
        """Get the data associated with this item."""
        return self['data']

    def data_json(self):
        """Get the data associated with this item, parsed as JSON."""
        return json.loads(self.data())

    def id(self) -> str:
        """Get the ID of the item."""
        return self['id']
