from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetRequest(_message.Message):
    __slots__ = ("id", "key")
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    id: int
    key: str
    def __init__(self, id: _Optional[int] = ..., key: _Optional[str] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("id", "key", "value")
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    id: int
    key: str
    value: int
    def __init__(self, id: _Optional[int] = ..., key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("empty",)
    EMPTY_FIELD_NUMBER: _ClassVar[int]
    empty: str
    def __init__(self, empty: _Optional[str] = ...) -> None: ...

class CommitQueryMessage(_message.Message):
    __slots__ = ("id", "commit_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    COMMIT_ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    commit_id: int
    def __init__(self, id: _Optional[int] = ..., commit_id: _Optional[int] = ...) -> None: ...

class CommitQueryResponse(_message.Message):
    __slots__ = ("ack",)
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: str
    def __init__(self, ack: _Optional[str] = ...) -> None: ...

class CommitMessage(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class CommitResponse(_message.Message):
    __slots__ = ("ack",)
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: str
    def __init__(self, ack: _Optional[str] = ...) -> None: ...
