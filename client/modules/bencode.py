from typing import Any, Union


class BencodeDecodeError(ValueError):
    """Exception raised when bencode data is malformed or invalid."""

    pass


class BencodeEncodeError(TypeError):
    """Exception raised when an object cannot be bencoded."""

    pass


class BencodeDecoder:

    def __init__(self, data: Union[bytes, bytearray] = b"") -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(f"Expected bytes or bytearray, got {type(data).__name__}")
        self._data = bytes(data)
        self._index = 0

    @classmethod
    def decode_data(cls, data: Union[bytes, bytearray]) -> Any:
        decoder = cls(data)
        result = decoder._parse_next()
        if decoder._index != len(decoder._data):
            raise BencodeDecodeError(
                f"Unconsumed data remaining at index {decoder._index} (total length {len(decoder._data)})"
            )
        return result

    def decode(self, data: Union[bytes, bytearray] = None) -> Any:
        if data is not None:
            return self.decode_data(data)
        result = self._parse_next()
        if self._index != len(self._data):
            raise BencodeDecodeError(
                f"Unconsumed data remaining at index {self._index}"
            )
        return result

    def _peek(self) -> int:
        if self._index >= len(self._data):
            raise BencodeDecodeError("Unexpected EOF while parsing bencode data")
        return self._data[self._index]

    def _read_until(self, delimiter: bytes) -> bytes:
        start = self._index
        pos = self._data.find(delimiter, start)
        if pos == -1:
            raise BencodeDecodeError(f"Delimiter {delimiter!r} not found after index {start}")
        self._index = pos + len(delimiter)
        return self._data[start:pos]

    def _parse_next(self) -> Any:
        byte = self._peek()

        if byte == ord(b"i"):
            return self._parse_int()
        elif byte == ord(b"l"):
            return self._parse_list()
        elif byte == ord(b"d"):
            return self._parse_dict()
        elif ord(b"0") <= byte <= ord(b"9"):
            return self._parse_string()
        else:
            raise BencodeDecodeError(
                f"Invalid bencode token {bytes([byte])!r} at index {self._index}"
            )

    def _parse_int(self) -> int:
        self._index += 1  # Skip 'i'
        raw = self._read_until(b"e")
        if not raw:
            raise BencodeDecodeError("Empty integer value in bencode data")
        if raw == b"-0":
            raise BencodeDecodeError("Negative zero '-0' is not allowed in bencode integer")
        if len(raw) > 1 and raw.startswith(b"0"):
            raise BencodeDecodeError("Leading zeros are not allowed in bencode integer")
        if len(raw) > 2 and raw.startswith(b"-0"):
            raise BencodeDecodeError("Leading zeros after sign are not allowed in bencode integer")
        try:
            return int(raw)
        except ValueError as e:
            raise BencodeDecodeError(f"Invalid integer format {raw!r}") from e

    def _parse_string(self) -> bytes:
        raw_len = self._read_until(b":")
        if len(raw_len) > 1 and raw_len.startswith(b"0"):
            raise BencodeDecodeError("Leading zeros in string length prefix are not allowed")
        try:
            length = int(raw_len)
        except ValueError as e:
            raise BencodeDecodeError(f"Invalid string length prefix {raw_len!r}") from e

        if length < 0:
            raise BencodeDecodeError(f"Negative string length {length}")

        if self._index + length > len(self._data):
            raise BencodeDecodeError(
                f"String of length {length} exceeds remaining data size ({len(self._data) - self._index} bytes)"
            )

        start = self._index
        self._index += length
        return self._data[start : self._index]

    def _parse_list(self) -> list:
        self._index += 1  # Skip 'l'
        items = []
        while self._peek() != ord(b"e"):
            items.append(self._parse_next())
        self._index += 1  # Skip 'e'
        return items

    def _parse_dict(self) -> dict:
        self._index += 1  # Skip 'd'
        result = {}
        last_key = None
        while self._peek() != ord(b"e"):
            byte = self._peek()
            if not (ord(b"0") <= byte <= ord(b"9")):
                raise BencodeDecodeError(
                    f"Dictionary keys must be byte strings, got token {bytes([byte])!r} at index {self._index}"
                )
            key = self._parse_string()
            if last_key is not None and key <= last_key:
                pass  # Lexicographical check can be enforced or permissive
            last_key = key
            val = self._parse_next()
            result[key] = val
        self._index += 1  # Skip 'e'
        return result


class BencodeEncoder:

    @classmethod
    def encode_data(cls, obj: Any) -> bytes:
        encoder = cls()
        return encoder.encode(obj)

    def encode(self, obj: Any) -> bytes:
        if isinstance(obj, int) and not isinstance(obj, bool):
            return f"i{obj}e".encode("ascii")

        elif isinstance(obj, (bytes, bytearray)):
            raw = bytes(obj)
            return f"{len(raw)}:".encode("ascii") + raw

        elif isinstance(obj, str):
            raw = obj.encode("utf-8")
            return f"{len(raw)}:".encode("ascii") + raw

        elif isinstance(obj, (list, tuple)):
            parts = [b"l"]
            for item in obj:
                parts.append(self.encode(item))
            parts.append(b"e")
            return b"".join(parts)

        elif isinstance(obj, dict):
            parts = [b"d"]
            encoded_keys = []
            for k in obj.keys():
                if isinstance(k, bytes):
                    k_bytes = k
                elif isinstance(k, str):
                    k_bytes = k.encode("utf-8")
                else:
                    raise BencodeEncodeError(
                        f"Dictionary keys must be bytes or str, got {type(k).__name__}"
                    )
                encoded_keys.append((k_bytes, k))

            encoded_keys.sort(key=lambda item: item[0])

            for k_bytes, k in encoded_keys:
                parts.append(f"{len(k_bytes)}:".encode("ascii") + k_bytes)
                parts.append(self.encode(obj[k]))

            parts.append(b"e")
            return b"".join(parts)

        else:
            raise BencodeEncodeError(
                f"Type {type(obj).__name__} is not supported for bencode encoding"
            )


def decode(data: Union[bytes, bytearray]) -> Any:
    return BencodeDecoder.decode_data(data)


def encode(obj: Any) -> bytes:
    return BencodeEncoder.encode_data(obj)
