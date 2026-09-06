import sys
from pathlib import Path
import unittest

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.bencode import (
    encode,
    decode,
    BencodeEncoder,
    BencodeDecoder,
    BencodeDecodeError,
    BencodeEncodeError,
)


class TestBencode(unittest.TestCase):

    def test_encode_integers(self):
        self.assertEqual(encode(0), b"i0e")
        self.assertEqual(encode(42), b"i42e")
        self.assertEqual(encode(-42), b"i-42e")

    def test_decode_integers(self):
        self.assertEqual(decode(b"i0e"), 0)
        self.assertEqual(decode(b"i42e"), 42)
        self.assertEqual(decode(b"i-42e"), -42)

    def test_decode_invalid_integers(self):
        with self.assertRaises(BencodeDecodeError):
            decode(b"i-0e")
        with self.assertRaises(BencodeDecodeError):
            decode(b"i03e")
        with self.assertRaises(BencodeDecodeError):
            decode(b"ie")

    def test_encode_strings(self):
        self.assertEqual(encode(b"spam"), b"4:spam")
        self.assertEqual(encode("spam"), b"4:spam")
        self.assertEqual(encode(b""), b"0:")

    def test_decode_strings(self):
        self.assertEqual(decode(b"4:spam"), b"spam")
        self.assertEqual(decode(b"0:"), b"")

    def test_encode_lists(self):
        self.assertEqual(encode([b"spam", 42]), b"l4:spami42ee")
        self.assertEqual(encode([]), b"le")

    def test_decode_lists(self):
        self.assertEqual(decode(b"l4:spami42ee"), [b"spam", 42])
        self.assertEqual(decode(b"le"), [])

    def test_encode_dicts(self):
        data = {b"bar": b"spam", b"foo": 42}
        self.assertEqual(encode(data), b"d3:bar4:spam3:fooi42ee")
        # Test key sorting order when string/bytes mixed
        data_unordered = {"foo": 42, "bar": "spam"}
        self.assertEqual(encode(data_unordered), b"d3:bar4:spam3:fooi42ee")

    def test_decode_dicts(self):
        expected = {b"bar": b"spam", b"foo": 42}
        self.assertEqual(decode(b"d3:bar4:spam3:fooi42ee"), expected)
        self.assertEqual(decode(b"de"), {})

    def test_roundtrip(self):
        complex_structure = {
            b"announce": b"http://tracker.example.com/announce",
            b"info": {
                b"piece length": 262144,
                b"pieces": b"12345678901234567890",
                b"name": b"example.txt",
                b"length": 1000,
            },
            b"creation date": 1600000000,
        }
        encoded = encode(complex_structure)
        decoded = decode(encoded)
        self.assertEqual(decoded, complex_structure)

    def test_class_interface(self):
        decoder = BencodeDecoder(b"i123e")
        self.assertEqual(decoder.decode(), 123)
        encoder = BencodeEncoder()
        self.assertEqual(encoder.encode(123), b"i123e")


if __name__ == "__main__":
    unittest.main()
