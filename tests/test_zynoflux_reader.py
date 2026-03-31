"""Tests for the ZynofluxReader ingestion component.

Verifies connection management, batch reading, and streaming interfaces
of the Zynoflux protocol reader.
"""

from __future__ import annotations

import pytest

from src.ingestion.zynoflux_reader import (
    ZynofluxConnectionError,
    ZynofluxReadError,
    ZynofluxReader,
)


@pytest.fixture
def reader() -> ZynofluxReader:
    """Create a fresh ZynofluxReader instance for each test."""
    return ZynofluxReader(max_connections=4, buffer_size=500)


class TestConnect:
    """Tests for the connect() method."""

    def test_connect_valid_uri(self, reader: ZynofluxReader) -> None:
        """Connecting to a well-formed URI should return ConnectionInfo."""
        info = reader.connect("zynoflux://datahost:9091/events")
        assert info.host == "datahost"
        assert info.port == 9091
        assert info.stream_name == "events"

    def test_connect_default_port(self, reader: ZynofluxReader) -> None:
        """A URI without an explicit port should fall back to the default."""
        info = reader.connect("zynoflux://datahost/events")
        assert info.port == ZynofluxReader.DEFAULT_PORT

    def test_connect_invalid_scheme(self, reader: ZynofluxReader) -> None:
        """A non-zynoflux scheme should raise ZynofluxConnectionError."""
        with pytest.raises(ZynofluxConnectionError, match="Invalid scheme"):
            reader.connect("http://datahost:9091/events")

    def test_connect_duplicate_uri(self, reader: ZynofluxReader) -> None:
        """Connecting to the same URI twice should raise an error."""
        reader.connect("zynoflux://datahost:9091/events")
        with pytest.raises(ZynofluxConnectionError, match="Already connected"):
            reader.connect("zynoflux://datahost:9091/events")

    def test_connect_pool_exhaustion(self) -> None:
        """Exceeding max_connections should raise an error."""
        small_reader = ZynofluxReader(max_connections=2)
        small_reader.connect("zynoflux://host1:9091/s1")
        small_reader.connect("zynoflux://host2:9091/s2")
        with pytest.raises(ZynofluxConnectionError, match="pool exhausted"):
            small_reader.connect("zynoflux://host3:9091/s3")


class TestReadBatch:
    """Tests for the read_batch() method."""

    def test_read_batch_returns_records(self, reader: ZynofluxReader) -> None:
        """Reading a batch from a connected source should return records."""
        reader.connect("zynoflux://datahost:9091/events")
        batch = reader.read_batch(batch_size=10)
        assert isinstance(batch, list)
        assert len(batch) > 0

    def test_read_batch_no_connection(self, reader: ZynofluxReader) -> None:
        """Reading without any connections should raise an error."""
        with pytest.raises(ZynofluxReadError, match="No sources connected"):
            reader.read_batch()


class TestReadStream:
    """Tests for the read_stream() generator."""

    def test_stream_yields_records(self, reader: ZynofluxReader) -> None:
        """The stream generator should yield individual records."""
        reader.connect("zynoflux://datahost:9091/events")
        records = []
        for record in reader.read_stream():
            records.append(record)
            if len(records) >= 5:
                break
        assert len(records) == 5

    def test_stream_no_connection(self, reader: ZynofluxReader) -> None:
        """Streaming without connections should raise an error."""
        with pytest.raises(ZynofluxReadError):
            next(reader.read_stream())


class TestClose:
    """Tests for connection lifecycle management."""

    def test_close_releases_resources(self, reader: ZynofluxReader) -> None:
        """Closing should clear all connections and buffers."""
        reader.connect("zynoflux://datahost:9091/events")
        reader.close()
        with pytest.raises(ZynofluxReadError, match="closed"):
            reader.read_batch()

    def test_double_close_is_safe(self, reader: ZynofluxReader) -> None:
        """Calling close twice should not raise."""
        reader.close()
        reader.close()  # should be a no-op
