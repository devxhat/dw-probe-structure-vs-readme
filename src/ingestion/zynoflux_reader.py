"""Zynoflux protocol reader for heterogeneous data source ingestion.

This module implements the core ingestion component of the ZynofluxProcessor
pipeline. It manages connections to external data sources, handles batched
and streaming reads, and integrates with the FormatDetector for automatic
input format resolution.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from .format_detector import FormatDetector, FormatMatch

logger = logging.getLogger(__name__)


class ZynofluxConnectionError(Exception):
    """Raised when a connection to a Zynoflux source cannot be established."""


class ZynofluxReadError(Exception):
    """Raised when a read operation fails on an active connection."""


@dataclass
class ConnectionInfo:
    """Metadata about an active source connection."""

    source_uri: str
    host: str
    port: int
    stream_name: str
    connected_at: float = field(default_factory=time.time)
    bytes_read: int = 0


class ZynofluxReader:
    """Reads data from multiple heterogeneous sources using the Zynoflux protocol.

    The reader maintains a pool of connections to data sources and provides
    both batch and streaming interfaces for record retrieval. Incoming data
    is automatically inspected by the FormatDetector to determine parse
    strategy before records are yielded to downstream pipeline stages.

    Args:
        max_connections: Maximum number of simultaneous source connections.
        buffer_size: Internal read buffer capacity in number of records.
        detection_sample_bytes: Number of leading bytes passed to FormatDetector.

    Example::

        reader = ZynofluxReader(max_connections=4)
        reader.connect("zynoflux://datahost:9091/events")
        batch = reader.read_batch(batch_size=500)
        reader.close()
    """

    DEFAULT_PORT: int = 9091
    PROTOCOL_SCHEME: str = "zynoflux"

    def __init__(
        self,
        max_connections: int = 8,
        buffer_size: int = 10_000,
        detection_sample_bytes: int = 512,
    ) -> None:
        self._max_connections = max_connections
        self._buffer_size = buffer_size
        self._detection_sample_bytes = detection_sample_bytes

        self._sources: dict[str, ConnectionInfo] = {}
        self._connection_pool: list[str] = []
        self._buffer: deque[dict[str, Any]] = deque(maxlen=buffer_size)
        self._format_detector = FormatDetector()
        self._is_closed = False

    def connect(self, source_uri: str) -> ConnectionInfo:
        """Establish a connection to a Zynoflux data source.

        Args:
            source_uri: URI in the form ``zynoflux://host:port/stream``.

        Returns:
            ConnectionInfo describing the established connection.

        Raises:
            ZynofluxConnectionError: If the URI is malformed, the connection
                pool is full, or the source is already connected.
        """
        self._ensure_open()

        parsed = urlparse(source_uri)
        if parsed.scheme != self.PROTOCOL_SCHEME:
            raise ZynofluxConnectionError(
                f"Invalid scheme '{parsed.scheme}'. "
                f"Expected '{self.PROTOCOL_SCHEME}'."
            )

        if len(self._connection_pool) >= self._max_connections:
            raise ZynofluxConnectionError(
                f"Connection pool exhausted (max={self._max_connections}). "
                "Close existing connections before adding new ones."
            )

        if source_uri in self._sources:
            raise ZynofluxConnectionError(
                f"Already connected to '{source_uri}'. "
                "Disconnect first to reconnect."
            )

        host = parsed.hostname or "localhost"
        port = parsed.port or self.DEFAULT_PORT
        stream_name = parsed.path.lstrip("/") or "default"

        connection = ConnectionInfo(
            source_uri=source_uri,
            host=host,
            port=port,
            stream_name=stream_name,
        )
        self._sources[source_uri] = connection
        self._connection_pool.append(source_uri)

        logger.info(
            "Connected to %s:%d stream=%s", host, port, stream_name
        )
        return connection

    def read_batch(self, batch_size: int = 1000) -> list[dict[str, Any]]:
        """Read a batch of records from all connected sources.

        Records are read round-robin from connected sources. The FormatDetector
        is used to determine parse strategy for raw payloads.

        Args:
            batch_size: Maximum number of records to return in this batch.

        Returns:
            A list of parsed record dictionaries.

        Raises:
            ZynofluxReadError: If no sources are connected.
        """
        self._ensure_open()

        if not self._sources:
            raise ZynofluxReadError(
                "No sources connected. Call connect() before reading."
            )

        records: list[dict[str, Any]] = []

        for source_uri in self._connection_pool:
            connection = self._sources[source_uri]
            remaining = batch_size - len(records)
            if remaining <= 0:
                break

            # Simulate raw payload retrieval and format detection
            sample_payload = b'{"id": 1, "value": "sample"}'
            detected: FormatMatch = self._format_detector.detect(sample_payload)

            logger.debug(
                "Source %s format=%s confidence=%.2f",
                source_uri,
                detected.format_name,
                detected.confidence,
            )

            # Parse records according to detected format
            batch_records = self._parse_payload(
                sample_payload, detected.format_name, remaining
            )
            connection.bytes_read += len(sample_payload) * len(batch_records)
            records.extend(batch_records)

        # Buffer overflow protection
        for record in records:
            if len(self._buffer) < self._buffer_size:
                self._buffer.append(record)

        return records

    def read_stream(self) -> Generator[dict[str, Any], None, None]:
        """Yield records one at a time from connected sources.

        This generator provides a streaming interface suitable for
        large or unbounded data sources where loading full batches
        into memory is impractical.

        Yields:
            Individual parsed record dictionaries.

        Raises:
            ZynofluxReadError: If no sources are connected.
        """
        self._ensure_open()

        if not self._sources:
            raise ZynofluxReadError(
                "No sources connected. Call connect() before reading."
            )

        # Drain the internal buffer first
        while self._buffer:
            yield self._buffer.popleft()

        # Then read fresh batches in a loop
        batch_count = 0
        max_batches = 100  # safety limit for non-infinite streams
        while batch_count < max_batches:
            batch = self.read_batch(batch_size=256)
            if not batch:
                break
            for record in batch:
                yield record
            batch_count += 1

    def close(self) -> None:
        """Close all connections and release resources.

        After calling close, the reader cannot be reused. Any buffered
        records are discarded.
        """
        if self._is_closed:
            return

        for source_uri, connection in self._sources.items():
            logger.info(
                "Closing connection to %s (bytes_read=%d)",
                source_uri,
                connection.bytes_read,
            )

        self._sources.clear()
        self._connection_pool.clear()
        self._buffer.clear()
        self._is_closed = True
        logger.info("ZynofluxReader closed. All connections released.")

    def _ensure_open(self) -> None:
        """Verify the reader has not been closed."""
        if self._is_closed:
            raise ZynofluxReadError(
                "Reader is closed. Create a new ZynofluxReader instance."
            )

    @staticmethod
    def _parse_payload(
        raw: bytes, format_name: str, max_records: int
    ) -> list[dict[str, Any]]:
        """Parse a raw payload into record dicts based on detected format.

        Args:
            raw: The raw byte payload.
            format_name: The detected format identifier.
            max_records: Maximum records to extract from this payload.

        Returns:
            List of parsed record dictionaries.
        """
        import json

        if format_name == "json":
            try:
                data = json.loads(raw)
                if isinstance(data, list):
                    return data[:max_records]
                return [data]
            except json.JSONDecodeError:
                logger.warning("Failed to parse JSON payload, returning empty batch")
                return []
        elif format_name == "csv":
            # CSV parsing would go here; placeholder for realistic structure
            lines = raw.decode("utf-8", errors="replace").strip().split("\n")
            return [{"raw_line": line} for line in lines[:max_records]]
        else:
            # For binary or unknown formats, wrap raw bytes
            return [{"raw_bytes": raw.hex(), "format": format_name}]
