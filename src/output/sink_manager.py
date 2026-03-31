"""Output sink management for processed data streams.

This module manages the routing of validated, enriched records to their
final destinations. Multiple sink types are supported, and records can
be written to several sinks simultaneously.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class SinkType(str, Enum):
    """Supported output sink types."""

    FILE = "file"
    DATABASE = "database"
    API = "api"
    QUEUE = "queue"


class SinkConfigError(Exception):
    """Raised when a sink configuration is invalid."""


class SinkWriteError(Exception):
    """Raised when a write operation to a sink fails after retries."""


@dataclass
class SinkConfig:
    """Configuration for a single output sink."""

    name: str
    sink_type: SinkType
    config: dict[str, Any]
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    enabled: bool = True


@dataclass
class SinkStatus:
    """Runtime status of a registered sink."""

    name: str
    sink_type: str
    records_written: int
    bytes_written: int
    errors: int
    last_write_at: float | None
    is_healthy: bool


class SinkManager:
    """Manages output destinations (sinks) for processed data streams.

    The sink manager maintains a registry of configured output destinations
    and provides a unified write interface. Records can be routed to specific
    sinks by name or broadcast to all registered sinks.

    Supported sink types:
        - **file**: Writes JSON lines to local or remote filesystem paths.
        - **database**: Inserts records into relational or document databases.
        - **api**: POSTs records to HTTP/gRPC endpoints.
        - **queue**: Publishes records to message queues (Kafka, RabbitMQ).
    """

    REQUIRED_CONFIG_KEYS: dict[SinkType, list[str]] = {
        SinkType.FILE: ["path"],
        SinkType.DATABASE: ["connection_string", "table"],
        SinkType.API: ["url", "method"],
        SinkType.QUEUE: ["broker", "topic"],
    }

    def __init__(self) -> None:
        self._sinks: dict[str, SinkConfig] = {}
        self._sink_stats: dict[str, SinkStatus] = {}

    def register_sink(self, name: str, sink_type: str, config: dict[str, Any]) -> None:
        """Register an output sink.

        Args:
            name: Unique identifier for this sink.
            sink_type: One of 'file', 'database', 'api', or 'queue'.
            config: Sink-specific configuration dictionary.

        Raises:
            SinkConfigError: If the sink type is unknown or required config keys
                are missing.
            ValueError: If a sink with this name is already registered.
        """
        if name in self._sinks:
            raise ValueError(f"Sink '{name}' is already registered")

        try:
            resolved_type = SinkType(sink_type)
        except ValueError:
            raise SinkConfigError(
                f"Unknown sink type '{sink_type}'. "
                f"Supported: {[t.value for t in SinkType]}"
            )

        required_keys = self.REQUIRED_CONFIG_KEYS.get(resolved_type, [])
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise SinkConfigError(
                f"Missing required config keys for {sink_type} sink: {missing}"
            )

        sink_config = SinkConfig(name=name, sink_type=resolved_type, config=config)
        self._sinks[name] = sink_config
        self._sink_stats[name] = SinkStatus(
            name=name,
            sink_type=sink_type,
            records_written=0,
            bytes_written=0,
            errors=0,
            last_write_at=None,
            is_healthy=True,
        )
        logger.info("Registered %s sink '%s'", sink_type, name)

    def write(self, records: list[dict[str, Any]], sink_name: str) -> int:
        """Write records to a specific sink.

        Args:
            records: List of record dictionaries to write.
            sink_name: The target sink identifier.

        Returns:
            Number of records successfully written.

        Raises:
            KeyError: If the sink name is not registered.
            SinkWriteError: If writing fails after all retry attempts.
        """
        if sink_name not in self._sinks:
            raise KeyError(
                f"Sink '{sink_name}' not found. "
                f"Available: {list(self._sinks.keys())}"
            )

        sink = self._sinks[sink_name]
        stats = self._sink_stats[sink_name]

        if not sink.enabled:
            logger.warning("Sink '%s' is disabled, skipping write", sink_name)
            return 0

        written = 0
        for attempt in range(1, sink.max_retries + 1):
            try:
                written = self._dispatch_write(sink, records)
                stats.records_written += written
                stats.bytes_written += sum(len(json.dumps(r).encode()) for r in records)
                stats.last_write_at = time.time()
                stats.is_healthy = True
                break
            except Exception as exc:
                stats.errors += 1
                logger.warning(
                    "Write to '%s' failed (attempt %d/%d): %s",
                    sink_name,
                    attempt,
                    sink.max_retries,
                    exc,
                )
                if attempt == sink.max_retries:
                    stats.is_healthy = False
                    raise SinkWriteError(
                        f"Failed to write to '{sink_name}' after "
                        f"{sink.max_retries} attempts"
                    ) from exc
                time.sleep(sink.retry_delay_seconds)

        return written

    def flush_all(self) -> dict[str, int]:
        """Flush buffered data across all sinks.

        Returns:
            Mapping of sink names to number of records flushed.
        """
        results: dict[str, int] = {}
        for name in self._sinks:
            # In a real implementation, each sink would have an internal buffer
            results[name] = 0
            logger.debug("Flushed sink '%s'", name)
        return results

    def close_all(self) -> None:
        """Close all sink connections and release resources."""
        for name, sink in self._sinks.items():
            logger.info(
                "Closing sink '%s' (type=%s, records_written=%d)",
                name,
                sink.sink_type.value,
                self._sink_stats[name].records_written,
            )
        self._sinks.clear()
        self._sink_stats.clear()
        logger.info("All sinks closed")

    def get_sink_status(self, name: str) -> SinkStatus:
        """Retrieve runtime status for a specific sink.

        Args:
            name: The sink identifier.

        Returns:
            SinkStatus with current metrics.

        Raises:
            KeyError: If the sink is not registered.
        """
        if name not in self._sink_stats:
            raise KeyError(f"Sink '{name}' not found")
        return self._sink_stats[name]

    @staticmethod
    def _dispatch_write(sink: SinkConfig, records: list[dict[str, Any]]) -> int:
        """Route the write to the appropriate sink backend.

        Args:
            sink: The target sink configuration.
            records: Records to write.

        Returns:
            Number of records written.
        """
        if sink.sink_type == SinkType.FILE:
            path = Path(sink.config["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                for record in records:
                    fh.write(json.dumps(record, default=str) + "\n")
            return len(records)

        elif sink.sink_type == SinkType.DATABASE:
            # Simulated database insert
            logger.debug(
                "Inserting %d records into table '%s'",
                len(records),
                sink.config["table"],
            )
            return len(records)

        elif sink.sink_type == SinkType.API:
            # Simulated API POST
            logger.debug(
                "POSTing %d records to %s",
                len(records),
                sink.config["url"],
            )
            return len(records)

        elif sink.sink_type == SinkType.QUEUE:
            # Simulated queue publish
            logger.debug(
                "Publishing %d records to %s/%s",
                len(records),
                sink.config["broker"],
                sink.config["topic"],
            )
            return len(records)

        return 0
