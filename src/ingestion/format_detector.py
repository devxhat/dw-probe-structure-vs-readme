"""Auto-detection of input data formats from raw byte streams.

This module provides format identification capabilities for the ingestion
layer, inspecting stream headers and magic bytes to determine the encoding
of incoming data before it is parsed by the ZynofluxReader.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import ClassVar

logger = logging.getLogger(__name__)


@dataclass
class FormatMatch:
    """Result of a format detection attempt."""

    format_name: str
    confidence: float
    header_bytes_matched: int


class FormatDetector:
    """Auto-detects input data format (JSON, CSV, Parquet, Avro) from stream headers.

    The detector maintains a registry of known format signatures and inspects
    the leading bytes of a data stream to determine its encoding. Custom
    formats can be registered at runtime via ``register_format``.

    Attributes:
        _custom_signatures: User-registered format signatures that extend
            the built-in detection capabilities.
    """

    MAGIC_SIGNATURES: ClassVar[dict[str, bytes]] = {
        "parquet": b"PAR1",
        "avro": b"Obj\x01",
        "gzip": b"\x1f\x8b",
        "zip": b"PK\x03\x04",
        "orc": b"ORC",
        "bson": b"\x00\x00\x00",  # length-prefixed, heuristic
    }

    TEXT_FORMAT_HEURISTICS: ClassVar[dict[str, list[bytes]]] = {
        "json": [b"{", b"["],
        "csv": [b",", b";"],
        "xml": [b"<?xml", b"<"],
        "yaml": [b"---", b"%YAML"],
    }

    def __init__(self) -> None:
        self._custom_signatures: dict[str, bytes] = {}

    def detect(self, raw_bytes: bytes) -> FormatMatch:
        """Detect the format of the provided raw byte stream.

        Inspects magic bytes first, then falls back to text-based heuristics.

        Args:
            raw_bytes: The leading bytes of the data stream to inspect.
                At least 8 bytes are recommended for reliable detection.

        Returns:
            A FormatMatch indicating the detected format and confidence level.

        Raises:
            ValueError: If raw_bytes is empty.
        """
        if not raw_bytes:
            raise ValueError("Cannot detect format from empty byte stream")

        # Check binary magic signatures first (highest confidence)
        all_signatures = {**self.MAGIC_SIGNATURES, **self._custom_signatures}
        for format_name, signature in all_signatures.items():
            if raw_bytes[: len(signature)] == signature:
                logger.debug("Detected binary format %s via magic bytes", format_name)
                return FormatMatch(
                    format_name=format_name,
                    confidence=0.95,
                    header_bytes_matched=len(signature),
                )

        # Fall back to text-based heuristic detection
        stripped = raw_bytes.lstrip()
        for format_name, prefixes in self.TEXT_FORMAT_HEURISTICS.items():
            for prefix in prefixes:
                if stripped.startswith(prefix):
                    logger.debug(
                        "Detected text format %s via heuristic", format_name
                    )
                    return FormatMatch(
                        format_name=format_name,
                        confidence=0.70,
                        header_bytes_matched=len(prefix),
                    )

        logger.warning("Unable to detect format, defaulting to raw")
        return FormatMatch(format_name="raw", confidence=0.10, header_bytes_matched=0)

    def register_format(self, name: str, signature: bytes) -> None:
        """Register a custom format signature for detection.

        Args:
            name: Identifier for the new format.
            signature: Magic bytes that identify this format.

        Raises:
            ValueError: If the name conflicts with a built-in format.
        """
        if name in self.MAGIC_SIGNATURES:
            raise ValueError(
                f"Cannot override built-in format '{name}'. "
                "Use a unique name for custom formats."
            )
        self._custom_signatures[name] = signature
        logger.info("Registered custom format '%s' with %d-byte signature", name, len(signature))

    def supported_formats(self) -> list[str]:
        """Return a sorted list of all supported format names.

        Returns:
            Alphabetically sorted list of format identifiers including both
            built-in and custom-registered formats.
        """
        builtin = set(self.MAGIC_SIGNATURES.keys()) | set(
            self.TEXT_FORMAT_HEURISTICS.keys()
        )
        custom = set(self._custom_signatures.keys())
        return sorted(builtin | custom)
