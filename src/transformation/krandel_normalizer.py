"""Krandel canonical form normalization for heterogeneous data schemas.

This module implements the transformation layer's primary normalization
component. Raw records from the ingestion stage are converted into the
unified Krandel canonical form, enabling consistent downstream validation
and output regardless of the original source format.
"""

from __future__ import annotations

import hashlib
import logging
import time
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    """Raised when a record cannot be normalized to Krandel canonical form."""


@dataclass
class SchemaMapping:
    """Defines how a source schema maps to the Krandel canonical form."""

    name: str
    field_map: dict[str, str]
    required_fields: list[str]
    default_values: dict[str, Any] = field(default_factory=dict)
    coercions: dict[str, str] = field(default_factory=dict)


class KrandelNormalizer:
    """Normalizes heterogeneous data schemas into the unified Krandel canonical form.

    The normalizer maintains a registry of source schemas and their mappings
    to the canonical form. Each incoming record is matched against a registered
    schema, its fields are renamed and coerced, required fields are verified,
    and metadata is injected before the record is passed downstream.

    Attributes:
        _schema_registry: Mapping of schema names to their SchemaMapping definitions.
        _normalization_count: Running count of records successfully normalized.
    """

    CANONICAL_META_FIELDS: list[str] = [
        "_krandel_version",
        "_normalized_at",
        "_source_schema",
        "_record_hash",
    ]

    def __init__(self, strict: bool = True) -> None:
        self._schema_registry: dict[str, SchemaMapping] = {}
        self._normalization_count: int = 0
        self._strict = strict

    def register_schema(self, name: str, schema: dict[str, Any]) -> None:
        """Register a source schema mapping for normalization.

        Args:
            name: Unique identifier for this schema.
            schema: Dictionary containing ``field_map``, ``required_fields``,
                and optionally ``default_values`` and ``coercions``.

        Raises:
            ValueError: If a schema with the same name is already registered.
        """
        if name in self._schema_registry:
            raise ValueError(f"Schema '{name}' is already registered")

        mapping = SchemaMapping(
            name=name,
            field_map=schema.get("field_map", {}),
            required_fields=schema.get("required_fields", []),
            default_values=schema.get("default_values", {}),
            coercions=schema.get("coercions", {}),
        )
        self._schema_registry[name] = mapping
        logger.info("Registered schema '%s' with %d field mappings", name, len(mapping.field_map))

    def normalize(self, record: dict[str, Any], schema_name: str | None = None) -> dict[str, Any]:
        """Normalize a record into the Krandel canonical form.

        Args:
            record: The raw input record to normalize.
            schema_name: Optional schema to use. If not provided, the normalizer
                attempts to auto-detect the schema from record keys.

        Returns:
            A new dictionary in the Krandel canonical form.

        Raises:
            NormalizationError: If required fields are missing and strict mode is on.
        """
        if schema_name is None:
            schema_name = self._detect_schema(record)

        mapping = self._schema_registry.get(schema_name)
        if mapping is None:
            if self._strict:
                raise NormalizationError(
                    f"No schema registered for '{schema_name}'. "
                    f"Available: {list(self._schema_registry.keys())}"
                )
            # Passthrough mode: wrap record with metadata only
            return self._inject_metadata(deepcopy(record), schema_name="_passthrough")

        # Apply field mapping
        canonical: dict[str, Any] = {}
        for source_field, target_field in mapping.field_map.items():
            if source_field in record:
                value = record[source_field]
                # Apply type coercion if specified
                if target_field in mapping.coercions:
                    value = self._coerce(value, mapping.coercions[target_field])
                canonical[target_field] = value

        # Apply defaults for missing fields
        for field_name, default in mapping.default_values.items():
            if field_name not in canonical:
                canonical[field_name] = default

        # Verify required fields
        missing = [f for f in mapping.required_fields if f not in canonical]
        if missing and self._strict:
            raise NormalizationError(
                f"Missing required fields after normalization: {missing}"
            )

        canonical = self._inject_metadata(canonical, schema_name=schema_name)
        self._normalization_count += 1
        return canonical

    def validate_canonical(self, record: dict[str, Any]) -> bool:
        """Check whether a record conforms to the Krandel canonical form.

        Args:
            record: The record to validate.

        Returns:
            True if all canonical metadata fields are present and well-formed.
        """
        for meta_field in self.CANONICAL_META_FIELDS:
            if meta_field not in record:
                return False
        if record.get("_krandel_version") != "2.1":
            return False
        return True

    @property
    def stats(self) -> dict[str, Any]:
        """Return normalization statistics."""
        return {
            "schemas_registered": len(self._schema_registry),
            "records_normalized": self._normalization_count,
            "strict_mode": self._strict,
        }

    def _detect_schema(self, record: dict[str, Any]) -> str:
        """Attempt to auto-detect schema by matching record keys against registrations."""
        best_match: str | None = None
        best_score: float = 0.0

        record_keys = set(record.keys())
        for name, mapping in self._schema_registry.items():
            schema_keys = set(mapping.field_map.keys())
            if not schema_keys:
                continue
            overlap = len(record_keys & schema_keys) / len(schema_keys)
            if overlap > best_score:
                best_score = overlap
                best_match = name

        if best_match is None or best_score < 0.5:
            return "_unknown"
        return best_match

    @staticmethod
    def _coerce(value: Any, target_type: str) -> Any:
        """Coerce a value to the specified target type."""
        coercion_map: dict[str, type] = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
        }
        converter = coercion_map.get(target_type)
        if converter is None:
            return value
        try:
            return converter(value)
        except (ValueError, TypeError):
            return value

    @staticmethod
    def _inject_metadata(record: dict[str, Any], schema_name: str) -> dict[str, Any]:
        """Inject Krandel canonical metadata into a record."""
        raw_repr = str(sorted(record.items()))
        record_hash = hashlib.sha256(raw_repr.encode()).hexdigest()[:16]

        record["_krandel_version"] = "2.1"
        record["_normalized_at"] = time.time()
        record["_source_schema"] = schema_name
        record["_record_hash"] = record_hash
        return record
