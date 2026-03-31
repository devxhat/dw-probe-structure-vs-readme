"""JSON Schema validation with detailed error reporting.

This module provides the primary validation gate in the ZynofluxProcessor
pipeline. Records that have been normalized and enriched are checked against
registered JSON Schema definitions before being forwarded to output sinks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import jsonschema
from jsonschema import Draft202012Validator, ValidationError as JsonSchemaValidationError

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Outcome of validating a single record against a schema.

    Attributes:
        is_valid: Whether the record passes all schema constraints.
        errors: List of error messages for hard constraint violations.
        warnings: List of warning messages for soft constraint issues.
        schema_name: The schema that was used for validation.
        fields_checked: Number of fields inspected during validation.
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    schema_name: str = ""
    fields_checked: int = 0


class SchemaNotFoundError(Exception):
    """Raised when a requested schema name is not in the registry."""


class SchemaValidator:
    """Validates records against JSON Schema definitions with detailed error reporting.

    The validator maintains a registry of named JSON Schemas and provides
    methods to validate individual records, collecting both hard errors and
    soft warnings. Schemas follow the JSON Schema Draft 2020-12 specification.

    Args:
        strict: If True, unknown properties cause validation errors.
            If False, unknown properties generate warnings instead.
        max_errors: Maximum number of errors to collect per validation
            before short-circuiting.
    """

    def __init__(self, strict: bool = True, max_errors: int = 50) -> None:
        self._schemas: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, Draft202012Validator] = {}
        self._strict = strict
        self._max_errors = max_errors
        self._validation_count: int = 0
        self._total_errors: int = 0

    def register_schema(self, name: str, schema: dict[str, Any]) -> None:
        """Register a JSON Schema for record validation.

        Args:
            name: Unique identifier for this schema.
            schema: A valid JSON Schema document (dict).

        Raises:
            jsonschema.SchemaError: If the schema itself is invalid.
            ValueError: If a schema with this name is already registered.
        """
        if name in self._schemas:
            raise ValueError(f"Schema '{name}' is already registered")

        # Validate the schema document itself
        Draft202012Validator.check_schema(schema)

        self._schemas[name] = schema
        self._validators[name] = Draft202012Validator(schema)
        logger.info(
            "Registered validation schema '%s' (%d properties)",
            name,
            len(schema.get("properties", {})),
        )

    def validate(self, record: dict[str, Any], schema_name: str) -> ValidationResult:
        """Validate a record against a named schema.

        Args:
            record: The data record to validate.
            schema_name: The name of the registered schema to validate against.

        Returns:
            A ValidationResult with errors and warnings.

        Raises:
            SchemaNotFoundError: If the schema_name is not registered.
        """
        if schema_name not in self._validators:
            raise SchemaNotFoundError(
                f"Schema '{schema_name}' not found. "
                f"Available: {self.list_schemas()}"
            )

        validator = self._validators[schema_name]
        errors: list[str] = []
        warnings: list[str] = []

        # Collect validation errors
        validation_errors = sorted(
            validator.iter_errors(record),
            key=lambda e: list(e.absolute_path),
        )

        for error in validation_errors[: self._max_errors]:
            path = ".".join(str(p) for p in error.absolute_path) or "(root)"
            message = f"{path}: {error.message}"
            errors.append(message)

        # Check for unknown properties (soft warning or hard error)
        schema = self._schemas[schema_name]
        known_properties = set(schema.get("properties", {}).keys())
        if known_properties:
            record_keys = set(record.keys())
            unknown = record_keys - known_properties
            # Exclude internal metadata fields from unknown check
            unknown = {k for k in unknown if not k.startswith("_")}
            if unknown:
                msg = f"Unknown properties: {sorted(unknown)}"
                if self._strict:
                    errors.append(msg)
                else:
                    warnings.append(msg)

        fields_checked = len(schema.get("properties", {}))
        is_valid = len(errors) == 0

        self._validation_count += 1
        self._total_errors += len(errors)

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            schema_name=schema_name,
            fields_checked=fields_checked,
        )

    def list_schemas(self) -> list[str]:
        """Return a sorted list of all registered schema names.

        Returns:
            Alphabetically sorted schema identifiers.
        """
        return sorted(self._schemas.keys())

    @property
    def stats(self) -> dict[str, Any]:
        """Return validation statistics."""
        return {
            "schemas_registered": len(self._schemas),
            "records_validated": self._validation_count,
            "total_errors": self._total_errors,
            "strict_mode": self._strict,
        }
