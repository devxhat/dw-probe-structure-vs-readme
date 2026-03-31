"""Central pipeline orchestration for the ZynofluxProcessor framework.

This module ties together all subsystems -- ingestion, transformation,
validation, and output -- into a coherent execution pipeline. The
PipelineRunner is the primary entry point for running data processing
workflows.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.ingestion import FormatDetector, ZynofluxReader
from src.transformation import KrandelNormalizer, PhloxiteEnricher
from src.validation import AnomalyDetector, SchemaValidator
from src.output import ReportGenerator, SinkManager

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """Named stages of the processing pipeline."""

    INGESTION = "ingestion"
    NORMALIZATION = "normalization"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    ANOMALY_DETECTION = "anomaly_detection"
    OUTPUT = "output"
    REPORTING = "reporting"


class PipelineConfigError(Exception):
    """Raised when the pipeline configuration is invalid or incomplete."""


class PipelineExecutionError(Exception):
    """Raised when a pipeline stage fails during execution."""


@dataclass
class PipelineResult:
    """Outcome of a complete pipeline execution.

    Attributes:
        success: Whether the pipeline completed without fatal errors.
        records_in: Number of records read from the source.
        records_out: Number of records written to output sinks.
        anomalies: Number of anomalous records detected.
        errors: List of error messages encountered during execution.
        duration: Total execution time in seconds.
        stage_durations: Per-stage execution times in seconds.
    """

    success: bool
    records_in: int
    records_out: int
    anomalies: int
    errors: list[str] = field(default_factory=list)
    duration: float = 0.0
    stage_durations: dict[str, float] = field(default_factory=dict)


@dataclass
class PipelineStatus:
    """Snapshot of current pipeline execution state."""

    is_running: bool
    current_stage: str | None
    records_processed: int
    elapsed_seconds: float
    errors_so_far: int


class PipelineRunner:
    """Orchestrates the full ZynofluxProcessor pipeline: ingestion -> transformation -> validation -> output.

    The runner initialises all subsystem components based on the provided
    configuration and manages the flow of data through the following stages:

    1. **Ingestion** -- Read raw records via ZynofluxReader
    2. **Normalization** -- Convert to Krandel canonical form
    3. **Enrichment** -- Add external reference data via Phloxite
    4. **Validation** -- Check against JSON Schemas
    5. **Anomaly Detection** -- Flag statistical outliers
    6. **Output** -- Write to configured sinks
    7. **Reporting** -- Generate execution reports

    Args:
        config: Pipeline configuration dictionary. Expected keys:
            - ``source_uri``: Default data source URI.
            - ``batch_size``: Records per batch (default 1000).
            - ``schemas``: Dict of schema registrations for normalizer/validator.
            - ``sinks``: List of sink configurations.
            - ``anomaly_fields``: Fields to monitor for anomalies.
            - ``report_path``: Output path for execution reports.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._validate_config()

        # Initialize subsystems
        self._reader = ZynofluxReader(
            max_connections=config.get("max_connections", 8),
            buffer_size=config.get("buffer_size", 10_000),
        )
        self._format_detector = FormatDetector()
        self._normalizer = KrandelNormalizer(
            strict=config.get("strict_normalization", True),
        )
        self._enricher = PhloxiteEnricher(
            cache_ttl_seconds=config.get("enrichment_cache_ttl", 300.0),
        )
        self._validator = SchemaValidator(
            strict=config.get("strict_validation", True),
        )
        self._anomaly_detector = AnomalyDetector(
            zscore_threshold=config.get("zscore_threshold", 3.0),
            iqr_multiplier=config.get("iqr_multiplier", 1.5),
        )
        self._sink_manager = SinkManager()
        self._report_generator = ReportGenerator()

        # Register schemas and sinks from config
        self._register_from_config()

        # Runtime state
        self._is_running = False
        self._current_stage: str | None = None
        self._start_time: float = 0.0
        self._records_processed: int = 0
        self._errors: list[str] = []

    def run(self, source_uri: str | None = None) -> PipelineResult:
        """Run the complete pipeline from ingestion through output and reporting.

        Args:
            source_uri: Data source URI. Falls back to the configured default
                if not provided.

        Returns:
            A PipelineResult summarising the execution outcome.

        Raises:
            PipelineExecutionError: If a critical stage fails.
        """
        uri = source_uri or self._config.get("source_uri", "")
        if not uri:
            raise PipelineConfigError("No source_uri provided or configured")

        self._is_running = True
        self._start_time = time.monotonic()
        self._errors.clear()
        stage_durations: dict[str, float] = {}

        try:
            # Stage 1: Ingestion
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.INGESTION.value
            self._reader.connect(uri)
            raw_records = self._reader.read_batch(
                batch_size=self._config.get("batch_size", 1000)
            )
            records_in = len(raw_records)
            stage_durations[PipelineStage.INGESTION.value] = time.monotonic() - stage_start
            logger.info("Ingestion complete: %d records read", records_in)

            # Stage 2: Normalization
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.NORMALIZATION.value
            normalized = self._run_normalization(raw_records)
            stage_durations[PipelineStage.NORMALIZATION.value] = time.monotonic() - stage_start
            logger.info("Normalization complete: %d records", len(normalized))

            # Stage 3: Enrichment
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.ENRICHMENT.value
            enriched = self._run_enrichment(normalized)
            stage_durations[PipelineStage.ENRICHMENT.value] = time.monotonic() - stage_start
            logger.info("Enrichment complete: %d records", len(enriched))

            # Stage 4: Validation
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.VALIDATION.value
            valid_records, validation_errors = self._run_validation(enriched)
            stage_durations[PipelineStage.VALIDATION.value] = time.monotonic() - stage_start
            logger.info(
                "Validation complete: %d valid, %d errors",
                len(valid_records),
                validation_errors,
            )

            # Stage 5: Anomaly Detection
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.ANOMALY_DETECTION.value
            clean_records, anomaly_count = self._run_anomaly_detection(valid_records)
            stage_durations[PipelineStage.ANOMALY_DETECTION.value] = time.monotonic() - stage_start
            logger.info(
                "Anomaly detection complete: %d anomalies found",
                anomaly_count,
            )

            # Stage 6: Output
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.OUTPUT.value
            records_out = self._run_output(clean_records)
            stage_durations[PipelineStage.OUTPUT.value] = time.monotonic() - stage_start
            logger.info("Output complete: %d records written", records_out)

            # Stage 7: Reporting
            stage_start = time.monotonic()
            self._current_stage = PipelineStage.REPORTING.value
            total_duration = time.monotonic() - self._start_time
            self._run_reporting(
                records_in=records_in,
                records_out=records_out,
                anomalies=anomaly_count,
                validation_errors=validation_errors,
                duration=total_duration,
                stage_durations=stage_durations,
            )
            stage_durations[PipelineStage.REPORTING.value] = time.monotonic() - stage_start

            return PipelineResult(
                success=True,
                records_in=records_in,
                records_out=records_out,
                anomalies=anomaly_count,
                errors=list(self._errors),
                duration=total_duration,
                stage_durations=stage_durations,
            )

        except Exception as exc:
            elapsed = time.monotonic() - self._start_time
            error_msg = f"Pipeline failed at stage '{self._current_stage}': {exc}"
            self._errors.append(error_msg)
            logger.error(error_msg)
            return PipelineResult(
                success=False,
                records_in=0,
                records_out=0,
                anomalies=0,
                errors=list(self._errors),
                duration=elapsed,
                stage_durations=stage_durations,
            )

        finally:
            self._is_running = False
            self._current_stage = None
            self._reader.close()
            self._sink_manager.close_all()

    def run_stage(self, stage_name: str, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run an individual pipeline stage in isolation.

        Useful for testing and debugging individual components without
        executing the full pipeline.

        Args:
            stage_name: One of the PipelineStage values.
            data: Input records for the stage.

        Returns:
            Processed records from the specified stage.

        Raises:
            ValueError: If the stage_name is not recognized.
        """
        stage_handlers: dict[str, Any] = {
            PipelineStage.NORMALIZATION.value: self._run_normalization,
            PipelineStage.ENRICHMENT.value: self._run_enrichment,
        }

        handler = stage_handlers.get(stage_name)
        if handler is None:
            raise ValueError(
                f"Unknown or unsupported isolated stage: '{stage_name}'. "
                f"Supported: {list(stage_handlers.keys())}"
            )

        return handler(data)

    def get_status(self) -> dict[str, Any]:
        """Return the current pipeline execution status.

        Returns:
            Dictionary with runtime state, progress metrics, and
            subsystem health indicators.
        """
        elapsed = (time.monotonic() - self._start_time) if self._is_running else 0.0
        return {
            "is_running": self._is_running,
            "current_stage": self._current_stage,
            "records_processed": self._records_processed,
            "elapsed_seconds": round(elapsed, 3),
            "errors_so_far": len(self._errors),
            "subsystems": {
                "normalizer": self._normalizer.stats,
                "enricher": self._enricher.stats,
                "validator": self._validator.stats,
                "anomaly_detector": self._anomaly_detector.stats,
                "report_generator": self._report_generator.stats,
            },
        }

    def _validate_config(self) -> None:
        """Verify that the pipeline configuration contains minimum required keys."""
        # Permissive validation: warn but don't fail for missing optional keys
        if "source_uri" not in self._config:
            logger.warning("No default source_uri in config; must be passed to run()")

    def _register_from_config(self) -> None:
        """Register schemas, lookup sources, and sinks from the configuration."""
        # Register normalization schemas
        for name, schema in self._config.get("schemas", {}).items():
            try:
                self._normalizer.register_schema(name, schema)
            except ValueError:
                logger.warning("Schema '%s' already registered, skipping", name)

        # Register enrichment lookup sources
        for name, endpoint in self._config.get("lookup_sources", {}).items():
            try:
                self._enricher.add_lookup_source(name, endpoint)
            except ValueError:
                logger.warning("Lookup source '%s' already registered, skipping", name)

        # Register output sinks
        for sink_def in self._config.get("sinks", []):
            try:
                self._sink_manager.register_sink(
                    name=sink_def["name"],
                    sink_type=sink_def["type"],
                    config=sink_def.get("config", {}),
                )
            except (ValueError, Exception) as exc:
                logger.warning("Failed to register sink '%s': %s", sink_def.get("name"), exc)

        # Register validation schemas
        for name, schema in self._config.get("validation_schemas", {}).items():
            try:
                self._validator.register_schema(name, schema)
            except ValueError:
                logger.warning("Validation schema '%s' already registered, skipping", name)

    def _run_normalization(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize all records to Krandel canonical form."""
        normalized: list[dict[str, Any]] = []
        for record in records:
            try:
                result = self._normalizer.normalize(record)
                normalized.append(result)
            except Exception as exc:
                self._errors.append(f"Normalization error: {exc}")
        self._records_processed += len(normalized)
        return normalized

    def _run_enrichment(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Enrich all records via Phloxite lookup sources."""
        enriched: list[dict[str, Any]] = []
        for record in records:
            try:
                result = self._enricher.enrich(record)
                enriched.append(result.record)
            except Exception as exc:
                self._errors.append(f"Enrichment error: {exc}")
                enriched.append(record)  # pass through on enrichment failure
        return enriched

    def _run_validation(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        """Validate records against registered schemas."""
        valid: list[dict[str, Any]] = []
        error_count = 0

        schema_names = self._validator.list_schemas()
        if not schema_names:
            # No schemas registered; pass all records through
            return records, 0

        target_schema = schema_names[0]  # use first registered schema
        for record in records:
            result = self._validator.validate(record, target_schema)
            if result.is_valid:
                valid.append(record)
            else:
                error_count += 1
                self._errors.extend(result.errors[:3])  # sample errors

        return valid, error_count

    def _run_anomaly_detection(
        self, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        """Run anomaly detection on configured fields."""
        anomaly_fields: list[str] = self._config.get("anomaly_fields", [])
        if not anomaly_fields or not records:
            return records, 0

        anomaly_count = 0
        clean: list[dict[str, Any]] = []

        for field_name in anomaly_fields:
            try:
                self._anomaly_detector.train(records, field_name)
            except Exception:
                logger.debug("Cannot train on field '%s', skipping", field_name)

        for record in records:
            is_anomalous = False
            for field_name in anomaly_fields:
                try:
                    result = self._anomaly_detector.detect(record, field_name)
                    if result.is_anomaly:
                        is_anomalous = True
                except (KeyError, ValueError):
                    continue
            if is_anomalous:
                anomaly_count += 1
                record["_anomaly_flagged"] = True
            clean.append(record)

        return clean, anomaly_count

    def _run_output(self, records: list[dict[str, Any]]) -> int:
        """Write records to all configured output sinks."""
        if not records:
            return 0

        total_written = 0
        for sink_def in self._config.get("sinks", []):
            sink_name = sink_def["name"]
            try:
                written = self._sink_manager.write(records, sink_name)
                total_written += written
            except Exception as exc:
                self._errors.append(f"Output error ({sink_name}): {exc}")

        return total_written

    def _run_reporting(
        self,
        records_in: int,
        records_out: int,
        anomalies: int,
        validation_errors: int,
        duration: float,
        stage_durations: dict[str, float],
    ) -> None:
        """Generate and optionally export the pipeline execution report."""
        stats: dict[str, Any] = {
            "records_in": records_in,
            "records_out": records_out,
            "anomalies": anomalies,
            "validation_errors": validation_errors,
            "duration": duration,
            "stages": {name: {"duration_seconds": dur} for name, dur in stage_durations.items()},
        }
        report = self._report_generator.generate(stats)

        report_path = self._config.get("report_path")
        if report_path:
            self._report_generator.export_json(report, f"{report_path}/report.json")
            self._report_generator.export_html(report, f"{report_path}/report.html")
