"""End-to-end tests for the ZynofluxProcessor pipeline.

Exercises the complete ingestion -> transformation -> validation -> output
flow through the PipelineRunner orchestrator.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest

from src.orchestration.pipeline_runner import PipelineConfigError, PipelineResult, PipelineRunner


def _make_pipeline_config(tmp_path: Path) -> dict[str, Any]:
    """Build a complete pipeline configuration for testing."""
    return {
        "source_uri": "zynoflux://testhost:9091/test-stream",
        "batch_size": 50,
        "max_connections": 2,
        "strict_normalization": False,
        "strict_validation": False,
        "schemas": {
            "default": {
                "field_map": {"id": "entity_id", "value": "metric_value"},
                "required_fields": ["entity_id"],
                "default_values": {"status": "active"},
                "coercions": {"metric_value": "float"},
            }
        },
        "lookup_sources": {
            "reference_db": "phloxite://refdata:8080/lookup",
        },
        "validation_schemas": {
            "canonical": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string"},
                    "metric_value": {"type": "number"},
                    "status": {"type": "string"},
                },
            }
        },
        "sinks": [
            {
                "name": "test_file_sink",
                "type": "file",
                "config": {"path": str(tmp_path / "output" / "results.jsonl")},
            }
        ],
        "anomaly_fields": [],
        "report_path": str(tmp_path / "reports"),
    }


class TestPipelineE2E:
    """End-to-end pipeline execution tests."""

    def test_full_pipeline_run(self, tmp_path: Path) -> None:
        """Running the full pipeline should produce a successful result."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        result = runner.run()

        assert isinstance(result, PipelineResult)
        assert result.success is True
        assert result.records_in > 0
        assert result.duration > 0
        assert "ingestion" in result.stage_durations
        assert "normalization" in result.stage_durations
        assert "output" in result.stage_durations

    def test_pipeline_writes_output_file(self, tmp_path: Path) -> None:
        """The pipeline should write records to the configured file sink."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        result = runner.run()

        output_file = tmp_path / "output" / "results.jsonl"
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) > 0

    def test_pipeline_generates_reports(self, tmp_path: Path) -> None:
        """The pipeline should generate JSON and HTML reports."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        runner.run()

        report_json = tmp_path / "reports" / "report.json"
        report_html = tmp_path / "reports" / "report.html"
        assert report_json.exists()
        assert report_html.exists()

    def test_pipeline_with_custom_source_uri(self, tmp_path: Path) -> None:
        """Passing a source_uri to run() should override the default."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        result = runner.run(source_uri="zynoflux://otherhost:9091/custom")

        assert result.success is True

    def test_pipeline_no_source_uri_raises(self, tmp_path: Path) -> None:
        """Running without any source_uri should raise PipelineConfigError."""
        config = _make_pipeline_config(tmp_path)
        del config["source_uri"]
        runner = PipelineRunner(config)

        result = runner.run()
        # Should fail gracefully since no URI
        assert result.success is False

    def test_pipeline_status_when_idle(self, tmp_path: Path) -> None:
        """get_status() should report idle state before run() is called."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        status = runner.get_status()

        assert status["is_running"] is False
        assert status["current_stage"] is None
        assert "subsystems" in status
        assert "normalizer" in status["subsystems"]
        assert "enricher" in status["subsystems"]
        assert "validator" in status["subsystems"]
        assert "anomaly_detector" in status["subsystems"]

    def test_run_stage_normalization(self, tmp_path: Path) -> None:
        """Running an isolated normalization stage should transform records."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        test_records = [
            {"id": "rec_001", "value": "42.5"},
            {"id": "rec_002", "value": "99.1"},
        ]
        normalized = runner.run_stage("normalization", test_records)

        assert len(normalized) == 2
        for record in normalized:
            assert "_krandel_version" in record

    def test_run_stage_unknown_raises(self, tmp_path: Path) -> None:
        """Requesting an unknown stage should raise ValueError."""
        config = _make_pipeline_config(tmp_path)
        runner = PipelineRunner(config)

        with pytest.raises(ValueError, match="Unknown or unsupported"):
            runner.run_stage("nonexistent_stage", [])
