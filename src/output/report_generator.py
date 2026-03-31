"""Pipeline execution and data quality report generation.

This module produces structured reports summarising pipeline execution
metrics, data quality statistics, and anomaly detection results. Reports
can be exported in HTML and JSON formats for integration with monitoring
dashboards and alerting systems.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Report:
    """A complete pipeline execution report.

    Attributes:
        timestamp: Unix timestamp when the report was generated.
        records_processed: Total records that entered the pipeline.
        records_output: Total records written to output sinks.
        anomalies_found: Number of records flagged as anomalous.
        validation_errors: Number of records that failed schema validation.
        duration_seconds: Total pipeline execution time.
        stages: Per-stage execution metrics.
        metadata: Arbitrary key-value metadata.
    """

    timestamp: float
    records_processed: int
    records_output: int
    anomalies_found: int
    validation_errors: int
    duration_seconds: float
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class ReportGenerator:
    """Generates data quality and pipeline execution reports.

    The generator accepts pipeline statistics collected during execution
    and produces a structured Report object. Reports can then be exported
    to HTML for human consumption or JSON for programmatic access.

    Args:
        include_record_details: Whether to include per-record detail in reports.
        max_error_samples: Maximum number of sample error records to include.
    """

    def __init__(
        self,
        include_record_details: bool = False,
        max_error_samples: int = 10,
    ) -> None:
        self._include_record_details = include_record_details
        self._max_error_samples = max_error_samples
        self._reports_generated: int = 0

    def generate(self, pipeline_stats: dict[str, Any]) -> Report:
        """Generate a report from pipeline execution statistics.

        Args:
            pipeline_stats: Dictionary of metrics collected during pipeline
                execution. Expected keys include ``records_in``, ``records_out``,
                ``anomalies``, ``validation_errors``, ``duration``, and ``stages``.

        Returns:
            A populated Report instance.
        """
        report = Report(
            timestamp=time.time(),
            records_processed=pipeline_stats.get("records_in", 0),
            records_output=pipeline_stats.get("records_out", 0),
            anomalies_found=pipeline_stats.get("anomalies", 0),
            validation_errors=pipeline_stats.get("validation_errors", 0),
            duration_seconds=pipeline_stats.get("duration", 0.0),
            stages=pipeline_stats.get("stages", {}),
            metadata=pipeline_stats.get("metadata", {}),
        )

        self._reports_generated += 1
        logger.info(
            "Generated report: %d records processed, %d anomalies, %.2fs duration",
            report.records_processed,
            report.anomalies_found,
            report.duration_seconds,
        )
        return report

    def export_html(self, report: Report, path: str) -> None:
        """Export a report as a self-contained HTML file.

        Args:
            report: The report to export.
            path: Filesystem path for the output HTML file.
        """
        html_parts = [
            "<!DOCTYPE html>",
            "<html lang='en'>",
            "<head><meta charset='utf-8'>",
            "<title>ZynofluxProcessor Pipeline Report</title>",
            "<style>",
            "body { font-family: system-ui, sans-serif; max-width: 900px; margin: 2em auto; }",
            "table { border-collapse: collapse; width: 100%; margin: 1em 0; }",
            "th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
            "th { background: #f5f5f5; }",
            ".metric { font-size: 1.4em; font-weight: bold; }",
            ".anomaly { color: #c0392b; }",
            ".ok { color: #27ae60; }",
            "</style>",
            "</head><body>",
            "<h1>Pipeline Execution Report</h1>",
            f"<p>Generated at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(report.timestamp))}</p>",
            "<h2>Summary</h2>",
            "<table>",
            f"<tr><td>Records Processed</td><td class='metric'>{report.records_processed}</td></tr>",
            f"<tr><td>Records Output</td><td class='metric'>{report.records_output}</td></tr>",
            f"<tr><td>Anomalies Found</td><td class='metric anomaly'>{report.anomalies_found}</td></tr>",
            f"<tr><td>Validation Errors</td><td class='metric anomaly'>{report.validation_errors}</td></tr>",
            f"<tr><td>Duration</td><td class='metric'>{report.duration_seconds:.3f}s</td></tr>",
            "</table>",
        ]

        if report.stages:
            html_parts.append("<h2>Stage Metrics</h2>")
            for stage_name, metrics in report.stages.items():
                html_parts.append(f"<h3>{stage_name}</h3>")
                html_parts.append("<table>")
                for key, value in metrics.items():
                    html_parts.append(f"<tr><td>{key}</td><td>{value}</td></tr>")
                html_parts.append("</table>")

        html_parts.extend(["</body>", "</html>"])

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(html_parts), encoding="utf-8")
        logger.info("Exported HTML report to %s", path)

    def export_json(self, report: Report, path: str) -> None:
        """Export a report as a JSON file.

        Args:
            report: The report to export.
            path: Filesystem path for the output JSON file.
        """
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        report_dict = asdict(report)
        output_path.write_text(
            json.dumps(report_dict, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info("Exported JSON report to %s", path)

    @property
    def stats(self) -> dict[str, Any]:
        """Return generator statistics."""
        return {
            "reports_generated": self._reports_generated,
            "include_record_details": self._include_record_details,
        }
