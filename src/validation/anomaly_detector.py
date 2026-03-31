"""Statistical anomaly detection for data quality monitoring.

This module provides the data quality layer of the ZynofluxProcessor
pipeline. Validated records are inspected for statistical outliers using
configurable detection methods, enabling automated flagging of suspect
data before it reaches output sinks.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class DetectionMethod(str, Enum):
    """Supported anomaly detection methods."""

    ZSCORE = "zscore"
    IQR = "iqr"
    MODIFIED_ZSCORE = "modified_zscore"


@dataclass
class AnomalyResult:
    """Outcome of an anomaly detection check on a single field value.

    Attributes:
        is_anomaly: Whether the value is considered anomalous.
        score: The computed anomaly score (interpretation depends on method).
        method: The detection method that was applied.
        threshold: The threshold that was used for the decision.
        field_name: The field that was inspected.
        value: The raw value that was evaluated.
    """

    is_anomaly: bool
    score: float
    method: str
    threshold: float
    field_name: str = ""
    value: float | None = None


@dataclass
class FieldBaseline:
    """Statistical baseline for a single numeric field."""

    mean: float
    std: float
    median: float
    q1: float
    q3: float
    iqr: float
    mad: float  # median absolute deviation
    sample_count: int


class InsufficientDataError(Exception):
    """Raised when there is not enough training data to establish a baseline."""


class AnomalyDetector:
    """Statistical anomaly detection for data quality monitoring using z-score and IQR methods.

    The detector must be trained on a representative sample of records
    before it can be used for detection. Training computes statistical
    baselines (mean, std, quartiles) per field, which are then used to
    score incoming values.

    Args:
        default_method: The detection method to use when none is specified.
        zscore_threshold: Z-score above which a value is considered anomalous.
        iqr_multiplier: IQR multiplier for outlier fences (typically 1.5 or 3.0).
        min_training_samples: Minimum records required to establish a baseline.
    """

    def __init__(
        self,
        default_method: DetectionMethod = DetectionMethod.ZSCORE,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        min_training_samples: int = 30,
    ) -> None:
        self._default_method = default_method
        self._zscore_threshold = zscore_threshold
        self._iqr_multiplier = iqr_multiplier
        self._min_training_samples = min_training_samples
        self._baselines: dict[str, FieldBaseline] = {}
        self._detection_count: int = 0
        self._anomaly_count: int = 0

    def train(self, records: list[dict[str, Any]], field_name: str) -> FieldBaseline:
        """Compute a statistical baseline for the specified field.

        Args:
            records: Training records containing the target field.
            field_name: The numeric field to compute statistics for.

        Returns:
            The computed FieldBaseline.

        Raises:
            InsufficientDataError: If fewer than ``min_training_samples``
                records contain valid numeric values for the field.
        """
        values = []
        for record in records:
            raw = record.get(field_name)
            if raw is not None:
                try:
                    values.append(float(raw))
                except (ValueError, TypeError):
                    continue

        if len(values) < self._min_training_samples:
            raise InsufficientDataError(
                f"Need at least {self._min_training_samples} samples for field "
                f"'{field_name}', got {len(values)}"
            )

        arr = np.array(values, dtype=np.float64)
        q1 = float(np.percentile(arr, 25))
        q3 = float(np.percentile(arr, 75))
        median = float(np.median(arr))
        mad = float(np.median(np.abs(arr - median)))

        baseline = FieldBaseline(
            mean=float(np.mean(arr)),
            std=float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
            median=median,
            q1=q1,
            q3=q3,
            iqr=q3 - q1,
            mad=mad,
            sample_count=len(values),
        )

        self._baselines[field_name] = baseline
        logger.info(
            "Trained baseline for '%s': mean=%.4f, std=%.4f, iqr=%.4f (n=%d)",
            field_name,
            baseline.mean,
            baseline.std,
            baseline.iqr,
            baseline.sample_count,
        )
        return baseline

    def detect(
        self,
        record: dict[str, Any],
        field_name: str,
        method: DetectionMethod | None = None,
    ) -> AnomalyResult:
        """Check whether a record's field value is anomalous.

        Args:
            record: The record to inspect.
            field_name: The numeric field to evaluate.
            method: Detection method to use; defaults to the detector's
                configured default.

        Returns:
            An AnomalyResult describing the detection outcome.

        Raises:
            KeyError: If no baseline has been trained for the specified field.
            ValueError: If the field value cannot be converted to float.
        """
        if field_name not in self._baselines:
            raise KeyError(
                f"No baseline for field '{field_name}'. "
                "Call train() before detect()."
            )

        baseline = self._baselines[field_name]
        method = method or self._default_method
        raw = record.get(field_name)

        if raw is None:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                method=method.value,
                threshold=0.0,
                field_name=field_name,
                value=None,
            )

        value = float(raw)

        if method == DetectionMethod.ZSCORE:
            result = self._detect_zscore(value, baseline, field_name)
        elif method == DetectionMethod.IQR:
            result = self._detect_iqr(value, baseline, field_name)
        elif method == DetectionMethod.MODIFIED_ZSCORE:
            result = self._detect_modified_zscore(value, baseline, field_name)
        else:
            raise ValueError(f"Unknown detection method: {method}")

        self._detection_count += 1
        if result.is_anomaly:
            self._anomaly_count += 1

        return result

    def reset_baseline(self, field_name: str | None = None) -> None:
        """Clear trained baselines.

        Args:
            field_name: If provided, only reset the baseline for this field.
                If None, reset all baselines.
        """
        if field_name is not None:
            self._baselines.pop(field_name, None)
            logger.info("Reset baseline for field '%s'", field_name)
        else:
            self._baselines.clear()
            logger.info("Reset all baselines")

    @property
    def stats(self) -> dict[str, Any]:
        """Return detection statistics."""
        anomaly_rate = (
            (self._anomaly_count / self._detection_count)
            if self._detection_count > 0
            else 0.0
        )
        return {
            "fields_trained": list(self._baselines.keys()),
            "detections_run": self._detection_count,
            "anomalies_found": self._anomaly_count,
            "anomaly_rate": round(anomaly_rate, 4),
        }

    def _detect_zscore(
        self, value: float, baseline: FieldBaseline, field_name: str
    ) -> AnomalyResult:
        """Z-score based anomaly detection."""
        if baseline.std == 0:
            score = 0.0
        else:
            score = abs(value - baseline.mean) / baseline.std

        return AnomalyResult(
            is_anomaly=score > self._zscore_threshold,
            score=round(score, 6),
            method=DetectionMethod.ZSCORE.value,
            threshold=self._zscore_threshold,
            field_name=field_name,
            value=value,
        )

    def _detect_iqr(
        self, value: float, baseline: FieldBaseline, field_name: str
    ) -> AnomalyResult:
        """Interquartile range based anomaly detection."""
        if baseline.iqr == 0:
            score = 0.0
            is_anomaly = False
        else:
            lower_fence = baseline.q1 - self._iqr_multiplier * baseline.iqr
            upper_fence = baseline.q3 + self._iqr_multiplier * baseline.iqr
            is_anomaly = value < lower_fence or value > upper_fence
            # Score as distance beyond fence normalized by IQR
            if value < lower_fence:
                score = (lower_fence - value) / baseline.iqr
            elif value > upper_fence:
                score = (value - upper_fence) / baseline.iqr
            else:
                score = 0.0

        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=round(score, 6),
            method=DetectionMethod.IQR.value,
            threshold=self._iqr_multiplier,
            field_name=field_name,
            value=value,
        )

    def _detect_modified_zscore(
        self, value: float, baseline: FieldBaseline, field_name: str
    ) -> AnomalyResult:
        """Modified z-score using median absolute deviation (MAD)."""
        if baseline.mad == 0:
            score = 0.0
        else:
            # 0.6745 is the 0.75th quantile of the standard normal distribution
            score = 0.6745 * abs(value - baseline.median) / baseline.mad

        return AnomalyResult(
            is_anomaly=score > self._zscore_threshold,
            score=round(score, 6),
            method=DetectionMethod.MODIFIED_ZSCORE.value,
            threshold=self._zscore_threshold,
            field_name=field_name,
            value=value,
        )
