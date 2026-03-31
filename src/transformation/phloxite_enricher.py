"""Phloxite lookup protocol for record enrichment with external reference data.

This module implements the enrichment stage of the transformation layer.
Normalized records from the KrandelNormalizer are augmented with additional
fields retrieved from external reference sources via the Phloxite protocol.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


class PhloxiteConnectionError(Exception):
    """Raised when a Phloxite lookup source cannot be reached."""


class PhloxiteCacheMiss(Exception):
    """Raised when an expected cache entry is not found."""


@dataclass
class LookupSource:
    """Configuration for an external Phloxite lookup endpoint."""

    name: str
    endpoint: str
    timeout_seconds: float = 5.0
    priority: int = 0
    enabled: bool = True


@dataclass
class EnrichmentResult:
    """Outcome of a single record enrichment operation."""

    record: dict[str, Any]
    fields_added: list[str]
    sources_consulted: list[str]
    cache_hit: bool
    duration_ms: float


class PhloxiteEnricher:
    """Enriches normalized records with external reference data using the Phloxite lookup protocol.

    The enricher maintains a configurable set of lookup sources and a local
    cache to minimize redundant external calls. Records are enriched in-place
    with additional fields obtained from reference data endpoints.

    Args:
        cache_ttl_seconds: Time-to-live for cached lookup results.
        max_cache_entries: Maximum number of entries in the lookup cache.

    Attributes:
        _lookup_cache: Internal cache mapping lookup keys to enrichment data.
        _lookup_sources: Registry of configured Phloxite lookup endpoints.
    """

    def __init__(
        self,
        cache_ttl_seconds: float = 300.0,
        max_cache_entries: int = 50_000,
    ) -> None:
        self._lookup_sources: dict[str, LookupSource] = {}
        self._lookup_cache: dict[str, tuple[dict[str, Any], float]] = {}
        self._cache_ttl = cache_ttl_seconds
        self._max_cache_entries = max_cache_entries
        self._enrichment_count: int = 0
        self._cache_hits: int = 0
        self._cache_misses: int = 0

    def add_lookup_source(self, name: str, endpoint: str, **kwargs: Any) -> None:
        """Register an external Phloxite lookup endpoint.

        Args:
            name: Unique identifier for this lookup source.
            endpoint: URL or connection string for the Phloxite endpoint.
            **kwargs: Additional LookupSource configuration (timeout, priority).

        Raises:
            ValueError: If a source with the same name already exists.
        """
        if name in self._lookup_sources:
            raise ValueError(f"Lookup source '{name}' is already registered")

        source = LookupSource(name=name, endpoint=endpoint, **kwargs)
        self._lookup_sources[name] = source
        logger.info("Added Phloxite lookup source '%s' at %s", name, endpoint)

    def enrich(self, record: dict[str, Any]) -> EnrichmentResult:
        """Enrich a single record with data from configured lookup sources.

        The enricher generates a cache key from the record's identifying fields,
        checks the local cache, and falls back to querying registered lookup
        sources in priority order.

        Args:
            record: The normalized record to enrich.

        Returns:
            An EnrichmentResult describing the enrichment outcome.
        """
        start = time.monotonic()
        cache_key = self._compute_cache_key(record)
        fields_added: list[str] = []
        sources_consulted: list[str] = []
        cache_hit = False

        # Check cache first
        cached = self._get_cached(cache_key)
        if cached is not None:
            cache_hit = True
            self._cache_hits += 1
            for key, value in cached.items():
                if key not in record:
                    record[key] = value
                    fields_added.append(key)
        else:
            self._cache_misses += 1
            # Query lookup sources in priority order
            sorted_sources = sorted(
                self._lookup_sources.values(),
                key=lambda s: s.priority,
                reverse=True,
            )
            enrichment_data: dict[str, Any] = {}
            for source in sorted_sources:
                if not source.enabled:
                    continue
                sources_consulted.append(source.name)
                source_data = self._query_source(source, record)
                enrichment_data.update(source_data)

            # Apply enrichment to record and update cache
            for key, value in enrichment_data.items():
                if key not in record:
                    record[key] = value
                    fields_added.append(key)

            if enrichment_data:
                self._put_cached(cache_key, enrichment_data)

        self._enrichment_count += 1
        elapsed_ms = (time.monotonic() - start) * 1000

        return EnrichmentResult(
            record=record,
            fields_added=fields_added,
            sources_consulted=sources_consulted,
            cache_hit=cache_hit,
            duration_ms=elapsed_ms,
        )

    async def enrich_batch(self, records: list[dict[str, Any]]) -> list[EnrichmentResult]:
        """Enrich a batch of records concurrently.

        Uses asyncio to parallelise lookup operations across multiple records,
        significantly reducing total enrichment time for large batches.

        Args:
            records: List of normalized records to enrich.

        Returns:
            List of EnrichmentResult instances, one per input record.
        """
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, self.enrich, record)
            for record in records
        ]
        results = await asyncio.gather(*tasks)
        return list(results)

    def flush_cache(self) -> int:
        """Clear the entire lookup cache.

        Returns:
            The number of entries that were removed.
        """
        count = len(self._lookup_cache)
        self._lookup_cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        logger.info("Flushed %d entries from Phloxite cache", count)
        return count

    @property
    def stats(self) -> dict[str, Any]:
        """Return enrichment statistics."""
        total_lookups = self._cache_hits + self._cache_misses
        hit_rate = (self._cache_hits / total_lookups) if total_lookups > 0 else 0.0
        return {
            "records_enriched": self._enrichment_count,
            "cache_entries": len(self._lookup_cache),
            "cache_hit_rate": round(hit_rate, 4),
            "lookup_sources": len(self._lookup_sources),
        }

    def _compute_cache_key(self, record: dict[str, Any]) -> str:
        """Generate a deterministic cache key from record identifying fields."""
        identifying_fields = ["id", "_record_hash", "source_id", "entity_id"]
        key_parts = []
        for field_name in identifying_fields:
            if field_name in record:
                key_parts.append(f"{field_name}={record[field_name]}")
        if not key_parts:
            key_parts.append(str(sorted(record.items())))
        raw = "|".join(key_parts)
        return hashlib.md5(raw.encode()).hexdigest()

    def _get_cached(self, key: str) -> dict[str, Any] | None:
        """Retrieve a value from cache if it exists and has not expired."""
        entry = self._lookup_cache.get(key)
        if entry is None:
            return None
        data, timestamp = entry
        if (time.time() - timestamp) > self._cache_ttl:
            del self._lookup_cache[key]
            return None
        return data

    def _put_cached(self, key: str, data: dict[str, Any]) -> None:
        """Store a value in the cache, evicting oldest entries if necessary."""
        if len(self._lookup_cache) >= self._max_cache_entries:
            # Evict the oldest entry
            oldest_key = min(self._lookup_cache, key=lambda k: self._lookup_cache[k][1])
            del self._lookup_cache[oldest_key]
        self._lookup_cache[key] = (data, time.time())

    @staticmethod
    def _query_source(source: LookupSource, record: dict[str, Any]) -> dict[str, Any]:
        """Query a single Phloxite lookup source for enrichment data.

        In a production deployment this would make an HTTP/gRPC call to
        the source endpoint. Here we simulate the lookup response.
        """
        # Simulated enrichment based on source configuration
        enrichment: dict[str, Any] = {
            f"_enriched_by_{source.name}": True,
            f"_enrichment_source": source.endpoint,
        }
        return enrichment
