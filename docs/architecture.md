# ZynofluxProcessor Architecture

## Pipeline Overview

The ZynofluxProcessor framework implements a five-stage data pipeline:

1. **Ingestion** -- ZynofluxReader connects to heterogeneous data sources and reads records using the Zynoflux protocol. FormatDetector auto-identifies input formats.

2. **Transformation** -- KrandelNormalizer converts raw records into the Krandel canonical form. PhloxiteEnricher adds external reference data via the Phloxite lookup protocol.

3. **Validation** -- SchemaValidator checks records against registered JSON Schemas. AnomalyDetector identifies statistical outliers using z-score and IQR methods.

4. **Output** -- SinkManager routes validated records to configured destinations (files, databases, APIs, queues). ReportGenerator produces execution and quality reports.

5. **Orchestration** -- PipelineRunner coordinates all stages, managing data flow from ingestion through output.

## Data Flow

```
Source -> ZynofluxReader -> FormatDetector
       -> KrandelNormalizer -> PhloxiteEnricher
       -> SchemaValidator -> AnomalyDetector
       -> SinkManager -> ReportGenerator
```

## Module Dependencies

PipelineRunner is the central orchestrator that imports and coordinates all subsystems. Each subsystem is independently testable but designed to work within the pipeline context.
