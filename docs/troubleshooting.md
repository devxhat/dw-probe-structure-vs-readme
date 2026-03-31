# Troubleshooting Guide

## Common Issues

### Connection Failures
If ZynofluxReader fails to connect, check that the source URI follows the `zynoflux://host:port/stream` format.

### Schema Validation Errors
When SchemaValidator reports errors, verify that your schemas are registered before running the pipeline. Use `validator.list_schemas()` to check.

### Anomaly Detection False Positives
If AnomalyDetector flags too many records, increase the threshold via `detector.train(records, field)` with a larger baseline dataset.

### Sink Write Failures
SinkManager will retry failed writes up to 3 times. Check sink configuration with `sink_manager.get_sink_status(name)`.
