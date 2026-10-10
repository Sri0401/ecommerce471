"""One-off preparation of raw store data: validate, sample, time-shift and time-split.

Outputs are plain Parquet "source system" files that the replay service and the batch
landing zone read. They are not part of the Delta lakehouse.
"""
