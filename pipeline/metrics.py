"""Cloud Monitoring custom metric for the MongoDB → BigQuery ETL.

A single job-level metric is emitted once at the end of each run:

  custom.googleapis.com/mongo_bq_etl/job_success   (INT64 GAUGE, 1=ok 0=failed)

The metric descriptor is auto-created by Cloud Monitoring on first write.
Build an alert policy on this metric (value < 1) to be notified of failures.

Emitting metrics must never crash the pipeline, so all errors are swallowed
and logged as warnings.
"""
import logging
import time

from google.cloud import monitoring_v3

METRIC_PREFIX = 'custom.googleapis.com/mongo_bq_etl'


def _make_series(project_id, metric_suffix, value_key, value, labels):
    """Build a GAUGE TimeSeries with a single point at the current time."""
    now = time.time()
    series = monitoring_v3.TimeSeries()
    series.metric.type = f'{METRIC_PREFIX}/{metric_suffix}'
    for k, v in labels.items():
        series.metric.labels[k] = v
    series.resource.type = 'global'
    series.resource.labels['project_id'] = project_id
    series.points.append(
        monitoring_v3.Point({
            'interval': {'end_time': {'seconds': int(now), 'nanos': 0}},
            'value': {value_key: value},
        })
    )
    return series


def emit_job_metrics(project_id, success):
    """Write the job success metric (1 = success, 0 = failure)."""
    try:
        client = monitoring_v3.MetricServiceClient()
        client.create_time_series(
            name=f'projects/{project_id}',
            time_series=[
                _make_series(
                    project_id, 'job_success', 'int64_value',
                    1 if success else 0, {},
                ),
            ],
        )
    except Exception as e:  # noqa: BLE001
        logging.warning('Failed to emit job success metric: %s', e)
