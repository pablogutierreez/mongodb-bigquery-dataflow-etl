"""MongoDB → BigQuery ETL — Apache Beam / Google Cloud Dataflow pipeline.

A reusable template that syncs one or more MongoDB collections into BigQuery.

How it works
------------
1. Reads every document from each configured MongoDB collection (in parallel).
2. Normalises BSON types into BigQuery-friendly JSON (see transformations.py).
3. Writes each collection to its own BigQuery table with a full
   truncate-and-reload (WRITE_TRUNCATE), so the table always mirrors Mongo.
4. Documents that fail to transform are routed to a Dead Letter Queue
   (Pub/Sub topic) instead of crashing the job.
5. A single success/failure metric is emitted to Cloud Monitoring at the end.

The pipeline is packaged as a Dataflow Flex Template and is normally triggered
on a schedule (Cloud Scheduler → Cloud Workflows → Dataflow), but it can also
be launched manually with `gcloud dataflow flex-template run`.

All project-specific values are passed as parameters — nothing is hard-coded.
"""
import argparse
import json
import logging
import time

import apache_beam as beam
import certifi
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.cloud import pubsub_v1, secretmanager
from pymongo import MongoClient
from pymongo.read_preferences import Secondary

from transformations import normalise_document
from metrics import emit_job_metrics


# BigQuery schema

# Every collection is stored with the same generic schema:
#   id        — the Mongo _id as a string (primary key)
#   json_data — the full document as a native BigQuery JSON column
#
# Storing the document as a JSON column means you don't need to maintain a
# rigid schema per collection: you can query nested fields directly in SQL
# with json_data.field.subfield. Adapt this if you prefer a flat schema.
BQ_SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'json_data', 'type': 'JSON', 'mode': 'NULLABLE'},
    ]
}



# Helpers
def _resolve_secret(value: str) -> str:
    """Resolve a Secret Manager resource path to its payload.

    If *value* looks like `projects/PROJECT/secrets/NAME/versions/VERSION`
    the secret payload is fetched and returned. Otherwise *value* is returned
    unchanged, so you can also pass a raw connection string directly.
    """
    if value.startswith('projects/') and '/secrets/' in value:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=value)
        return response.payload.data.decode('utf-8').strip()
    return value


def _table_ref(bq_project: str, dataset: str, collection: str, suffix: str = '') -> str:
    """Build a fully-qualified BigQuery table reference."""
    return f'{bq_project}:{dataset}.{collection}{suffix}'



# Beam DoFns
class ReadFromMongoDB(beam.DoFn):
    """Read every document from a MongoDB collection, one doc per element."""

    def __init__(self, mongo_uri, mongo_db, collection, prefer_secondary=True):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.collection = collection
        self.prefer_secondary = prefer_secondary

    def process(self, _ignored):
        # Reading from a secondary keeps load off the primary node. For a
        # replica set with a dedicated analytics node you can tag it here,
        # e.g. Secondary(tag_sets=[{'nodeType': 'ANALYTICS'}]).
        read_pref = Secondary() if self.prefer_secondary else None
        client = MongoClient(
            self.mongo_uri,
            tlsCAFile=certifi.where(),
            read_preference=read_pref,
            appname='MongoDB to BigQuery ETL',
        )
        try:
            client.admin.command('ping')
            logging.info('Connected to MongoDB (collection=%s)', self.collection)
            db = client[self.mongo_db]
            for doc in db[self.collection].find():
                yield doc
        finally:
            client.close()


class TransformDocument(beam.DoFn):
    """Normalise a raw pymongo document into a BQ row, or route it to the DLQ."""

    DLQ_TAG = 'dlq'

    def __init__(self, collection, dlq_topic):
        self.collection = collection
        self.dlq_topic = dlq_topic

    def setup(self):
        self._publisher = pubsub_v1.PublisherClient() if self.dlq_topic else None

    def process(self, doc):
        result = normalise_document(doc, self.collection)
        if result['ok']:
            yield result['row']
            return

        # Transformation failed — publish to the dead-letter topic if set,
        # otherwise just log it. Either way, emit a tagged output so callers
        # can count / inspect failures without stopping the pipeline.
        if self._publisher and self.dlq_topic:
            try:
                self._publisher.publish(
                    self.dlq_topic,
                    result['dead_letter'].encode('utf-8'),
                ).result(timeout=30)
            except Exception as e:  # noqa: BLE001 - never let DLQ failures crash the job
                logging.error('Failed to publish to DLQ: %s', e)
        else:
            logging.warning('Dead-letter (%s): %s', self.collection, result['dead_letter'])
        yield beam.pvalue.TaggedOutput(self.DLQ_TAG, result['dead_letter'])


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
def run(argv=None):
    parser = argparse.ArgumentParser(description='MongoDB → BigQuery Dataflow ETL')
    parser.add_argument('--mongo_uri', required=True,
                        help='MongoDB connection string, or a Secret Manager '
                             'path (projects/PROJECT/secrets/NAME/versions/VERSION).')
    parser.add_argument('--mongo_db', required=True,
                        help='MongoDB database name.')
    parser.add_argument('--collections', required=True,
                        help='Comma-separated list of collection names to sync.')
    parser.add_argument('--bq_project', required=True,
                        help='GCP project that owns the BigQuery dataset.')
    parser.add_argument('--bq_dataset', required=True,
                        help='BigQuery dataset to write the tables into.')
    parser.add_argument('--dlq_topic', default='',
                        help='Full Pub/Sub topic path for dead-letter messages '
                             '(projects/PROJECT/topics/NAME). Empty = log only.')
    parser.add_argument('--table_suffix', default='',
                        help='Optional suffix appended to every table name '
                             '(handy for test runs, e.g. "_test").')
    parser.add_argument('--no_secondary_read', action='store_true',
                        help='Read from the primary instead of a secondary node.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    mongo_uri = _resolve_secret(known_args.mongo_uri)
    collections = [c.strip() for c in known_args.collections.split(',') if c.strip()]
    dlq_topic = known_args.dlq_topic or None

    options = PipelineOptions(pipeline_args)
    # save_main_session lets workers unpickle objects that reference globals
    # defined in this module.
    options.view_as(SetupOptions).save_main_session = True
    # setup_file ensures local modules (transformations, metrics) ship to workers.
    options.view_as(SetupOptions).setup_file = './setup.py'

    start = time.time()
    success = True

    try:
        with beam.Pipeline(options=options) as p:
            for collection in collections:
                seed = p | f'Seed_{collection}' >> beam.Create([None])

                raw_docs = seed | f'ReadMongo_{collection}' >> beam.ParDo(
                    ReadFromMongoDB(
                        mongo_uri,
                        known_args.mongo_db,
                        collection,
                        prefer_secondary=not known_args.no_secondary_read,
                    )
                )

                results = raw_docs | f'Transform_{collection}' >> beam.ParDo(
                    TransformDocument(collection, dlq_topic)
                ).with_outputs(TransformDocument.DLQ_TAG, main='rows')

                dest_table = _table_ref(
                    known_args.bq_project,
                    known_args.bq_dataset,
                    collection,
                    known_args.table_suffix,
                )

                (
                    results['rows']
                    | f'WriteBQ_{collection}' >> WriteToBigQuery(
                        table=dest_table,
                        schema=BQ_SCHEMA,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        method='FILE_LOADS',
                    )
                )
    except Exception:
        success = False
        raise
    finally:
        duration = time.time() - start
        logging.info(
            '--- Sync finished (%d collections → %s.%s) in %.1fs ---',
            len(collections), known_args.bq_project,
            known_args.bq_dataset, duration,
        )
        try:
            emit_job_metrics(known_args.bq_project, success=success)
        except Exception as e:  # noqa: BLE001
            logging.warning('Failed to emit metrics: %s', e)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )
    run()
