"""Post-run validation — compare row counts between MongoDB and BigQuery.

Run this after the pipeline to confirm every collection has the same number
of documents in MongoDB and rows in BigQuery. Exits with code 1 on any
mismatch, so it can be wired into CI or an alerting step.

Usage:
    python validate.py \
        --mongo_uri="mongodb+srv://..." \
        --mongo_db=mydb \
        --bq_project=my-gcp-project \
        --bq_dataset=mongo_data \
        --collections=users,orders,products
"""
import argparse
import logging
import sys

import certifi
from pymongo import MongoClient
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def get_mongo_counts(uri, db_name, collections):
    client = MongoClient(uri, tlsCAFile=certifi.where())
    try:
        db = client[db_name]
        return {coll: db[coll].count_documents({}) for coll in collections}
    finally:
        client.close()


def get_bq_counts(project, dataset, collections, table_suffix=''):
    client = bigquery.Client(project=project)
    counts = {}
    for coll in collections:
        table = f'{project}.{dataset}.{coll}{table_suffix}'
        try:
            counts[coll] = client.get_table(table).num_rows
        except NotFound:
            counts[coll] = None
    return counts


def main():
    parser = argparse.ArgumentParser(description='Validate MongoDB ↔ BigQuery row counts')
    parser.add_argument('--mongo_uri', required=True)
    parser.add_argument('--mongo_db', required=True)
    parser.add_argument('--bq_project', required=True)
    parser.add_argument('--bq_dataset', required=True)
    parser.add_argument('--collections', required=True,
                        help='Comma-separated list of collection names.')
    parser.add_argument('--table_suffix', default='')
    args = parser.parse_args()

    collections = [c.strip() for c in args.collections.split(',') if c.strip()]

    logging.info('Counting documents in MongoDB…')
    mongo_counts = get_mongo_counts(args.mongo_uri, args.mongo_db, collections)

    logging.info('Counting rows in BigQuery…')
    bq_counts = get_bq_counts(args.bq_project, args.bq_dataset, collections, args.table_suffix)

    mismatches = 0
    print(f'\n{"collection":<24}{"mongo":>12}{"bigquery":>12}   status')
    print('-' * 64)
    for coll in collections:
        m = mongo_counts.get(coll)
        b = bq_counts.get(coll)
        if b is None:
            status = 'MISSING TABLE'
            mismatches += 1
        elif m == b:
            status = 'OK'
        else:
            status = 'MISMATCH'
            mismatches += 1
        b_display = '—' if b is None else b
        print(f'{coll:<24}{m:>12}{b_display:>12}   {status}')

    print('-' * 64)
    if mismatches:
        print(f'\n{mismatches} mismatch(es) found.')
        sys.exit(1)
    print('\nAll collections match. ✅')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main()
