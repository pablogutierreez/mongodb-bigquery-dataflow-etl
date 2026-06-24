"""BSON → BigQuery JSON normalisation.

MongoDB documents contain BSON-specific types (ObjectId, Decimal128, Binary,
etc.) that aren't valid JSON. This module recursively converts a raw pymongo
document into a structure that BigQuery's JSON type can store and query.

The chosen representations are deliberately explicit so the original type is
recoverable from SQL, e.g. ObjectId becomes {"oid": "..."} rather than a bare
string that could collide with a real string field.
"""
import base64
import json
import logging
import math
from datetime import datetime

from bson import ObjectId, Regex, Decimal128, Binary, Int64, DBRef, Code


def _convert_bson_types(obj):
    """Recursively convert BSON/pymongo types to JSON-compatible values.

    ObjectId   → {"oid": "hex"}
    datetime   → {"date": "ISO8601Z"}
    Regex      → {"regex": "pattern", "options": "flags"}
    Decimal128 → string
    Binary     → base64 string
    Int64      → int
    DBRef      → {"ref": "collection", "id": ...}
    Code       → string
    NaN / Inf  → None (BigQuery JSON cannot represent them)
    bytes      → base64 string
    """
    if isinstance(obj, dict):
        return {k: _convert_bson_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_bson_types(item) for item in obj]
    if isinstance(obj, ObjectId):
        return {'oid': str(obj)}
    if isinstance(obj, datetime):
        if obj.microsecond:
            ms = obj.microsecond // 1000
            return {'date': obj.strftime('%Y-%m-%dT%H:%M:%S') + f'.{ms:03d}Z'}
        return {'date': obj.strftime('%Y-%m-%dT%H:%M:%SZ')}
    if isinstance(obj, Regex):
        flag_map = {1: 'i', 2: 'l', 4: 'm', 8: 's', 16: 'u', 32: 'x'}
        options = ''.join(v for k, v in flag_map.items() if obj.flags & k)
        return {'regex': obj.pattern, 'options': options}
    if isinstance(obj, Decimal128):
        return str(obj)
    if isinstance(obj, Binary):
        return base64.b64encode(obj).decode('ascii')
    if isinstance(obj, Int64):
        return int(obj)
    if isinstance(obj, DBRef):
        return {'ref': obj.collection, 'id': _convert_bson_types(obj.id)}
    if isinstance(obj, Code):
        return str(obj)
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('ascii')
    return obj


def normalise_document(doc, collection_name='unknown'):
    """Transform a raw pymongo document into {id, json_data} for BigQuery.

    Returns one of:
        {'ok': True,  'row': {'id': str, 'json_data': dict}}
        {'ok': False, 'dead_letter': '<json error payload>'}

    A document with no usable _id, or one that raises during conversion, is
    turned into a dead-letter payload instead of stopping the pipeline.
    """
    try:
        raw_id = doc.get('_id')
        if isinstance(raw_id, ObjectId):
            record_id = str(raw_id)
        elif isinstance(raw_id, dict):
            record_id = raw_id.get('$oid') or raw_id.get('oid')
        else:
            record_id = str(raw_id) if raw_id else None

        if not record_id:
            raise ValueError('Document has no valid _id')

        return {
            'ok': True,
            'row': {
                'id': str(record_id),
                'json_data': _convert_bson_types(doc),
            },
        }
    except Exception as e:  # noqa: BLE001 - failures become dead letters
        logging.error('Error processing document in %s: %s', collection_name, e)
        return {
            'ok': False,
            'dead_letter': json.dumps({
                'collection': collection_name,
                'error': str(e),
                'document': json.dumps(doc, default=str),
            }),
        }
