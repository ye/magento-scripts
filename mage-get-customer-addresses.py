#!/usr/bin/env python
import argparse
import json
from mysql import dict_cursor

def get_attributes(cur, entity_type=1, fields=None):
    sql ='''
SELECT attribute_id, attribute_code, backend_type
FROM eav_attribute
WHERE entity_type_id = %s 
AND attribute_code IN ('%s')
'''
    cur = dict_cursor()
    real_sql = sql % (entity_type, "','".join(fields))
    print real_sql
    cur.execute( real_sql )
    results = cur.fetchall()
    attributes = {}
    mappings = {}
    for row in results:
        attr_type = attributes.get(row['backend_type'], {}) # handle init value
        attr_type[row['attribute_id']] = row['attribute_code']
        attributes[row['backend_type']] = attr_type
        mappings[row['attribute_id']] = row['attribute_code']
    return attributes, mappings

def get_customer_address_entities(cur, customer_ids):
    '''
    Query the customer_address_entity table to get a list of address entities.
    '''
    cur = dict_cursor()
    sql = '''
SELECT cae.entity_id, ce.sap_account_number
FROM customer_entity ce
LEFT JOIN customer_address_entity cae ON (cae.parent_id = ce.entity_id)
WHERE ce.sap_account_number IN (%s)
    '''
    real_sql = sql % ','.join(customer_ids)
    print real_sql 
    cur.execute( real_sql ) 
    results = cur.fetchall()
    return [(row['sap_account_number'],row['entity_id']) for row in results]

def get_attributes_by_entity_ids(cur, attributes, entity_ids, mappings):
    attribute_values = []
    join_ids = lambda ids: ','.join([str(i) for i in ids])
    sql = '''
SELECT attr.entity_id, attr.attribute_id, attr.value FROM customer_address_entity_%s attr
WHERE attr.attribute_id IN (%s) AND attr.entity_id in (%s)
'''
    for attr_type in attributes:
        real_sql = sql % (attr_type, 
            join_ids(attributes[attr_type].keys()),
            join_ids(entity_ids)
        )
        print real_sql
        cur.execute( real_sql )
        attribute_values += cur.fetchall()

    attributes = {}
    for attr in attribute_values:
        eid = attr.pop('entity_id')
        attributes[eid] = attributes.get(eid, [])
        aid = attr.pop('attribute_id')
        val = attr.pop('value')
        attributes[eid].append({mappings[aid]: val})
    return attributes


def get_addresses(cur, customer_ids):
    address_entity_mappings = get_customer_address_entities(cur, customer_ids)
    entity_ids = [m[1] for m in address_entity_mappings]
    attributes, mappings = get_attributes(cur, entity_type=2, 
        fields=('firstname', 'lastname', 'street', 'city', 'region', 'postcode')
    )
    attr_values = get_attributes_by_entity_ids(cur, attributes, entity_ids, mappings)
    return attr_values

if '__main__' == __name__:
    out = None
    parser = argparse.ArgumentParser()
    parser.add_argument('--json', '-j', help='Output in JSON format')
    parser.add_argument('customer_ids', help='List of Customer Account Numbers')

    args = parser.parse_args()
    cur = dict_cursor()
    addresses = get_addresses(cur, args.customer_ids.split(','))
    if args.json:
        out = json.dumps(addresses)
    else:
        out = addresses

    print out
