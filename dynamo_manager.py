import json
import sys
from decimal import Decimal
import random

import boto3
import operator as op
from boto3.dynamodb.conditions import Key, Attr

_dyn_client = boto3.resource('dynamodb')


def parse_tabledef(conf_file):
    require_keys = [
        'table_name',
        'pk',
        'pkdef',
    ]
    with open(conf_file) as fh:
        conf = json.loads(fh.read())
        if require_keys == list(conf.keys()):
            return conf
        else:
            raise KeyError('Invalid configuration.')


def create_dynamo_table(table_name, pk, pkdef):
    table = _dyn_client.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table


def get_dynamo_table(table_name):
    return _dyn_client.Table(table_name)
#    print(_dyn_client.Table(table_name))


def parse_productdef(prod_file):
    require_keys = [
        'category',
        'sku'
    ]
    with open(prod_file) as pf:
        prodf = json.loads(pf.read())
#        breakpoint()
        return prodf
        if set(require_keys).issubset(set(prodf.keys())):
            return prodf
        else:
            raise KeyError('Invalid configuration.')

            
def create_product(table_name, category, sku, **item):
    table = get_dynamo_table(table_name)
    keys = {
        'category': category,
        'sku': sku
    }
    item.update(keys)
    
    # Convert float values to decimal values
    for key in item.keys():
        if type(item[key]) == float:
            item[key] = Decimal(item[key]).quantize(Decimal('.01'))
    
#    breakpoint()
    table.put_item(Item=item)
    return table.get_item(Key=keys)['Item']


def update_product(table_name, category, sku, **item):
    table = get_dynamo_table(table_name)
    keys = {
        'category': category,
        'sku': sku
    }
    
    # Convert float values to decimal values
    for key in item.keys():
        if type(item[key]) == float:
            item[key] = Decimal(item[key]).quantize(Decimal('.01'))
#    breakpoint()
    
    expr = ', '.join([f'{k}=:{k}' for k in item.keys()])
    vals = {f':{k}': v for k,v in item.items()}
    
    table.update_item(
        Key = keys,
        UpdateExpression = f'SET {expr}',
        ExpressionAttributeValues = vals
    )
    return table.get_item(Key=keys)['Item']

# Note: n_items arg is a string, so converted to int
def create_random_items(n_items):
    items = []
    sku_types = ('foo', 'bar')
    categories = ('dress', 'shorts', 'sandals')
    status = (True, False)
    prices = (Decimal('34.75'), Decimal('49.75'), Decimal('54.75'))
#    breakpoint()
    for id in range(n_items):
        id += 1
        items.append({
            'category': random.choice(categories),
            'sku': f'{random.choice(sku_types)}-apparel-{id}',
            'product_name': f'Apparel{id}',
            'is_published': random.choice(status),
            'price': random.choice(prices),
            'in_stock': random.choice(status)
        })
        
    return items


def create_dynamo_items(table_name, n_items, keys=None):
    table = get_dynamo_table(table_name)
    items = create_random_items(n_items)
    params = {
        'overwrite_by_pkeys': keys
    } if keys else {}
    
    with table.batch_writer(**params) as batch:
        for item in items:
            batch.put_item(Item=item)
    return True


def query_products(table_name, pk_value, 
        sk_value=None, sk_condition=None,
        attr_name=None, attr_condition=None, attr_value=None):
        
    table = get_dynamo_table(table_name)
    key_expr = Key('category').eq(pk_value)
    
    if sk_value:
        key_expr = key_expr & getattr(Key('sku'), sk_condition)(sk_value)
#    breakpoint()
    params = {
        'KeyConditionExpression': key_expr
    }
    
    if all(_ is not None for _ in [attr_name, attr_condition, attr_value]):
        filter_expr = getattr(Attr(attr_name), attr_condition)(attr_value)
#    breakpoint()    
        params['FilterExpression'] = filter_expr
        
    res = table.query(**params)
    return res['Items']
#    print(res['Items'])


def scan_products(table_name,
        attr_name, attr_condition, attr_value):
        
    table = get_dynamo_table(table_name)
    filter_expr = getattr(Attr(attr_name), attr_condition)(attr_value)
    
    params = {
        'FilterExpression': filter_expr
    }
    res = table.scan(**params)
    return res['Items']
#    print(res['Items'])
    
def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='Commands',
    )

    # Create table subcommand
    sp_create_dynamo_table = subparsers.add_parser(
        'create_dynamo_table',
        help='Create a DynamoDB table',
    )
    sp_create_dynamo_table.add_argument(
        'tabledef',
        help='Table definition file (JSON)',
    )
    sp_create_dynamo_table.set_defaults(func=create_dynamo_table)
    
    # Get table subcommand
    sp_get_dynamo_table = subparsers.add_parser(
        'get_dynamo_table',
        help='Get a DynamoDB table',
    )
    sp_get_dynamo_table.add_argument(
        'table_name',
        help='Name of DynamoDB table to get.',
    )
    sp_get_dynamo_table.set_defaults(func=get_dynamo_table)
    
    # Create product subcommand
    sp_create_product = subparsers.add_parser(
        'create_product',
        help='Create a product in a DynamoDB table',
    )
    sp_create_product.add_argument(
        'table_name',
        help='DynamoDB table where to create product',
    )
    sp_create_product.add_argument(
        'productdef',
        help='Product definition file (JSON)',
    )
    sp_create_product.set_defaults(func=create_product)
    
    # Update product subcommand
    sp_update_product = subparsers.add_parser(
        'update_product',
        help='Update a product in a DynamoDB table',
    )
    sp_update_product.add_argument(
        'table_name',
        help='DynamoDB table where to update product',
    )
    sp_update_product.add_argument(
        'productdef',
        help='Product definition file (JSON)',
    )
    sp_update_product.set_defaults(func=update_product)
    
    # Create dynamo items subcommand
    sp_create_dynamo_items = subparsers.add_parser(
        'create_dynamo_items',
        help='Create multiple items in a DynamoDB table',
    )
    sp_create_dynamo_items.add_argument(
        'table_name',
        help='DynamoDB table where to create items',
    )
    sp_create_dynamo_items.add_argument(
        'n_items',
        help='Number (int) of random items to create',
    )
    sp_create_dynamo_items.set_defaults(func=create_dynamo_items)
    
    # Query products subcommand
    sp_query_products = subparsers.add_parser(
        'query_products',
        help='Query items in a DynamoDB table using key filter\
        and possibly, filter expression',
    )
    sp_query_products.add_argument(
        'table_name',
        help='DynamoDB table where to query items',
    )
    sp_query_products.add_argument(
        'pk_value',
        help='Partition key value for key filter\
        to query DynamoDB table'
    )
    sp_query_products.add_argument(
        '--sk_value',
        help='Sort key value for key filter\
        to query DynamoDB table'
    )
    sp_query_products.add_argument(
        '--sk_condition',
        help='Sort key condition for key filter\
        to query DynamoDB table\
        possible values include\
        eq, le, ge, gt, between, begins_with\
        (default: begins_with)',
        default='begins_with'
    )
    sp_query_products.add_argument(
        '--attr_name',
        help='Attribute name for filter expression\
        to query DynamoDB table\
        (Note: only string attribute types accepted so far)'
    )
    sp_query_products.add_argument(
        '--attr_condition',
        help='Attribute condition for filter expression\
        to query DynamoDB table\
        e.g. eq, le, ge, gt, between, begins_with\
        (default: begins_with)',
        default='begins_with'
    )
    sp_query_products.add_argument(
        '--attr_value',
        help='Attribute value for filter expression\
        to query DynamoDB table'
    )
    sp_query_products.set_defaults(func=query_products)
    
    # Scan products subcommand
    sp_scan_products = subparsers.add_parser(
        'scan_products',
        help='Scan items in a DynamoDB table using\
        filter expression',
    )
    sp_scan_products.add_argument(
        'table_name',
        help='DynamoDB table where to scan for items',
    )
    sp_scan_products.add_argument(
        'attr_name',
        help='Attribute name for filter expression\
        to scan DynamoDB table\
        (Note: only string attribute types accepted so far)'
    )
    sp_scan_products.add_argument(
        'attr_condition',
        help='Attribute condition for filter expression\
        to scan DynamoDB table\
        e.g. eq, le, ge, gt, between, begins_with\
        (default: begins_with)',
        default='begins_with'
    )
    sp_scan_products.add_argument(
        'attr_value',
        help='Attribute value for filter expression\
        to scan DynamoDB table'
    )
    sp_scan_products.set_defaults(func=scan_products)
    
    # Delete table subcommand
    sp_delete_dynamo_table = subparsers.add_parser(
        'delete_dynamo_table',
        help='Delete a DynamoDB table',
    )
    sp_delete_dynamo_table.add_argument(
        'table_name',
        help='Name of DynamoDB table to delete.',
    )
    sp_delete_dynamo_table.set_defaults(func=delete_dynamo_table)

    # Execute subcommand function
    args = parser.parse_args()
    action = args.func.__name__ if hasattr(args, 'func') else ''
    if action == 'delete_dynamo_table':
        args.func(args.table_name)
    elif action == 'create_dynamo_table':
        conf = parse_tabledef(args.tabledef)
        args.func(**conf)
    elif action == 'get_dynamo_table':
        args.func(args.table_name)
    elif action == 'create_product':
        prodf = parse_productdef(args.productdef)
        args.func(args.table_name, **prodf)
    elif action == 'update_product':
        prodf = parse_productdef(args.productdef)
        args.func(args.table_name, **prodf)
    elif action == 'create_dynamo_items':
        args.func(args.table_name, int(args.n_items))
    elif action == 'query_products':
        args.func(args.table_name, args.pk_value,
            args.sk_value, args.sk_condition,
            args.attr_name, args.attr_condition, args.attr_value)
    elif action == 'scan_products':
        args.func(args.table_name, 
        args.attr_name, args.attr_condition, args.attr_value)
    else:
        print('Invalid/Missing command.')
        sys.exit(1)

    print('Done')

