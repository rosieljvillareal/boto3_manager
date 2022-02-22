import boto3
import sys
from datetime import datetime, timezone
import json

def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupNamePrefix': group_name
    } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']
#    print(res['logGroups'])
    
    
def list_log_group_streams(group_name=None, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name
    } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']
#    print(res['logStreams'])
  
    
def filter_log_events(
        group_name, filter_pat,
        region_name=None,
        start=None, stop=None):
    
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat
    }
    
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']
#    print(res['events'])

   
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    
    subparsers = parser.add_subparsers(
        title='Commands',
    )
    
    # List log groups subcommand
    sp_list_log_groups = subparsers.add_parser(
        'list_log_groups',
        help='List log groups'
    )
    
    sp_list_log_groups.add_argument(
        '--group_name',
        help='Name of log group'
    )
    
    sp_list_log_groups.add_argument(
        '--region_name',
        help='Name of region where to list log groups\
        (default: ap-southeast-1)',
        default = 'ap-southeast-1'
    )
    
    sp_list_log_groups.set_defaults(func=list_log_groups)
    
    # List log group streams subcommand
    sp_list_log_group_streams = subparsers.add_parser(
        'list_log_group_streams',
        help='List log group streams'
    )
    
    sp_list_log_group_streams.add_argument(
        'group_name',
        help='Name of log group'
    )
    
    sp_list_log_group_streams.add_argument(
        '--stream_name',
        help='Name of log group stream'
    )
    
    sp_list_log_group_streams.add_argument(
        '--region_name',
        help='Name of region where to list log group streams\
        (default: ap-southeast-1)',
        default = 'ap-southeast-1'
    )
    
    sp_list_log_group_streams.set_defaults(func=list_log_group_streams)
    
    # Filter log events subcommand
    sp_filter_log_events = subparsers.add_parser(
        'filter_log_events',
        help='Filter log events using group name and filter pattern'
    )
    
    sp_filter_log_events.add_argument(
        'group_name',
        help='Name of log group'
    )
    
    sp_filter_log_events.add_argument(
        'filter_pat',
        help='Pattern to use to filter log events'
    )
    
    sp_filter_log_events.add_argument(
        '--start',
        help='Start time to filter log events'
    )
    
    sp_filter_log_events.add_argument(
        '--stop',
        help='End time to filter log events'
    )
    
    sp_filter_log_events.add_argument(
        '--region_name',
        help='Name of region where to filter log events\
        (default: ap-southeast-1)',
        default = 'ap-southeast-1'
    )
    
    sp_filter_log_events.set_defaults(func=filter_log_events)
    
    args = parser.parse_args()
    action = args.func.__name__ if hasattr(args, 'func') else ''
    
    if action == 'list_log_groups':
        args.func(args.group_name, args.region_name)
    elif action == 'list_log_group_streams':
        args.func(args.group_name, args.stream_name, args.region_name)
    elif action == 'filter_log_events':
        args.func(args.group_name, args.filter_pat,
            args.region_name,
            args.start, args.stop)
    else:
        print('Invalid/Missing command.')
        sys.exit(1)
        
    print('Done')
       
