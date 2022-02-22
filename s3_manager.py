import boto3
from botocore.exceptions import ClientError

import sys
import logging
import uuid
from pathlib import Path, PosixPath

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s'
)

log = logging.getLogger()

_s3_client = boto3.resource('s3')

def create_bucket(name, region=None):
    region = region or 'ap-southeast-1'
    client = boto3.resource('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region
        }
    }
    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False


def list_buckets():
    count = 0
    for bucket in _s3_client.buckets.all():
        print(bucket.name)
        count += 1
    print(f'Found {count} buckets!')
    
    
def get_bucket(name, create=False, region=None):
    bucket = _s3_client.Bucket(name=name)
    if bucket.creation_date:
        return bucket
#        print(bucket.creation_date)
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return


def create_tempfile(file_name=None, content=None, size=300):
    """Create a temporary text file"""
    filename = f'{file_name or uuid.uuid4().hex}.txt'
    with open(filename, 'w') as f:
        f.write(f'{(content or "0") * size}')
    return filename 


def create_bucket_object(bucket_name, file_path, key_prefix=None):
    """Create a bucket object
    
    :params bucket_name: The target bucket
    :params type: str
    
    :params file_path: The path to the file to be uploaded to the bucket
    :params type: str
    
    :params key_prefix: Optional prefix to set in the bucket for the file
    :params type: str
    """
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object


def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    """Download a bucket object
    
    :params bucket_name: The target bucket
    :params type: str
    
    :params object_key: The bucket object to get
    :params type: str
    
    :params dest: Optional location where the downloaded
    file will be stored in your local.
    :params type: str
    
    :returns: The bucket object and downloaded file path object.
    :rtype: tuple
    """
    bucket = get_bucket(bucket_name)
    params = {'key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path


def enable_bucket_versioning(bucket_name):
    """Enable bucket versioning for the given bucket_name
    """
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status


def delete_bucket_objects(bucket_name, key_prefix=None):
    """Delete all bucket objects including all versions of versioned objects.
    """
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()
        
    targets = []
    for obj in objects:
        targets.append({
            'Key': obj.object_key,
            'VersionId': obj.version_id
        })
        
    bucket.delete_objects(Delete={
        'Objects': targets,
        'Quiet': True
    })
    
    return len(targets)
    
    
def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource('s3')
        for bucket in client.buckets.iterator():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')
                
    return count
    
    
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    
    subparsers = parser.add_subparsers(
        title='Commands',
    )
    
    # Create bucket subcommand
    sp_create_bucket = subparsers.add_parser(
        'create_bucket',
        help='Create an S3 bucket'
    )
    
    sp_create_bucket.add_argument(
        'name',
        help='Name of bucket to create'
    )
    
    sp_create_bucket.add_argument(
        '--region',
        help='Name of region where to create bucket\
        (default: ap-southeast-1)',
        default = 'ap-southeast-1'
    )
    
    sp_create_bucket.set_defaults(func=create_bucket)
    
    # List buckets subcommand
    sp_list_buckets = subparsers.add_parser(
        'list_buckets',
        help='List S3 buckets'
    )
    
    sp_list_buckets.set_defaults(func=list_buckets)
    
    
    # Get bucket subcommand
    sp_get_bucket = subparsers.add_parser(
        'get_bucket',
        help='Get S3 bucket'
    )
    
    sp_get_bucket.add_argument(
        'name',
        help='Name of bucket to get'
    )
    
    sp_get_bucket.add_argument(
        '--create',
        help='Flag to create bucket',
        action='store_true',
        default=False
    )
    
    sp_get_bucket.add_argument(
        '--region',
        help='Name of region where to create bucket\
        (default: ap-southeast-1)',
        default = 'ap-southeast-1'
    )
    
    sp_get_bucket.set_defaults(func=get_bucket)
    
    # Create tempfile subcommand
    sp_create_tempfile = subparsers.add_parser(
        'create_tempfile',
        help='Create temporary file'
    )
    
    sp_create_tempfile.add_argument(
        '--file_name', '-F',
        help='Name of temp file to create'
    )
    
    sp_create_tempfile.add_argument(
        '--content', '-C',
        help='Content to add to temp file'
    )
    
    sp_create_tempfile.set_defaults(func=create_tempfile)
    
    # Create bucket object subcommand
    sp_create_bucket_object = subparsers.add_parser(
        'create_bucket_object',
        help='Create bucket object'
    )
    
    sp_create_bucket_object.add_argument(
        'bucket_name',
        help='Name of bucket where to create object'
    )
    
    sp_create_bucket_object.add_argument(
        'file_path',
        help='The path to the file to be uploaded to the bucket'
    )
    
    sp_create_bucket_object.add_argument(
        '--key_prefix',
        help='Optional prefix to set in the bucket for the file'
    )
    
    sp_create_bucket_object.set_defaults(func=create_bucket_object)
    
    # Get bucket object subcommand
    sp_get_bucket_object = subparsers.add_parser(
        'get_bucket_object',
        help='Get bucket object'
    )
    
    sp_get_bucket_object.add_argument(
        'bucket_name',
        help='Name of bucket'
    )
    
    sp_get_bucket_object.add_argument(
        'object_key',
        help='The bucket object to get'
    )
    
    sp_get_bucket_object.add_argument(
        '--dest',
        help='Optional location where the downloaded file will be stored in your local'
    )
    
    sp_get_bucket_object.set_defaults(func=get_bucket_object)
    
    # Enable bucket versioning subcommand
    sp_enable_bucket_versioning = subparsers.add_parser(
        'enable_bucket_versioning',
        help='Enable bucket versioning'
    )
    
    sp_enable_bucket_versioning.add_argument(
        'bucket_name',
        help='Name of bucket'
    )
    
    sp_enable_bucket_versioning.set_defaults(func=enable_bucket_versioning)
    
    # Delete bucket objects
    sp_delete_bucket_objects = subparsers.add_parser(
        'delete_bucket_objects',
        help='Delete bucket objects'
    )
    
    sp_delete_bucket_objects.add_argument(
        'bucket_name',
        help='Name of bucket'
    )
    
    sp_delete_bucket_objects.add_argument(
        '--key_prefix',
        help='Optional prefix to set in the bucket for the file'
    )
    
    sp_delete_bucket_objects.set_defaults(func=delete_bucket_objects)
    
     # Delete bucket
    sp_delete_buckets = subparsers.add_parser(
        'delete_buckets',
        help='Delete buckets'
    )
    
    sp_delete_buckets.add_argument(
        'bucket_name',
        help='Name of bucket to delete'
    )
    
    sp_delete_buckets.set_defaults(func=delete_buckets)
    
    args = parser.parse_args()
    action = args.func.__name__ if hasattr(args, 'func') else ''
    
    if action == 'create_bucket':
        args.func(args.name, args.region)
    elif action == 'list_buckets':
        args.func()
    elif action == 'get_bucket':
        args.func(args.name, args.create, args.region)
    elif action == 'create_tempfile':
        args.func(args.file_name, args.content)
    elif action == 'create_bucket_object':
        args.func(args.bucket_name, args.file_path, args.key_prefix)
    elif action == 'get_bucket_object':
        args.func(args.bucket_name, args.object_key, args.dest)
    elif action == 'enable_bucket_versioning':
        args.func(args.bucket_name)
    elif action == 'delete_bucket_objects':
        args.func(args.bucket_name, args.key_prefix)
    elif action == 'delete_buckets':
        args.func(args.bucket_name)
    else:
        print('Invalid/Missing command.')
        sys.exit(1)
    
    print('Done')

