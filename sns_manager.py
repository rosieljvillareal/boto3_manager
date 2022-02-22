import boto3
import sys

_sns_client = boto3.client('sns')


def create_sns_topic(topic_name):
    _sns_client.create_topic(Name=topic_name)
    return True

    
def list_sns_topics(next_token=None):
    params = {'NextToken': next_token} if next_token else {}
    topics = _sns_client.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)
#    print(topics.get('Topics', []), topics.get('NextToken', None))


def list_sns_subscriptions(next_token=None):
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = _sns_client.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []), \
        subscriptions.get('NextToken', None)
#    print(subscriptions.get('Subscriptions', []), \
#        subscriptions.get('NextToken', None))


def subscribe_sns_topic(topic_arn, mobile_number):
    params = {
        'TopicArn': topic_arn,
        'Protocol': 'sms',
        'Endpoint': mobile_number
    }
    res = _sns_client.subscribe(**params)
    print(res)
    return True


def send_sns_message(topic_arn, message):
    params = {
        'TopicArn': topic_arn,
        'Message': message
    }
    res = _sns_client.publish(**params)
    print(res)
    return True
    
    
def unsubscribe_sns_topic(subscription_arn):
    params = {
        'SubscriptionArn': subscription_arn
    }
    res = _sns_client.unsubscribe(**params)
    print(res)
    return True


def delete_sns_topic(topic_arn):
    _sns_client.delete_topic(TopicArn=topic_arn)
    return True

            
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    
    subparsers = parser.add_subparsers(
        title='Commands',
    )
    
    # Create SNS topic subcommand
    sp_create_sns_topic = subparsers.add_parser(
        'create_sns_topic',
        help='Create an SNS topic'
    )
    
    sp_create_sns_topic.add_argument(
        'topic_name',
        help='Name of SNS topic to create'
    )
    
    sp_create_sns_topic.set_defaults(func=create_sns_topic)
    
    # List SNS topics subcommand
    sp_list_sns_topics = subparsers.add_parser(
        'list_sns_topics',
        help='List SNS topics'
    )
    
    sp_list_sns_topics.set_defaults(func=list_sns_topics)
    
    # List SNS subscriptions subcommand
    sp_list_sns_subscriptions = subparsers.add_parser(
        'list_sns_subscriptions',
        help='List SNS subscriptions'
    )
    
    sp_list_sns_subscriptions.set_defaults(func=list_sns_subscriptions)
    
    # Subscribe to SNS topic subcommand
    sp_subscribe_sns_topic = subparsers.add_parser(
        'subscribe_sns_topic',
        help='Subscribe to an SNS topic using SMS'
    )
    
    sp_subscribe_sns_topic.add_argument(
        'topic_arn',
        help='ARN of SNS topic to subscribe to'
    )
    
    sp_subscribe_sns_topic.add_argument(
        'mobile_number',
        help='Mobile number to subscribe to SNS topic'
    )
    
    sp_subscribe_sns_topic.set_defaults(func=subscribe_sns_topic)
    
    # Send SNS message subcommand
    sp_send_sns_message = subparsers.add_parser(
        'send_sns_message',
        help='Publish message to SNS topic'
    )
    
    sp_send_sns_message.add_argument(
        'topic_arn',
        help='ARN of SNS topic where to publish message'
    )
    
    sp_send_sns_message.add_argument(
        'message',
        help='Message to publish to SNS topic'
    )
    
    sp_send_sns_message.set_defaults(func=send_sns_message)
    
    # Unsubscribe to SNS topic subcommand
    sp_unsubscribe_sns_topic = subparsers.add_parser(
        'unsubscribe_sns_topic',
        help='Unsubscribe to an SNS topic'
    )
    
    sp_unsubscribe_sns_topic.add_argument(
        'subscription_arn',
        help='ARN of SNS subscription to delete'
    )
    
    sp_unsubscribe_sns_topic.set_defaults(func=unsubscribe_sns_topic)
    
    # Delete SNS topic subcommand
    sp_delete_sns_topic = subparsers.add_parser(
        'delete_sns_topic',
        help='Delete an SNS topic'
    )
    
    sp_delete_sns_topic.add_argument(
        'topic_arn',
        help='ARN of SNS topic to delete'
    )
    
    sp_delete_sns_topic.set_defaults(func=delete_sns_topic)
    
    args = parser.parse_args()
    action = args.func.__name__ if hasattr(args, 'func') else ''
    
    if action == 'create_sns_topic':
        args.func(args.topic_name)
    elif action == 'list_sns_topics':
        args.func()
    elif action == 'list_sns_subscriptions':
        args.func()
    elif action == 'subscribe_sns_topic':
        args.func(args.topic_arn, args.mobile_number)
    elif action == 'unsubscribe_sns_topic':
        args.func(args.subscription_arn)
    elif action == 'send_sns_message':
        args.func(args.topic_arn, args.message)
    elif action == 'delete_sns_topic':
        args.func(args.topic_arn)
    else:
        print('Invalid/Missing command.')
        sys.exit(1)
        
    print('Done')
