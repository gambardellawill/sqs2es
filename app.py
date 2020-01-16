import argparse
import boto3
import requests
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser(description='Redirects messages from SQS into an ElasticSearch index.')

parser.add_argument('-s', '--source', dest='source', type=str, required=True, help='The name of the SQS queue to be saved.')

parser.add_argument('-d', '--destination', dest='destination', type=str, required=True, help='The name of the SQS queue to be saved.')

parser.add_argument('-c', '--count', dest='message_count', type=str, required=False, help='The number of messages to be retrieved.')

parser.add_argument('-e', '--es_host', dest='elasticsearch_host', type=str, required=True, help='The number of messages to be retrieved.')

sqs = boto3.resource('sqs')

if __name__ == "__main__":
    args = parser.parse_args()

    es_connect = Elasticsearch([args.elasticsearch_host])
   
    try:
        source_queue = sqs.get_queue_by_name(QueueName=args.source)
    except:
        print("\033[0;31m{} : No such queue!\033[0m".format(args.source))
    
    print("\nSource queue found: {}\n".format(source_queue.url))

    try:
        destination_queue = sqs.get_queue_by_name(QueueName=args.destination)
    except:
        print("\033[0;31m{} : No such queue!\033[0m".format(args.destination))
    
    print("Destination queue found: {}\n".format(destination_queue.url))

    if args.message_count is None:
        messages_to_transfer = input("How many messages do you wish to transfer? ")
    else:
        messages_to_transfer = args.message_count

    queue_message_count = source_queue.attributes['ApproximateNumberOfMessages']

    if queue_message_count < messages_to_transfer:
        messages_to_transfer = queue_message_count
    
    remaining_messages = int(messages_to_transfer)

    while remaining_messages > 0 :
        if remaining_messages > 10: # Maximum batch size
            transfer_count = 10
        else:
            transfer_count = remaining_messages
        
        print("Transferring {} of {} messages...\n".format(transfer_count, remaining_messages))

        response = source_queue.receive_messages(MaxNumberOfMessages=transfer_count)
        destination_messages = list()

        for source_message in response:
            destination_messages.append({'Id': source_message.message_id, 'MessageBody': source_message.body})

        confirmation = destination_queue.send_messages(Entries=destination_messages)

        for source_message in response:
            es_connect.create(index=args.source.lower(),body=source_message.body,doc_type="message", id=source_message.message_id)
            source_message.delete()

        remaining_messages -= transfer_count
    
    print("Transfer complete.")
