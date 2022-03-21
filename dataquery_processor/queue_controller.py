import boto3
import json
import logging
logger = logging.getLogger(__name__)


class Message(object):

    def __init__(self, message):
        self.message = message

    def order(self):
        body = self.message['Body']
        response = json.loads(body)
        payload = response['responsePayload']['order']
        return payload

    def receipt_handle(self):
        return self.message['ReceiptHandle']


class QueueController(object):

    def __init__(self):
        session = boto3.Session(profile_name='dev')
        self.sqsClient = session.client('sqs', region_name='us-east-2')
        self.queue = "https://sqs.us-east-2.amazonaws.com/240624597515/dataquery-queue"

    def read_message(self):
        messageResponse = self.sqsClient.receive_message(
            QueueUrl=self.queue,
            MaxNumberOfMessages=1
        )
        if 'Messages' in messageResponse:
            return Message(messageResponse['Messages'][0])
        else:
            return None

    def delete_message(self, message):
        self.sqsClient.delete_message(
            QueueUrl=self.queue,
            ReceiptHandle=message.receipt_handle()
            )
        logger.info("deleted "+message.order()['orderRef'])



