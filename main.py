#!/usr/bin/env python3
import base64
import json
import os
import time
from google.api_core import retry
from google.api_core import exceptions
from google.cloud import pubsub_v1
from google.cloud import logging
from protobuf_to_dict import protobuf_to_dict


PROJECT=os.environ.get('PUBSUB_PROJECT', 'project')
SUBSCRIPTION=os.environ.get('PUBSUB_SUBSCRIPTION', 'test_sub')

client = logging.Client()
logger = client.logger('pubsub_topic_logger')

if True or __name__ == '__main__':
    predicate = retry.if_exception_type(
        exceptions.InternalServerError,
        exceptions.TooManyRequests,
        exceptions.ServiceUnavailable,
        exceptions.DeadlineExceeded,
    )
    print(f'Logging messages for {PROJECT}/{SUBSCRIPTION}...')
    retry_options = retry.Retry(predicate=predicate, initial=0.1, multiplier=1.1, maximum=0.3, deadline=2*60)
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT, SUBSCRIPTION)
    while True:
        try:
            # print('pulling...')
            response = subscriber.pull(subscription_path, max_messages=10, retry=retry_options, timeout=12)
        except (exceptions.DeadlineExceeded, exceptions.RetryError) as e:
            # print('nothing ...')
            response = None

        if response:
            now = time.time()
            for msg in response.received_messages:
                secs = msg.message.publish_time.seconds
                nanos = msg.message.publish_time.nanos

                publish_time = secs + nanos / pow(10, 9)
                log = {
                    'receive_time': now,
                    'lag_milliseconds': (now - publish_time) * 1000,
                    'message': protobuf_to_dict(msg),
                }
                data = log['message']['message']['data']
                try:
                    log['message']['message']['json'] = json.loads(data.decode('utf-8'))
                    log['message']['message']['data'] = data.decode('utf-8')
                except:
                    log['message']['message']['data'] = base64.b64encode(data).decode('utf-8')
                # print("Received message:", json.dumps(log))
                logger.log_struct(log)

            ack_ids = [msg.ack_id for msg in response.received_messages]
            subscriber.acknowledge(subscription_path, ack_ids)
