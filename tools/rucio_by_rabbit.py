# Listen for a rabbit MQ request and download the requested files.
# Looking at the AST, replace the datasets with the downloaded files (relative to "data").
import adl_func_backend.dataset_resolvers.gridds as gridds
from ruciopylib.rucio_cache_interface import DatasetQueryStatus, rucio_cache_interface
from ruciopylib.dataset_local_cache import dataset_local_cache
import pika
import sys
import ast
import pickle
from time import sleep
import json
import base64
import os
import logging
import asyncio
import threading
import inspect

class RucioNoSuchDataset(BaseException):
    'Thrown if we cannot find a dataset'
    def __init__ (self, msg):
        BaseException.__init__(self, msg)

async def chained_identity(a):
    return a

def run_download_work(ch, method, properties, body, connection):
    # The body contains the ast, in pickle format.
    # TODO: errors! Errors! Errors!
    logging.debug('Starting thread callback')
    info = json.loads(body)
    hash = info['hash']
    a = pickle.loads(base64.b64decode(info['ast']))
    if a is None or not isinstance(a, ast.AST):
        # This is an internal error. Dead letterbox this message. Send the crash report and clear it from the queue.
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'crashed'})))
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='crashed_request',
            body=json.dumps({'hash':hash, 'message':'while downloading a dataset via rucio', 'log': [f"Body of message wasn't of type AST: {a}"]})))
        connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=method.delivery_tag))
        logging.info('Done processing message with an error - we did not get an AST')
        return

    # Make sure there is an event loop running in this thread.
    asyncio.set_event_loop(asyncio.new_event_loop())

    # Next, lets look at it and process the files.
    connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'downloading'})))
    try:
        new_ast_async = gridds.use_executor_dataset_resolver(a, chained_executor=chained_identity)
        loop = asyncio.get_event_loop()
        new_ast = loop.run_until_complete(new_ast_async)
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'done_downloading'})))
    except BaseException as e:
        # Ouch. Ok - this crashed for some reason. Better report it.
        logging.info(f'Failed to download dataset with error: {str(e)}.')
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='status_change_state', body=json.dumps({'hash':hash, 'phase':'crashed'})))
        frame = inspect.trace()[-1]
        where_raised = f'Exception raised in function {frame[3]} ({frame[1]}:{frame[2]}).'
        msg_data = json.dumps({'hash':hash, 'message':'while downloading a dataset via rucio', 'log': [f'Failed to download dataset:', f'  {str(e)}', f'  {where_raised}']})
        connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='crashed_request',
            body=msg_data))
        connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=method.delivery_tag))
        return
        
    # Pickle the converted AST back up, and send it down the line.
    new_info = {
        'hash': hash,
        'ast': base64.b64encode(pickle.dumps(new_ast)).decode(),
    }
    connection.add_callback_threadsafe(lambda: ch.basic_publish(exchange='', routing_key='parse_cpp', body=json.dumps(new_info)))

    # We are done with the download and we've sent the message on. Time to ask it so
    # we don't try to do it again.
    connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=method.delivery_tag))
    logging.info('Done successful processing of message')

def process_message(ch, method, properties, body, connection):
    '''This has to execute very fast so we can make sure we do not cause a block and
    make the heartbeat fail.
    '''
    logging.debug('starting processing of message')
    t = threading.Thread(target=run_download_work, args=(ch, method, properties, body, connection))
    t.start()
    logging.debug('started processing of message')

def download_ds (parsed_url, url:str, datasets:rucio_cache_interface):
    'Called when we are dealing with a local_ds scheme. We basically sit here and wait'

    ds_name = parsed_url.netloc
    # TODO: This file// is an illegal URL. It actually should be ///, but EventDataSet can't handle that for now.
    logging.info(f'Starting download of {ds_name}.')
    status,files = datasets.download_ds(ds_name, do_download=True, log_func=lambda l: logging.info(l))
    logging.info(f'Results from download of {ds_name}: {status} - {files}')

    if status == DatasetQueryStatus.does_not_exist:
        raise RucioNoSuchDataset(f'The dataset {ds_name} could not be found by rucio.')
    elif status == DatasetQueryStatus.results_valid:
        if files is None:
            raise BaseException('Valid results came back with None for the list of files. Not allowed! Programming error!')
        files = [f'file:///data/{f}' for f in files]
        return files
    else:
        raise BaseException("Do not know what the status means!")

def listen_to_queue(dataset_location:str, rabbit_node:str, rabbit_user:str, rabbit_pass:str):
    'Download and pass on datasets as we see them'

    # Where we will store everything
    datasets = rucio_cache_interface(dataset_local_cache(dataset_location))

    # Config the scanner
    gridds.resolve_callbacks['localds'] = lambda parsed_url, url: download_ds(parsed_url, url, datasets)

    # Connect and setup the queues we will listen to and push once we've done.
    if rabbit_pass in os.environ:
        rabbit_pass = os.environ[rabbit_pass]
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_node, credentials=credentials))
    channel = connection.channel()
    # We run pretty slowly, so make sure this guy doesn't keep too many.
    channel.basic_qos(prefetch_count=1)

    # Get the queues going
    channel.queue_declare(queue='find_did')
    channel.queue_declare(queue='parse_cpp')
    channel.basic_consume(queue='find_did', 
        on_message_callback=lambda ch, method, properties, body: process_message(ch, method, properties, body, connection),
        auto_ack=False
        )

    # We are setup. Off we go. We'll never come back.
    logging.info('Starting message consumption')
    channel.start_consuming()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    bad_args = len(sys.argv) != 5
    if bad_args:
        print ("Usage: python download_did_rabbit.py <dataset-cache-location> <rabbit-mq-node-address> <username> <password>")
    else:
        listen_to_queue (sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
