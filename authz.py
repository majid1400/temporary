import json
import pika
from elasticsearch import NotFoundError
from retry import retry
from config import Config, send_rabbitmq, client
from utils import get_index
from services import is_robot, is_exist


def callback(ch, method, properties, body):
    r = body.decode()
    json_resp = json.loads(r)

    result = json_resp[0][0]
    db_index = json_resp[0][1]
    post_id = json_resp[0][2]


    while True:
        try:
            robot = is_robot(result['user_id'])
            duplicate, duplicate_count = is_exist(result['message_clean'][:150])
        except Exception as e:
            print(f'error tasks: {e}')
            send_rabbitmq('fail',
                          {'result': result, 'db_index': db_index, 'post_id': post_id})
            ch.basic_ack(delivery_tag=method.delivery_tag)
            break

        r = {'jebhe': jebhe,'duplicate': duplicate, 'duplicate_count': duplicate_count}

        for k, v in r.items():
            query_update = {
                "script": {
                    "source": f'ctx._source.{k} = params.{k}',
                    "lang": 'painless',
                    "params": {
                        f"{k}": v
                    }
                }
            }

            try:
                client().update(index=db_index, id=post_id, body=query_update, retry_on_conflict=10)
                # ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f'INSERT tasks{k:16} {post_id:6}\t\t {db_index}')
            except NotFoundError:
                db_index = get_index(post_id, db_index)
                print('Change db get_index')
                continue
            except Exception as e:
                send_rabbitmq('fail',
                              {'result': result, 'db_index': db_index, 'post_id': post_id})
                # ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f'Error tasks: {e} {post_id} {db_index}')
                break

        break
    print('*#' * 30)


@retry(pika.exceptions.ChannelClosed, delay=5, jitter=(1, 3))
@retry(pika.exceptions.ConnectionClosed, delay=5, jitter=(1, 3))
@retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def consume():
    credentials = pika.PlainCredentials(Config.RABBITMQ_USERNAME, Config.RABBITMQ_PASSWORD)

    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(Config.RABBITMQ_HOST, port=Config.RABBITMQ_PORT,
                                      virtual_host=Config.RABBITMQ_VIRTUAL_HOST,
                                      credentials=credentials, heartbeat=100))
    except:
        print("Change connection without VIRTUAL HOST")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(Config.RABBITMQ_HOST, port=Config.RABBITMQ_PORT,
                                      credentials=credentials, heartbeat=100))

    try:
        channel = connection.channel()
    except:
        print("Error connection rabbitmq")

    channel.basic_qos(prefetch_count=100)
    channel.basic_consume(queue=Config.RABBITMQ_GET_QUEUE, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt as e:
        print(f"stop_consuming: {e}")
        channel.stop_consuming()
        connection.close()
    except pika.exceptions.ChannelClosed as e:
        print('Error ChannelClosed: ')
        pass
    except pika.exceptions.ConnectionClosedByBroker as e:
        print(f"ConnectionClosedByBroker: {e}")
        pass
    except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
        print(f'Error close: {e}')
        pass
    except Exception as e:
        print(f"Error rabbitmq123: {e}")
        pass


if __name__ == "__main__":
    consume()
