import random
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaError, SerializingProducer

from utils import kafka_delivery_report

kafka_conf = {'bootstrap.servers': 'localhost:9092'}

if __name__ == '__main__':
    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
    )
    cursor = conn.cursor()
    candidates_query = cursor.execute(
        """
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """
    )
    candidates = [candidate[0] for candidate in cursor.fetchall()]

    if len(candidates) == 0:
        raise Exception('No candidates available in database')

    producer = SerializingProducer(kafka_conf)
    consumer = Consumer(
        kafka_conf
        | {
            'group.id': 'voting-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
    )
    consumer.subscribe(['voters'])

    try:
        while True:
            msg = consumer.poll()
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'The kafka error {msg.error()}')
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = (
                    voter
                    | chosen_candidate
                    | {
                        'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                        'vote': 1
                    }
                )

                print(
                    'User {} is voting for the candidate {}'.format(
                        vote['voter_id'], vote['candidate_id']
                    )
                )
                cursor.execute(
                    '''
                    INSERT INTO votes (voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)                    
                ''',
                    (vote['voter_id'], vote['candidate_id'], vote['voting_time']),
                )

                producer.produce(
                    topic='votes',
                    key=vote['voter_id'],
                    value=json.dumps(vote),
                    on_delivery=kafka_delivery_report,
                )
                producer.poll(0)
                conn.commit()
    except Exception as e:
        print(f'The exception: {e}')
