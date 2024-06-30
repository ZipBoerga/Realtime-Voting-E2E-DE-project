import json
from typing import Optional

import requests
import psycopg2
from psycopg2.errorcodes import UNIQUE_VIOLATION
from psycopg2 import errors
from confluent_kafka import SerializingProducer

from utils import kafka_delivery_report

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
CANDIDATES_NUMBER = 3
VOTERS_NUMBER = 1000


def generate_candidate_data(
    candidate_number: int, parties: list[str]
) -> Optional[dict]:
    response = requests.get(
        f'{BASE_URL}&gender={"female" if candidate_number % 2 == 0 else "male"}'
    )
    if response.status_code == 200:
        raw_data = response.json().get('results')[0]
        return {
            "candidate_id": raw_data['login']['uuid'],
            "candidate_name": f"{raw_data['name']['first']} {raw_data['name']['last']}",
            "party_affiliation": parties[candidate_number % len(parties)],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": raw_data['picture']['large'],
        }
    else:
        return None


def generate_voter_data() -> Optional[dict]:
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        raw_data = response.json().get('results')[0]
        return {
            "voter_id": raw_data['login']['uuid'],
            "voter_name": f"{raw_data['name']['first']} {raw_data['name']['last']}",
            "date_of_birth": raw_data['dob']['date'],
            "gender": raw_data['gender'],
            "nationality": raw_data['nat'],
            "registration_number": raw_data['login']['username'],
            "address": {
                "street": f"{raw_data['location']['street']['number']} {raw_data['location']['street']['name']}",
                "city": raw_data['location']['city'],
                "state": raw_data['location']['state'],
                "country": raw_data['location']['country'],
                "postcode": raw_data['location']['postcode'],
            },
            "email": raw_data['email'],
            "phone_number": raw_data['phone'],
            "picture": raw_data['picture']['large'],
            "registered_age": raw_data['registered']['age'],
        }
    else:
        return None


def is_valid(entry):
    return isinstance(entry, dict) and len(entry) != 0


if __name__ == '__main__':
    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
    )
    cursor = conn.cursor()
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

    candidate_counter = 0
    while candidate_counter < CANDIDATES_NUMBER:
        candidate = generate_candidate_data(candidate_counter, PARTIES)
        if not is_valid(candidate):
            print(
                f'error generating the candidate {candidate_counter}, will try to generate again'
            )
            continue
        try:
            cursor.execute(
                """
                            INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, 
                            photo_url) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                (
                    candidate['candidate_id'],
                    candidate['candidate_name'],
                    candidate['party_affiliation'],
                    candidate['biography'],
                    candidate['campaign_platform'],
                    candidate['photo_url'],
                ),
            )
            conn.commit()
            print(f'Successfully inserted candidate {candidate_counter}')
            candidate_counter = candidate_counter + 1
        except Exception as e:
            print(
                f'Couldn\'t process the voter due to the error: {e}, will retry it for the position {candidate_counter}'
            )

    voters_counter = 0

    while voters_counter < VOTERS_NUMBER:
        voter = generate_voter_data()
        if not is_valid(voter):
            print(f'error generating the voter {voters_counter}')
            continue
        try:
            cursor.execute(
                """
                INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, 
                address_street, address_city, address_state, address_country, address_postcode, email, phone_number, 
                picture, registered_age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    voter["voter_id"],
                    voter['voter_name'],
                    voter['date_of_birth'],
                    voter['gender'],
                    voter['nationality'],
                    voter['registration_number'],
                    voter['address']['street'],
                    voter['address']['city'],
                    voter['address']['state'],
                    voter['address']['country'],
                    voter['address']['postcode'],
                    voter['email'],
                    voter['phone_number'],
                    voter['picture'],
                    voter['registered_age'],
                ),
            )
            conn.commit()
            producer.produce(
                topic='voters',
                key=voter['voter_id'],
                value=json.dumps(voter),
                on_delivery=kafka_delivery_report,
            )
            print(f'Successfully processed voter {voters_counter}')
            voters_counter = voters_counter + 1
        except errors.lookup(UNIQUE_VIOLATION) as e:
            print(
                f'Couldn\'t process the voter due to the uniqueness error: {e},'
                f' will retry it for the position {voters_counter}'
            )
            conn.rollback()
        except Exception as e:
            print(
                f'Couldn\'t process the voter due to the error: {e}, will retry it for the position {voters_counter}'
            )

    conn.close()
    producer.flush()
