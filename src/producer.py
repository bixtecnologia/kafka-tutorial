import random
import concurrent.futures
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer
import time

BASE_URL = 'https://randomuser.me/api/?nat=us'
PARTIES = ["Management Party", "South Party", "Republic Party"]
random.seed(42)


def generate_voter_data():
    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, timeout=5)
            if response.status_code == 200:
                user_data = response.json()['results'][0]
                return {
                    "voter_id": user_data['login']['uuid'],
                    "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
                    "date_of_birth": user_data['dob']['date'],
                    "gender": user_data['gender'],
                    "nationality": user_data['nat'],
                    "registration_number": user_data['login']['username'],
                    "address": {
                        "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                        "city": user_data['location']['city'],
                        "state": user_data['location']['state'],
                        "country": user_data['location']['country'],
                        "postcode": user_data['location']['postcode']
                    },
                    "email": user_data['email'],
                    "phone_number": user_data['phone'],
                    "cell_number": user_data['cell'],
                    "picture": user_data['picture']['large'],
                    "registered_age": user_data['registered']['age']
                }
            else:
                print(f"Error fetching data, attempt {attempt+1}")
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Request exception: {e}, attempt {attempt+1}")
            time.sleep(1)
    raise Exception("Failed to fetch voter data after retries")


static_data_candidates = [
    {
        "candidate_name": "Jonathan Wright",
        "position": "Partido Republicano",
        "photo_url": "https://github.com/user-attachments/assets/3987da17-166f-4890-965c-39503c4f3a67"
    },
    {
        "candidate_name": "Michael Anderson",
        "position": "Partido Democrata",
        "photo_url": "https://github.com/user-attachments/assets/e5251c26-0769-4b1c-9265-12b2b19b7fe0"
    },
    {
        "candidate_name": "Emily Bolunn",
        "position": "Partido Libertário",
        "photo_url": "https://github.com/user-attachments/assets/9832fb17-b456-4d6e-8ff3-5e2fc68dda46"
    }
]

def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]


        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id SERIAL PRIMARY KEY,
            candidate_name VARCHAR(255),
            position VARCHAR(255),
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id INTEGER,
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()


def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address']['street'],
                 voter['address']['city'], voter['address']['state'], voter['address']['country'],
                 voter['address']['postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()


def process_voter(i):
    retries = 3
    for attempt in range(retries):
        try:
            # Each thread has its own connection and producer
            conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
            cur = conn.cursor()
            producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

            voter_data = generate_voter_data()

            insert_voters(conn, cur, voter_data)

            producer.produce(
                voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            producer.flush()

            print(f'Produced voter {i}, data: {voter_data}')

            cur.close()
            conn.close()
            break  # Break the retry loop if successful
        except Exception as e:
            print(f"Error processing voter {i} on attempt {attempt+1}: {e}")
            if attempt == retries - 1:
                print(f"Failed to process voter {i} after {retries} attempts.")
            time.sleep(1)  # Wait a bit before retrying

if __name__ == "__main__":
    # Initial setup remains the same
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    create_tables(conn, cur)
    # Insert static candidates if not already present
    cur.execute("SELECT COUNT(*) FROM candidates")
    if cur.fetchone()[0] == 0:
        for candidate in static_data_candidates:
            cur.execute("""
                INSERT INTO candidates (candidate_name, position, photo_url)
                VALUES (%s, %s, %s)
            """, (candidate['candidate_name'], candidate['position'], candidate['photo_url']))
        conn.commit()
    cur.close()
    conn.close()

    # Multithreading to process voters
    num_voters = 500
    max_workers = 15  # Adjust based on your system's capacity
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_voter, range(num_voters))