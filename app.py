import boto3
import time
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.cluster import Cluster
from cassandra_sigv4.auth import SigV4AuthProvider
from dotenv import load_dotenv
import os

from keyspace_crud_bulk import (
    bulk_insert_reviews, delete_all_reviews,
    read_all_reviews, update_all_ratings
)
from keyspace_crud_single import (
    insert_single_review, read_single_review,
    update_single_review, delete_single_review
)

from cassandra import InvalidRequest

def create_keyspace_if_not_exists(session, keyspace_name="all_beauty"):
    create_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH REPLICATION = {{'class': 'SingleRegionStrategy'}};
    """
    start = time.time()
    session.execute(create_query)
    create_duration = round((time.time() - start) * 1000, 2)
    print(f"✅ Keyspace '{keyspace_name}' creation triggered in {create_duration} ms")

    # Wait for keyspace to be available
    for i in range(10):
        try:
            session.set_keyspace(keyspace_name)
            print(f"✅ Keyspace '{keyspace_name}' is active.")
            return keyspace_name, create_duration
        except InvalidRequest:
            print(f"⏳ Waiting for keyspace '{keyspace_name}' to be available... retry {i + 1}")
            time.sleep(2)

    raise Exception(f"❌ Timed out waiting for keyspace '{keyspace_name}' to become active.")


# Create Table
def create_reviews_table(session):
    query = """
    CREATE TABLE IF NOT EXISTS reviews (
        asin text,
        review_id text,
        user_id text,
        title text,
        text text,
        rating float,
        timestamp bigint,
        helpful_vote int,
        verified_purchase boolean,
        PRIMARY KEY (asin, review_id)
    );
    """
    start = time.time()
    session.execute(query)
    duration = round((time.time() - start) * 1000, 2)
    print(f"✅ Table created in {duration} ms")
    return "reviews", duration

def delete_reviews_table(session):
    query = "DROP TABLE IF EXISTS reviews"
    start = time.time()
    session.execute(query)
    duration = round((time.time() - start) * 1000, 2)
    print(f"🧨 Table 'reviews' dropped in {duration} ms")
    return "reviews", duration

# --- Keyspace Deletion Helper ---
def delete_keyspace(session, keyspace_name="all_beauty"):
    query = f"DROP KEYSPACE IF EXISTS {keyspace_name}"
    start = time.time()
    session.execute(query)
    duration = round((time.time() - start) * 1000, 2)
    print(f"💥 Keyspace '{keyspace_name}' dropped in {duration} ms")
    return keyspace_name, duration

# --- Single Operations ---
def run_single_operations_with_timing(session):
    print("\n🚀 Starting single-record operations...")

    sample_record = {
        "asin": "B00YQ6X8EO",
        "user_id": "AGKHLEW2SOWHNMFQIJGBECAF7INQ",
        "title": "Such a lovely scent but not overpowering.",
        "text": "This spray is really nice. It smells really good, goes on really fine, and does the trick. I have a lot of hair, medium thickness. I am comparing to other brands with yucky chemicals so I'm gonna stick with this. Try it!",
        "rating": 5.0,
        "timestamp": 1588687728923,
        "helpful_vote": 0,
        "verified_purchase": True
    }

    review_id, insert_time = insert_single_review(session, sample_record)
    _, read_time = read_single_review(session, sample_record["asin"], review_id)
    _, update_time = update_single_review(session, sample_record["asin"], review_id, 4.7)
    _, delete_time = delete_single_review(session, sample_record["asin"], review_id)

    timing_table = {
        "Insert": insert_time,
        "Read": read_time,
        "Update": update_time,
        "Delete": delete_time,
    }

    print("\n📊 Single Operation Timings:")
    for op, t in timing_table.items():
        print(f"{op.ljust(15)} : {t} ms")

    return timing_table

# --- Bulk Operations ---
def run_bulk_operations_with_timing(session, filepath="All_Beauty.jsonl"):
    print("\n🚀 Starting bulk operations...")

    _, bulk_insert_time = bulk_insert_reviews(session, filepath)
    time.sleep(10)
    _, bulk_read_time = read_all_reviews(session)
    time.sleep(10)
    _, bulk_update_time = update_all_ratings(session, boost=0.5)
    time.sleep(10)
    _, bulk_delete_time = delete_all_reviews(session)
    time.sleep(10)

    bulk_timing_table = {
        "Bulk Insert": bulk_insert_time,
        "Bulk Read": bulk_read_time,
        "Bulk Update": bulk_update_time,
        "Bulk Delete": bulk_delete_time,
    }

    print("\n📊 Bulk Operation Timings:")
    for op, t in bulk_timing_table.items():
        print(f"{op.ljust(15)} : {t} ms")

    return bulk_timing_table

# --- Connect to Amazon Keyspaces ---
def setup_connection():
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('./AmazonRootCA1.pem')
    ssl_context.verify_mode = CERT_REQUIRED
    
    load_dotenv()  # Load variables from .env

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("AWS_REGION")

    boto_session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )

    auth_provider = SigV4AuthProvider(boto_session)

    cluster = Cluster(
        ['cassandra.us-east-2.amazonaws.com'],
        port=9142,
        ssl_context=ssl_context,
        auth_provider=auth_provider
    )

    session = cluster.connect()
    return session, cluster


# --- MAIN RUNNER ---
if __name__ == "__main__":
    session, cluster = setup_connection()
    print("✅ Connected to Amazon Keyspaces.")
    
    session.set_keyspace("all_beauty")

    # Run both simulations
    run_single_operations_with_timing(session)

    # Run bulk operations
    run_bulk_operations_with_timing(
        session,
        filepath="data/All_Beauty.jsonl",
    )
    
    # Clean shutdown
    session.shutdown()
    cluster.shutdown()
    print("✅ Connection closed.")
