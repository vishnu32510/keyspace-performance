import time
import uuid
import json
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

BATCH_SIZE = 200
MAX_WORKERS = 5

# Insert Bulk Review
def get_record_count(session):
    rows = session.execute("SELECT * FROM reviews").all()
    return len(rows)

def bulk_insert_reviews(session, filepath):
    inserted_ids = []

    with open(filepath, 'r') as f:
        records = [json.loads(line.strip()) for line in f]

    before_count = get_record_count(session)
    print(f"📊 Records before insert: {before_count}")

    start = time.time()
    query = """
        INSERT INTO reviews (
            asin, review_id, user_id, title, text, rating,
            timestamp, helpful_vote, verified_purchase
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    def insert_batch(batch):
        batch_ids = []
        for record in batch:
            review_id = str(uuid.uuid4())
            for attempt in range(3):
                try:
                    session.execute(stmt, (
                        record["asin"], review_id, record["user_id"], record["title"],
                        record["text"], float(record["rating"]), record["timestamp"],
                        int(record["helpful_vote"]), record["verified_purchase"]
                    ))
                    batch_ids.append((record["asin"], review_id))
                    break  # successful insert
                except Exception as e:
                    if attempt == 2:
                        print(f"❌ Insert failed for {review_id}: {e}")
                    else:
                        time.sleep(0.5)  # brief pause before retry
        return batch_ids

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for i in tqdm(range(0, len(records), BATCH_SIZE), desc="📦 Inserting"):
            batch = records[i:i + BATCH_SIZE]
            futures.append(executor.submit(insert_batch, batch))
            time.sleep(0.01)  # throttle slightly

        with tqdm(total=len(futures), desc="🔄 Processing Batches") as pbar:
            for future in as_completed(futures):
                inserted_ids.extend(future.result())
                pbar.update(1)

    after_count = get_record_count(session)
    duration = round((time.time() - start) * 1000, 2)
    inserted_count = after_count - before_count

    print(f"📊 Records after insert: {after_count}")
    print(f"✅ Inserted {inserted_count} new records in {duration} ms")
    return inserted_ids, duration



# Read Bulk Review
def read_all_reviews(session):
    query = "SELECT * FROM reviews"
    start = time.time()
    rows = session.execute(query).all()
    for _ in tqdm(range(len(rows)), desc="📖 Reading"):
        pass
    duration = round((time.time() - start) * 1000, 2)
    print(f"📖 Read {len(rows)} records in {duration} ms")
    return rows, duration

from cassandra import WriteTimeout
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Update Bulk Review
def update_all_ratings(session, boost=0.5):
    rows, _ = read_all_reviews(session)
    start = time.time()
    query = "UPDATE reviews SET rating=%s WHERE asin=%s AND review_id=%s"
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    def update_batch(batch):
        for row in batch:
            new_rating = row.rating + boost
            for attempt in range(3):  # Retry up to 3 times
                try:
                    session.execute(stmt, (new_rating, row.asin, row.review_id))
                    break  # Success, exit retry loop
                except WriteTimeout as e:
                    if attempt == 2:
                        print(f"❌ Failed to update {row.review_id} after 3 attempts: {e}")
                    else:
                        time.sleep(0.5)  # Wait a bit before retrying

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(update_batch, rows[i:i + BATCH_SIZE])
            for i in tqdm(range(0, len(rows), BATCH_SIZE), desc="🔄 Updating")
        ]
        with tqdm(total=len(futures), desc="🔄 Processing Batches") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    duration = round((time.time() - start) * 1000, 2)
    print(f"🔄 Updated rating for {len(rows)} records in {duration} ms")
    return len(rows), duration


# Delete All Reviews
def delete_all_reviews(session):
    rows, _ = read_all_reviews(session)
    start = time.time()
    query = "DELETE FROM reviews WHERE asin=%s AND review_id=%s"
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)

    def delete_batch(batch):
        for row in batch:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    session.execute(stmt, (row.asin, row.review_id))
                    break  # Successful delete
                except Exception as e:
                    if attempt == 2:
                        print(f"❌ Failed to delete {row.review_id} after 3 attempts: {e}")
                    else:
                        time.sleep(0.5)  # Retry delay

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(delete_batch, rows[i:i + BATCH_SIZE])
            for i in tqdm(range(0, len(rows), BATCH_SIZE), desc="🗑️ Deleting")
        ]
        with tqdm(total=len(futures), desc="🔄 Processing Batches") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    duration = round((time.time() - start) * 1000, 2)
    print(f"🗑️ Deleted {len(rows)} records in {duration} ms")
    return len(rows), duration

