import time
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import uuid

# Insert Single Review
def insert_single_review(session, record):
    review_id = str(uuid.uuid4())
    query = """
    INSERT INTO reviews (
        asin, review_id, user_id, title, text, rating, timestamp, helpful_vote, verified_purchase
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    start = time.time()
    session.execute(stmt, (
        record["asin"], review_id, record["user_id"], record["title"],
        record["text"], float(record["rating"]), record["timestamp"],
        int(record["helpful_vote"]), record["verified_purchase"]
    ))
    duration = round((time.time() - start) * 1000, 2)
    print(f"✅ Inserted record '{review_id}' in {duration} ms")
    return review_id, duration

# Read Single Review
def read_single_review(session, asin, review_id):
    query = "SELECT * FROM reviews WHERE asin=%s AND review_id=%s"
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    start = time.time()
    result = session.execute(stmt, (asin, review_id)).one()
    duration = round((time.time() - start) * 1000, 2)
    print(f"📖 Read record '{review_id}' in {duration} ms")
    return review_id, duration

# Update Single Review (e.g., update rating)
def update_single_review(session, asin, review_id, new_rating):
    query = "UPDATE reviews SET rating=%s WHERE asin=%s AND review_id=%s"
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    start = time.time()
    session.execute(stmt, (new_rating, asin, review_id))
    duration = round((time.time() - start) * 1000, 2)
    print(f"🔄 Updated rating for '{review_id}' in {duration} ms")
    return review_id, duration

# Delete Single Review
def delete_single_review(session, asin, review_id):
    query = "DELETE FROM reviews WHERE asin=%s AND review_id=%s"
    stmt = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    start = time.time()
    session.execute(stmt, (asin, review_id))
    duration = round((time.time() - start) * 1000, 2)
    print(f"🗑️ Deleted record '{review_id}' in {duration} ms")
    return review_id, duration

