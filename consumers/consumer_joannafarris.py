# consumers/consumer_joannafarris.py
import json
import sqlite3
import signal
import sys
from datetime import datetime

from utils.utils_config import (
    get_kafka_topic,
    get_kafka_consumer_group_id,
    get_sqlite_path,
)
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# --- Config from your getters ---
TOPIC = get_kafka_topic()
GROUP = get_kafka_consumer_group_id()
DB_PATH = get_sqlite_path()  # pathlib.Path

# --- DB setup ---
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
conn = sqlite3.connect(str(DB_PATH), isolation_level=None)  # autocommit per statement
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS message_tone (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    author TEXT NOT NULL,
    tone TEXT NOT NULL CHECK (tone IN ('question','excited','plain')),
    exclamations INTEGER NOT NULL DEFAULT 0,
    qmarks INTEGER NOT NULL DEFAULT 0,
    message_length INTEGER NOT NULL,
    category TEXT,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset INTEGER
);
""")

# --- Simple tone logic ---
def tone_for(message_text, sentiment):
    t = message_text or ""
    ex = t.count("!")
    qm = t.count("?")
    if qm > 0:
        return "question", ex, qm
    if ex >= 2 or (sentiment is not None and float(sentiment) >= 0.70):
        return "excited", ex, qm
    return "plain", ex, qm

def main():
    # Use shared helper; JSON-decode the message value into a dict
    consumer = create_kafka_consumer(
        topic_provided=TOPIC,
        group_id_provided=GROUP,
        value_deserializer_provided=lambda b: json.loads(b.decode("utf-8")),
    )

    logger.info(f"consumer ready | topic={TOPIC} group={GROUP} db={DB_PATH}")

    def shutdown(*_):
        logger.info("stopping consumer…")
        try:
            consumer.close()
        except Exception:
            pass
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    for record in consumer:
        try:
            msg = record.value  # dict (thanks to our deserializer)
            text = msg.get("message", "")
            author = msg.get("author", "unknown")
            ts = msg.get("timestamp") or datetime.utcnow().isoformat(timespec="seconds")
            category = msg.get("category")
            sentiment = msg.get("sentiment")
            mlen = int(msg.get("message_length") or len(text))

            tone, ex, qm = tone_for(text, sentiment)

            cur.execute(
                """INSERT INTO message_tone
                   (ts, author, tone, exclamations, qmarks, message_length, category,
                    kafka_topic, kafka_partition, kafka_offset)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, author, tone, ex, qm, mlen, category,
                 record.topic, record.partition, record.offset),
            )
            logger.info(f"[ok] {ts} | {author} | {tone} ex={ex} qm={qm} len={mlen}")
        except Exception as e:
            logger.warning(f"failed at offset {getattr(record, 'offset', '?')}: {e}")

if __name__ == "__main__":
    main()
