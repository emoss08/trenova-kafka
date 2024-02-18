# Former Implementations

### src/main/java/kafka/DatabaseUtils.java

```python
import os
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv

# Environment Variables
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
print(dotenv_path)
load_dotenv(dotenv_path)

CONN_PARAMS = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
}


class DictRowFactory:
    def __init__(self, cursor):
        self.fields = [c.name for c in cursor.description]

    def __call__(self, values: Sequence[Any]) -> dict[str, Any]:
        return dict(zip(self.fields, values))


def get_active_kafka_table_change_alerts():
    try:
        conn = psycopg.connect(**CONN_PARAMS, row_factory=DictRowFactory)
        cur = conn.cursor()

        query = """
        SELECT * FROM table_change_alert
        WHERE
            status = 'A' AND
            source = 'KAFKA' AND
            (
                (effective_date <= %s OR effective_date IS NULL) AND
                (expiration_date >= %s OR expiration_date IS NULL)
            );
        """

        # CUrrent timestamp
        now = datetime.now()

        cur.execute(query, (now, now))

        return cur.fetchall()
    except psycopg.OperationalError as e:
        print(f"Error connecting to database: {e}")
        raise e
```