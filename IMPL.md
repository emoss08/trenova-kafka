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

### src/main/java/kafka/KafkaListener.java

```python

from __future__ import annotations

import concurrent
import concurrent.futures
import json
import logging
import os
import signal
import time
import types
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from dotenv import load_dotenv

from kafka import constants, log, queries

# Environment Variables
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

logger = log.get_default_logger()
debug, error = logger.debug, logger.error


POLL_TIMEOUT = 1.0
NO_ALERTS_MSG = "No active table change alerts."
UPDATE_TOPIC = os.environ.get("KAFKA_ALERT_UPDATE_TOPIC")


class KafkaListener:
    """
    KafkaListener is a class that provides functionality to listen to specific Kafka topics
    and process their messages accordingly. It is primarily responsible for handling table change alerts.
    """

    running = True
    # TODO(Wolfred): Replace all prints with SSE or websockets. Still haven't decided.
    # TODO(Wolfred): Replace this with AIOKAFKA.

    def __init__(self, thread_pool_size=10) -> None:
        self.thread_pool_size = thread_pool_size

    def _signal_handler(self, _signal: int, frame: types.FrameType | None) -> None:
        debug("Received termination signal. Stopping listener...")
        self.running = False

    def _connect(self) -> tuple[Consumer, Consumer] | None:
        config = {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": os.environ.get("KAFKA_GROUP_ID"),
            "auto.offset.reset": os.environ.get("KAFKA_OFFSET_RESET"),
            "enable.auto.commit": os.environ.get("KAFKA_AUTO_COMMIT"),
            "fetch.min.bytes": os.environ.get("KAFKA_AUTO_COMMIT_INTERVAL_MS"),
            "auto.commit.interval.ms": os.environ.get("KAFKA_AUTO_COMMIT_INTERVAL_MS"),
        }

        while self.running:
            try:
                consumer = Consumer(config)
                consumer.list_topics(timeout=10)
                return consumer, Consumer(config)
            except KafkaError as e:
                if e.args[0].code() != KafkaError._ALL_BROKERS_DOWN:
                    error(f"KafkaError: {e}")
                    raise e
                debug("All brokers are down. Retrying connection...")
                time.sleep(5)
        return None

    @staticmethod
    def _get_topic_list() -> list[str]:
        """
        Fetch the list of topics to subscribe to. Ensure no empty strings are included.
        """
        active_alerts = queries.get_active_kafka_table_change_alerts() or []
        alerts = [alert["topic"] for alert in active_alerts if alert["topic"]]

        return alerts

    @staticmethod
    def _get_messages(
        *, consumer: Consumer, timeout: float, max_messages: int = 100
    ) -> list[Message]:
        messages = consumer.consume(max_messages, timeout)
        valid_messages = []
        for message in messages:
            if message is None:
                continue
            elif message.error():
                error(f"Consumer error: {message.error()}")
                continue
            valid_messages.append(message)
        return valid_messages

    @staticmethod
    def _parse_message(*, message: Message) -> dict[str, Any] | None:
        message_value = message.value().decode("utf-8")
        try:
            data = json.loads(message_value)
        except json.JSONDecodeError:
            error("Error decoding message value as JSON.")
            return None
        return data.get("payload", {})  # Accessing 'payload' key from the decoded JSON

    @staticmethod
    def _get_message(*, consumer: Consumer, timeout: float) -> Message | None:
        message = consumer.poll(timeout)
        if message is None:
            return None
        elif message.error():
            error(f"Consumer error: {message.error()}")
            return None
        return message

    @staticmethod
    def _update_subscriptions(
        *, data_consumer: Consumer, table_changes: list[str]
    ) -> None:
        """
        Update the consumer's subscription list.
        """
        try:
            if table_changes:
                data_consumer.unsubscribe()
                data_consumer.subscribe(table_changes)
                logging.debug(f"Subscribed to topics: {', '.join(table_changes)}")
            else:
                logging.debug("No active table changes to subscribe to.")
        except KafkaError as e:
            logging.error(f"Failed to update subscriptions: {e}", exc_info=True)
            raise

    @staticmethod
    def _format_message(*, field_value_dict: dict) -> str:
        return "\n".join(
            f"Field: {field}, Value: {value}"
            for field, value in field_value_dict.items()
        )

    def _process_message(
        self,
        data_message: Message,
        associated_table_change,  # Change this to the correct type
    ) -> None:
        if not data_message.value():
            return

        data = self._parse_message(message=data_message)

        if data is None:  # Added to handle cases where message is not valid JSON.
            return

        op_type = data.get("op")

        op_type_mapping = {
            "c": constants.ActionChoices.INSERT.value,
            "u": constants.ActionChoices.UPDATE.value,
        }
        if not op_type:
            return
        translated_op_type = op_type_mapping.get(op_type)

        if (
            not translated_op_type
            or translated_op_type not in associated_table_change["database_action"]
        ):
            return

        field_value_dict = data.get("after") or {}

        recipient_list = (
            associated_table_change["email_recipients"].split(",")
            if associated_table_change["email_recipients"]
            else []
        )
        subject = (
            associated_table_change["custom_subject"]
            or f"Table Change Alert: {data_message.topic()}"
        )

        formatted_message = self._format_message(field_value_dict=field_value_dict)

        debug(
            f"Sending email to {recipient_list} with subject {subject} for message {formatted_message}"
        )
        # send_mail(
        #     subject=subject,
        #     message=self._format_message(field_value_dict=field_value_dict),
        #     from_email="table_change@trenova.app",
        #     recipient_list=recipient_list,
        # )

    def listen(self) -> None:
        self.register_signals()
        consumers = self._connect()

        if consumers is None:
            error("Failed to connect, exiting...")
            return

        data_consumer, alert_update_consumer = consumers

        self._subscribe_consumers_to_topics(
            data_consumer=data_consumer, alert_update_consumer=alert_update_consumer
        )

        self._execute_tasks(
            data_consumer=data_consumer, alert_update_consumer=alert_update_consumer
        )

        alert_update_consumer.close()
        data_consumer.close()
        debug("Consumers closed.")

    def register_signals(self) -> None:
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _subscribe_consumers_to_topics(
        self, *, data_consumer: Consumer, alert_update_consumer: Consumer
    ) -> None:
        table_changes = self._get_topic_list()
        if not table_changes:
            debug(NO_ALERTS_MSG)
            return

        alert_update_consumer.subscribe(["trenova_app_.public.table_change_alert"])
        debug(f"Subscribed to alert update topic: {UPDATE_TOPIC}")
        data_consumer.subscribe(list(table_changes))
        debug(f"Subscribed to topics: {list(table_changes)}")

    def _execute_tasks(
        self, *, data_consumer: Consumer, alert_update_consumer: Consumer
    ) -> None:
        futures = set()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=int(os.environ.get("KAFKA_THREAD_POOL_SIZE")),
            thread_name_prefix="kafka_listener",
        ) as executor:
            try:
                while self.running:
                    if len(futures) < 100:
                        self._handle_messages(
                            data_consumer=data_consumer,
                            alert_update_consumer=alert_update_consumer,
                            futures=futures,
                            executor=executor,
                        )

                    self._wait_for_futures_to_complete(futures=futures)

            except Exception as e:
                self._handle_exception(
                    e=e,
                    data_consumer=data_consumer,
                    alert_update_consumer=alert_update_consumer,
                )

    def _handle_messages(
        self,
        *,
        data_consumer: Consumer,
        alert_update_consumer: Consumer,
        futures: set[concurrent.futures.Future],
        executor: concurrent.futures.ThreadPoolExecutor,
    ) -> None:
        self._handle_alert_message(
            alert_update_consumer=alert_update_consumer, data_consumer=data_consumer
        )
        self._handle_data_messages(
            data_consumer=data_consumer, futures=futures, executor=executor
        )

    def _handle_alert_message(
        self, *, alert_update_consumer: Consumer, data_consumer: Consumer
    ) -> None:
        alert_message = self._get_message(
            consumer=alert_update_consumer, timeout=POLL_TIMEOUT
        )

        if alert_message is not None:
            debug(f"Received alert update: {alert_message.value()}")
            self._update_subscriptions(
                data_consumer=data_consumer, table_changes=self._get_topic_list()
            )

    def _handle_data_messages(
        self,
        *,
        data_consumer: Consumer,
        futures: set[concurrent.futures.Future],
        executor: concurrent.futures.ThreadPoolExecutor,
    ) -> None:
        data_messages = self._get_messages(consumer=data_consumer, timeout=POLL_TIMEOUT)

        for data_message in data_messages:
            self._process_data_message(
                data_message=data_message, futures=futures, executor=executor
            )

    def _process_data_message(
        self,
        *,
        data_message: Message,
        futures: set[concurrent.futures.Future],
        executor: concurrent.futures.ThreadPoolExecutor,
    ) -> None:
        active_alerts = queries.get_active_kafka_table_change_alerts() or []

        if (
            data_message is not None
            and not data_message.error()
            and data_message.value() is not None
        ):
            debug(
                f"Received data: {data_message.value().decode('utf-8')} from topic: {data_message.topic()}"
            )

            if associated_table_change := next(
                (
                    alert
                    for alert in active_alerts
                    if alert["topic"] == data_message.topic()
                ),
                None,
            ):
                debug(f"Table Change Alert found for topic {data_message.topic()}.")
                future = executor.submit(
                    self._process_message,
                    data_message,
                    associated_table_change,
                )
                futures.add(future)

    @staticmethod
    def _wait_for_futures_to_complete(
        *, futures: set[concurrent.futures.Future]
    ) -> None:
        if len(futures) >= 100:
            done, futures = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

        done, futures = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_COMPLETED
        )

        for future in done:
            try:
                future.result()
            except Exception as e:
                error(f"Error processing message: {e}", exc_info=True)

        futures = {f for f in futures if not f.done()}

    def _handle_exception(
        self,
        *,
        e: Exception,
        data_consumer: Consumer,
        alert_update_consumer: Consumer,
    ) -> None:
        if (
            isinstance(e, KafkaException)
            and e.args[0].code() == KafkaError._ALL_BROKERS_DOWN
        ):
            error("All brokers are down. Attempting to reconnect...", exc_info=True)
            data_consumer, alert_update_consumer = self._connect()
        else:
            error("An unexpected error occurred: ", exc_info=True)
            raise e
```