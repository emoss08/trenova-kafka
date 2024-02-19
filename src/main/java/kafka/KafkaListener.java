/*
 * COPYRIGHT(c) 2024 Trenova
 *
 * This file is part of Trenova.
 *
 * The Trenova software is licensed under the Business Source License 1.1. You are granted the right
 * to copy, modify, and redistribute the software, but only for non-production use or with a total
 * of less than three server instances. Starting from the Change Date (November 16, 2026), the
 * software will be made available under version 2 or later of the GNU General Public License.
 * If you use the software in violation of this license, your rights under the license will be
 * terminated automatically. The software is provided "as is," and the Licensor disclaims all
 * warranties and conditions. If you use this license's text or the "Business Source License" name
 * and trademark, you must comply with the Licensor's covenants, which include specifying the
 * Change License as the GPL Version 2.0 or a compatible license, specifying an Additional Use
 * Grant, and not modifying the license in any other way.
 */

package kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaListener.class);

    private static Properties properties = new Properties();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Consumer<String, String> consumer;
    private ExecutorService threadPool;

    public KafkaListener(int threadPoolSize) {
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
        Runtime.getRuntime().addShutdownHook(new Thread(this.shutdown));
    }

    static {
        loadProperties();
    }

    private static void loadProperties() {
        try {
            properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void listen() {
        connect();

        List<String> topics = getTopicList();
        if (topics.isEmpty()) {
            log.info("No topics to subscribe to");
            return;
        }

        consumer.subscribe(topics);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received message: " + record.value());
                }
            }
        } catch (WakeupException e) {
            if (!running.get()) {
                log.info("Shutting down Kafka listener....");
            }
        } catch (Exception e) {
            log.info("Error fetching messages: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void connect() {
        consumer = new KafkaConsumer<>(properties);
    }

    public static List<String> getTopicList() {
        List<Map<String, Object>> alerts = DatabaseUtils.getActiveAlerts();
        List<String> topics = new ArrayList<>();

        for (Map<String, Object> alert : alerts) {
            String topic = (String) alert.get("topic");
            if (topic != null && !topic.isEmpty()) {
                topics.add(topic);
            }
        }

        return topics;
    }

    private static List<ConsumerRecords<String, String>> getMessages(Consumer<String, String> consumer, long timeoutMs,
            int maxMessages) {
        List<ConsumerRecords<String, String>> validMessages = new ArrayList<>();
        try {
            while (validMessages.size() < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutMs));
                if (!records.isEmpty()) {
                    validMessages.add(records);
                }
                // Break the loop if no records are fetched
                if (records.count() == 0) {
                    break;
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up");
        } catch (Exception e) {
            log.info("Error fetching messages: " + e.getMessage());
        } finally {
            consumer.close();
        }
        return validMessages;
    }

    private Runnable shutdown = () -> {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        threadPool.shutdown();
    };

    public static void main(String[] args) {
        KafkaListener listener = new KafkaListener(10);
        listener.listen();
    }
}
