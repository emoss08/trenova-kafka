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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A KafkaListener listens to data and alert update topics on Kafka.
 * It dynamically adjusts its thread pool size based on the number of
 * subscription updates
 * and cleanly shuts down on application termination.
 */
public class KafkaListener {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaListener.class);
    private static final Properties PROPERTIES = new Properties();
    private static final String KAFKA_PROPERTIES_FILE = "src/main/resources/kafka.properties";
    private static final int POLL_DURATION_MS = 1000;
    private static final int QUEUE_SIZE_THRESHOLD_HIGH = 50;
    private static final int QUEUE_SIZE_THRESHOLD_LOW = 10;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Consumer<String, String> dataConsumer;
    private final Consumer<String, String> alertUpdateConsumer;
    private final ThreadPoolExecutor threadPool;
    private final BlockingQueue<List<String>> subscriptionUpdates = new LinkedBlockingQueue<>();

    /**
     * Constructs a KafkaListener with specified thread pool configurations.
     * Initializes Kafka consumers and subscribes to alert update topics.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even if
     *                        they are idle
     * @param maximumPoolSize the maximum number of threads to allow in the pool
     * @param keepAliveTime   when the number of threads is greater than the core,
     *                        this is the maximum time that excess idle threads will
     *                        wait for new tasks before terminating
     */
    public KafkaListener(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        loadProperties();
        this.threadPool = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        this.dataConsumer = new KafkaConsumer<>(PROPERTIES);
        this.alertUpdateConsumer = new KafkaConsumer<>(PROPERTIES);
        subscribeToAlertUpdateTopic();
    }

    /**
     * Loads Kafka configuration properties from a file.
     */
    private static void loadProperties() {
        try (FileInputStream fis = new FileInputStream(KAFKA_PROPERTIES_FILE)) {
            PROPERTIES.load(fis);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Could not load kafka properties", ex);
        }
    }

    /**
     * Subscribes the alert update consumer to the configured topic.
     */
    private void subscribeToAlertUpdateTopic() {
        String alertUpdateTopic = PROPERTIES.getProperty("alert.update.topic");
        alertUpdateConsumer.subscribe(List.of(alertUpdateTopic));
        LOG.debug("Subscribed to alert update topic: {}", alertUpdateTopic);
    }

    /**
     * Starts listening for messages on data and alert update topics in separate
     * threads.
     * Also starts a thread to adjust the thread pool size based on subscription
     * updates.
     */
    public void listen() {
        new Thread(this::listenForDataMessages).start();
        new Thread(this::listenForAlertUpdates).start();
        new Thread(this::adjustThreadPoolSize).start();
    }

    /**
     * Monitors the subscription update queue and adjusts the thread pool size
     * accordingly.
     */
    private void adjustThreadPoolSize() {
        while (running.get()) {
            try {
                int queueSize = subscriptionUpdates.size();
                if (queueSize > QUEUE_SIZE_THRESHOLD_HIGH
                        && threadPool.getCorePoolSize() < threadPool.getMaximumPoolSize()) {
                    threadPool.setCorePoolSize(threadPool.getCorePoolSize() + 1);
                } else if (queueSize < QUEUE_SIZE_THRESHOLD_LOW && threadPool.getCorePoolSize() > 1) {
                    threadPool.setCorePoolSize(threadPool.getCorePoolSize() - 1);
                }
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("ThreadPool adjustment thread interrupted", e);
            }
        }
    }

    /**
     * Listens for data messages on Kafka and processes them.
     * Automatically updates consumer subscriptions based on the alert update topic.
     */
    private void listenForDataMessages() {
        try {
            while (running.get()) {
                List<String> newTopics = subscriptionUpdates.poll();
                if (newTopics != null) {
                    updateDataConsumerSubscriptions(newTopics);
                }

                ConsumerRecords<String, String> records = dataConsumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                records.forEach(record -> LOG.info("Received message: {}", record.value()));
            }
        } catch (WakeupException e) {
            if (!running.get()) {
                LOG.info("Shutting down data message listener...");
            }
        } finally {
            dataConsumer.close();
        }
    }

    /**
     * Listens for alert updates on Kafka to adjust data consumer subscriptions
     * dynamically.
     */
    private void listenForAlertUpdates() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = alertUpdateConsumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                records.forEach(record -> {
                    LOG.debug("Received alert update: {}", record.value());
                    try {
                        subscriptionUpdates.put(DatabaseUtils.getTopicList());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.error("Failed to update topic subscriptions", e);
                    }
                });
            }
        } catch (WakeupException e) {
            if (!running.get()) {
                LOG.info("Shutting down alert update listener...");
            }
        } finally {
            alertUpdateConsumer.close();
        }
    }

    /**
     * Updates the subscriptions of the data consumer to the given list of topics.
     *
     * @param topics the list of topics to subscribe to
     */
    private void updateDataConsumerSubscriptions(List<String> topics) {
        if (!topics.isEmpty()) {
            dataConsumer.unsubscribe();
            dataConsumer.subscribe(topics);
            LOG.debug("Data consumer subscriptions updated to topics: {}", String.join(", ", topics));
        } else {
            LOG.debug("No active table changes to subscribe to.");
        }
    }

    /**
     * Initiates a graceful shutdown of the KafkaListener, including Kafka consumers
     * and thread pool.
     */
    private void shutdown() {
        running.set(false);
        dataConsumer.wakeup();
        alertUpdateConsumer.wakeup();
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
        LOG.info("KafkaListener shutdown completed.");
    }

    public static void main(String[] args) {
        KafkaListener listener = new KafkaListener(1, 10, 60);
        listener.listen();
    }
}