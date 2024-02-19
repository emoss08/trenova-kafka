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
import java.util.concurrent.CountDownLatch;
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
     * Initializes KafkaListener with specific thread pool configurations, sets up
     * Kafka consumers for
     * data and alert updates, and subscribes to initial topics.
     *
     * @param corePoolSize    the core number of threads in the pool.
     * @param maximumPoolSize the maximum number of threads in the pool.
     * @param keepAliveTime   the time (in seconds) that threads exceeding the core
     *                        pool size may remain
     *                        idle before being terminated.
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
     * Loads Kafka configuration properties from the specified properties file.
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
     * Subscribes the Kafka consumer to a list of topics specified for alert
     * updates.
     * This method ensures that the consumer listens to the correct alert update
     * topics from the start.
     */
    private void subscribeToAlertUpdateTopic() {
        String alertUpdateTopic = PROPERTIES.getProperty("alert.update.topic");
        alertUpdateConsumer.subscribe(List.of(alertUpdateTopic));
        LOG.debug("Subscribed to alert update topic: {}", alertUpdateTopic);
    }

    /**
     * Begins listening for messages on both data and alert update topics.
     * It initializes separate threads for handling data messages, alert updates,
     * and adjusting the thread pool size.
     */
    public void listen() {
        List<String> initialTopics = DatabaseUtils.getTopicList();
        if (!initialTopics.isEmpty()) {
            dataConsumer.subscribe(initialTopics);
            LOG.info("Data consumer initially subscribed to topics: {}", initialTopics);
        } else {
            LOG.warn("No topics available for initial subscription.");
        }

        new Thread(this::listenForDataMessages).start();
        new Thread(this::listenForAlertUpdates).start();
        new Thread(this::adjustThreadPoolSize).start();
    }

    /**
     * Dynamically adjusts the size of the thread pool based on the current size of
     * the subscription updates queue.
     * It increases the pool size when the queue size exceeds a high threshold and
     * decreases it when the queue size falls below a low threshold.
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
     * Continuously polls for data messages from Kafka, processes them, and commits
     * offsets after processing.
     * It ensures that all messages from a poll are processed before committing
     * their offsets for at-least-once delivery semantics.
     */
    private void listenForDataMessages() {
        try {
            while (running.get()) {
                List<String> newTopics = subscriptionUpdates.poll();
                if (newTopics != null) {
                    updateDataConsumerSubscriptions(newTopics);
                }

                ConsumerRecords<String, String> records = dataConsumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                if (!records.isEmpty()) {
                    final CountDownLatch latch = new CountDownLatch(records.count());

                    records.forEach(record -> {
                        threadPool.execute(() -> {
                            try {
                                // Process the record
                                LOG.info("Processing message: {}", record.value());
                                // TODO: Send the message to the notification service
                            } finally {
                                latch.countDown();
                            }
                        });
                    });

                    // Wait for all messages to be processed
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.error("Interrupted while waiting for message processing to complete", e);
                    }

                    // Commit offset after all records have been processed
                    dataConsumer.commitAsync();
                }
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
     * Listens for updates on the alert update topic and dynamically adjusts the
     * subscription of the data consumer.
     * When an alert update is received, it triggers a subscription update to
     * include any new topics of interest.
     */
    private void listenForAlertUpdates() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = alertUpdateConsumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                records.forEach(record -> {
                    LOG.debug("Received alert update: {}", record.value());
                    try {

                        // Update the subscription list for the data consumer
                        subscriptionUpdates.put(DatabaseUtils.getTopicList());

                        // Commit the offset after updating the subscription
                        alertUpdateConsumer.commitAsync();
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
     * Updates the subscription list of the data consumer to include the specified
     * topics.
     * This method allows for dynamic changes in the topics that the data consumer
     * is subscribed to.
     *
     * @param topics the list of topics to subscribe to.
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
     * Gracefully shuts down the KafkaListener, ensuring that all consumers are
     * closed and the thread pool is terminated.
     * It attempts to process all remaining messages and commits their offsets
     * before shutting down.
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