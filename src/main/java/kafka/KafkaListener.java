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

public class KafkaListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaListener.class);
    private static Properties properties = new Properties();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Consumer<String, String> dataConsumer;
    private Consumer<String, String> alertUpdateConsumer;
    private ThreadPoolExecutor threadPool;
    private BlockingQueue<List<String>> subscriptionUpdates = new LinkedBlockingQueue<>();

    public KafkaListener(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        loadProperties();
        this.threadPool = new ThreadPoolExecutor(
                corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        this.dataConsumer = new KafkaConsumer<>(properties);
        this.alertUpdateConsumer = new KafkaConsumer<>(properties);
        subscribeToAlertUpdateTopic();
    }

    private static void loadProperties() {
        try {
            properties.load(new FileInputStream("src/main/resources/kafka.properties"));
        } catch (IOException ex) {
            log.error("Failed to load properties", ex);
        }
    }

    private void subscribeToAlertUpdateTopic() {
        String alertUpdateTopic = properties.getProperty("alert.update.topic");
        alertUpdateConsumer.subscribe(List.of(alertUpdateTopic));
        log.debug("Subscribed to alert update topic: {}", alertUpdateTopic);
    }

    public void listen() {
        new Thread(this::listenForDataMessages).start();
        new Thread(this::listenForAlertUpdates).start();
        new Thread(this::adjustThreadPoolSize).start();
    }

    private void adjustThreadPoolSize() {
        while (running.get()) {
            try {
                int queueSize = subscriptionUpdates.size();
                if (queueSize > 50 && threadPool.getCorePoolSize() < threadPool.getMaximumPoolSize()) {
                    threadPool.setCorePoolSize(threadPool.getCorePoolSize() + 1);
                } else if (queueSize < 10 && threadPool.getCorePoolSize() > 1) {
                    threadPool.setCorePoolSize(threadPool.getCorePoolSize() - 1);
                }
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("ThreadPool adjustment thread interrupted", e);
            }
        }
    }

    private void listenForDataMessages() {
        try {
            updateDataConsumerSubscriptions(DatabaseUtils.getTopicList());

            while (running.get()) {
                List<String> newTopics = subscriptionUpdates.poll();
                if (newTopics != null) {
                    updateDataConsumerSubscriptions(newTopics);
                }

                ConsumerRecords<String, String> records = dataConsumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> log.info("Received message: {}", record.value()));
            }
        } catch (WakeupException e) {
            if (!running.get()) {
                log.info("Shutting down data message listener...");
            }
        } finally {
            dataConsumer.close();
        }
    }

    private void listenForAlertUpdates() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = alertUpdateConsumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    log.debug("Received alert update: {}", record.value());
                    try {
                        subscriptionUpdates.put(DatabaseUtils.getTopicList());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Failed to update topic subscriptions", e);
                    }
                });
            }
        } catch (WakeupException e) {
            if (!running.get()) {
                log.info("Shutting down alert update listener...");
            }
        } finally {
            alertUpdateConsumer.close();
        }
    }

    private void updateDataConsumerSubscriptions(List<String> topics) {
        if (!topics.isEmpty()) {
            dataConsumer.unsubscribe();
            dataConsumer.subscribe(topics);
            log.debug("Data consumer subscriptions updated to topics: {}", String.join(", ", topics));
        } else {
            log.debug("No active table changes to subscribe to.");
        }
    }

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
        log.info("KafkaListener shutdown completed.");
    }

    public static void main(String[] args) {
        KafkaListener listener = new KafkaListener(1, 10, 60);
        listener.listen();
    }
}