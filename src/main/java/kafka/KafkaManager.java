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
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicListing;

public class KafkaManager {
    public static void main(String[] args) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("src/main/resources/kafka.properties"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void printTopicDetails(Properties properties) {
        Collection<TopicListing> topics;

        try (AdminClient client = AdminClient.create(properties)) {
            topics = getTopicListing(client, true);
            topics.forEach(
                    topic -> System.out.println("Topic: " + topic.name() + " isInternal: " + topic.isInternal()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static Collection<TopicListing> getTopicListing(AdminClient client, boolean isInternal)
            throws InterruptedException, ExecutionException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(isInternal);
        return client.listTopics(options).listings().get();
    }
}
