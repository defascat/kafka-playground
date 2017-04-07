package org.defascat.playground.kafka.topics;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

/**
 *
 * @author apanasyuk
 */
public class TopicCreator {
    public static void main(String[] args) {
        String zookeeperConnect = "localhost:2181";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
        
        final ExecutorService pool = Executors.newFixedThreadPool(10);
        String topic = "test-topic-";
        Properties topicConfig = new Properties();
        for (int i = 100_000; i < 1_000_000; i++) {
            final String topicName = topic + i;
            pool.execute(() -> {
                AdminUtils.createTopic(zkUtils, topicName, 1, 1, topicConfig, RackAwareMode.Disabled$.MODULE$);
                System.err.println("Topic " + topicName + " created.");
            });
        }
        pool.shutdown();
        while (!pool.isShutdown()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(TopicCreator.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        // zkClient.close();

    }
}
