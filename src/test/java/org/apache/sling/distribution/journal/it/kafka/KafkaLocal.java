/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.it.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.VerifiableProperties;
import scala.Some;
import scala.collection.Seq;

public class KafkaLocal implements Closeable {
    Logger LOG = LoggerFactory.getLogger(KafkaLocal.class);
    
    public KafkaServer kafka;
    public ZooKeeperLocal zookeeper;
    
    public KafkaLocal() throws Exception {
        this(kafkaProperties(), zookeeperProperties());
    }

    public KafkaLocal(Properties kafkaProperties, Properties zkProperties) throws Exception {
        zookeeper = new ZooKeeperLocal(zkProperties);
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        Seq<KafkaMetricsReporter> reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(kafkaProperties));
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        kafka = new KafkaServer(kafkaConfig, Time.SYSTEM, new Some<String>("kafka"), reporters);
        kafka.startup();
    }

    @Override
    public void close() throws IOException {
        System.out.println("stopping kafka...");
        try {
            kafka.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        zookeeper.close();
        System.out.println("done");
    }

    private static Properties kafkaProperties() {
        String logDir = "target/kafka-" + UUID.randomUUID().toString();
        Properties kafkaProps = new Properties();
        kafkaProps.put("zookeeper.connect", "localhost:2181");
        kafkaProps.put("advertised.host.name", "localhost");
        kafkaProps.put("host.name","localhost");
        kafkaProps.put("port", "9092");
        kafkaProps.put("auto.create.topics.enable", false);
        kafkaProps.put("broker.id", "0");
        kafkaProps.put("log.dir",logDir);
        kafkaProps.put("group.initial.rebalance.delay.ms", "0");
        kafkaProps.put("group.min.session.timeout.ms", "1000");
        return kafkaProps;
    }

    private static Properties zookeeperProperties() {
        Properties zkProps = new Properties();
        UUID uuid = UUID.randomUUID();
        zkProps.put("dataDir", "target/zookeeper/"+uuid.toString());
        zkProps.put("clientPort", "2181");
        return zkProps;
    }

}