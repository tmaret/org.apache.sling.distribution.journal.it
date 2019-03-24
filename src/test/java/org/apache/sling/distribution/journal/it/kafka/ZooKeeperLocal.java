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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperLocal implements Closeable {
    static Logger LOG = LoggerFactory.getLogger(ZooKeeperLocal.class);
    MyZooKeeperServerMain zooKeeperServer;

    public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException, ConfigException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        quorumConfiguration.parseProperties(zkProperties);
        zooKeeperServer = new MyZooKeeperServerMain(quorumConfiguration);

        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.startup();
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }

    @Override
    public void close() throws IOException {
        zooKeeperServer.shutdown();
    }
    
    static class MyZooKeeperServerMain extends ZooKeeperServerMain {

        private QuorumPeerConfig config;

        MyZooKeeperServerMain(QuorumPeerConfig config) {
            this.config = config;
        }

        public void startup() throws IOException {
            ServerConfig serverConfig = new ServerConfig();
            serverConfig.readFrom(config);
            runFromConfig(serverConfig);
        }

        public void shutdown() {
            try {
                super.shutdown();
            } catch (Exception e) {
                LOG.error("Error shutting down ZooKeeper", e);
            }
        }
    }
}