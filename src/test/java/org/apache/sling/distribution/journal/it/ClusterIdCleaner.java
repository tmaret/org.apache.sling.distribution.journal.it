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
package org.apache.sling.distribution.journal.it;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class ClusterIdCleaner {
    private NodeStore store;
    
    public ClusterIdCleaner(File nodeStoreBase) throws InvalidFileStoreVersionException, IOException {
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(nodeStoreBase)
                .withStrictVersionCheck(true)
                .build();
        this.store = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    public ClusterIdCleaner(NodeStore store) {
        this.store = store;
    }
    
    public void deleteClusterId() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder clusterConfigNode = builder.getChildNode(
                ClusterRepositoryInfo.CLUSTER_CONFIG_NODE);
        
        if (!clusterConfigNode.exists()) {
            // if it doesn't exist, then there is no way to delete
            System.out.println("clusterId was never set or already deleted.");
            return;
        }
        
        if (!clusterConfigNode.hasProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP)) {
            // the config node exists, but the clusterId not 
            // so again, no way to delete
            System.out.println("clusterId was never set or already deleted.");
            return;
        }
        String oldClusterId = clusterConfigNode.getProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP)
                .getValue(Type.STRING);
        clusterConfigNode.removeProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP);
        try {
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            System.out.println("clusterId deleted successfully. (old id was " + oldClusterId + ")");
        } catch (CommitFailedException e) {
            System.err.println("Failed to delete clusterId due to exception: "+e.getMessage());
            e.printStackTrace();
        }
    }
}
