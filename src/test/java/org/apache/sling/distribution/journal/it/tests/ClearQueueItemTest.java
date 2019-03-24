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
package org.apache.sling.distribution.journal.it.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.ext.AfterOsgi;
import org.apache.sling.distribution.journal.it.ext.BeforeOsgi;
import org.apache.sling.distribution.journal.it.ext.ExtPaxExam;
import org.apache.sling.distribution.journal.it.kafka.KafkaLocal;

import com.google.protobuf.ByteString;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.queue.DistributionQueueEntry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.spi.PaxExamRuntime;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.ops4j.pax.exam.util.PathUtils;

import static org.apache.sling.distribution.journal.messages.Messages.*;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

/**
 * Starts one author instance and one publisher instances.
 * The author instance also runs the test code.
 * The test sends 10 invalid distribution package that fails importing on the publisher.
 * The test sends a clear command for the first 1 entry and checks that the
 * package was removed from the queue.
 * The test sends a clear command for all entries and checks that the queue is cleared.
 *
 * The test also covers the remove interface while the REMOVABLE capability is supported.
 */
@RunWith(ExtPaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ClearQueueItemTest extends DistributionTestSupport {

    private static final String PUB1_AGENT = "agent1";
    private static final String SUB1_AGENT = "subscriber-agent1";

    @Inject
    @Filter(value = "(name=agent1)", timeout = 40000L)
    DistributionAgent agent;

    @Inject
    MessagingProvider clientProvider;

    private static TestContainer publisher;
    private static KafkaLocal kafka;

    @Configuration
    public Option[] configuration() {
        return new Option[] { //
                //debug(),
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                        .put("whitelist.bypass", "true").asOption(),
                baseConfiguration(), //
                defaultOsgiConfigs(), //
                authorOsgiConfigs() //
        };
    }

    public Option debug() {
        return CoreOptions.vmOption("-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    }

    @BeforeOsgi
    public static void startPublisher() throws Exception {
        kafka = new KafkaLocal();
        DistributionTestSupport.createTopics();
        publisher = startPublisher(publisherConfig(8182,  "RemoveQueueItemTest.publisher1", SUB1_AGENT));
    }

    private static TestContainer startPublisher(Option[] config) {
        ExamSystem testSystem;
        try {
            testSystem = PaxExamRuntime.createTestSystem(config);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        TestContainer container = PaxExamRuntime.createContainer(testSystem);
        container.start();
        return container;
    }

    @AfterOsgi
    public static void stopPublishers() throws IOException {
        if (publisher != null) {
            publisher.stop();
        }
        closeQuietly(kafka);
    }

    private static Option[] publisherConfig(int httpPort, String instanceName, String agentName) {
        String workdir = String.format("%s/target/paxexam/%s", PathUtils.getBaseDir(), instanceName);
        return CoreOptions.options( //
                new DistributionTestSupport().withHttpPort(httpPort).baseConfiguration(workdir), //
                defaultOsgiConfigs(), //
                publishOsgiConfigs(agentName), //
                CoreOptions.workingDirectory(workdir)
        );
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testClearItems() throws Exception {

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(this::queueNames, containsInAnyOrder(containsString(SUB1_AGENT)));

        String subAgent1QueueName = agent.getQueueNames().iterator().next();

        testClearMethod(subAgent1QueueName);
        testRemoveMethod(subAgent1QueueName);

    }

    private void testClearMethod(String subAgent1QueueName)
            throws Exception {

        sendInvalidPackages(10);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueSize(subAgent1QueueName, 10));

        String headEntryId1 = agent.getQueue(subAgent1QueueName).getHead().getId();

        agent.getQueue(subAgent1QueueName).clear(1);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueSize(subAgent1QueueName, 9));

        String headEntryId2 = agent.getQueue(subAgent1QueueName).getHead().getId();
        Assert.assertNotEquals(headEntryId1, headEntryId2);

        agent.getQueue(subAgent1QueueName).clear(-1);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueEmpty(subAgent1QueueName));
    }

    private void testRemoveMethod(String subAgent1QueueName)
            throws Exception {

        /*
         * This test must be removed when the
         * REMOVABLE capability is removed
         */

        sendInvalidPackages(10);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueSize(subAgent1QueueName, 10));

        String headEntryId1 = agent.getQueue(subAgent1QueueName).getHead().getId();

        agent.getQueue(subAgent1QueueName).remove(headEntryId1);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueSize(subAgent1QueueName, 9));

        String headEntryId2 = agent.getQueue(subAgent1QueueName).getHead().getId();
        Assert.assertNotEquals(headEntryId1, headEntryId2);

        String tailEntry = lastEntry(agent.getQueue(subAgent1QueueName).getEntries(0, -1)).getId();
        Set<String> entryIds = new HashSet<>();
        entryIds.add(tailEntry);
        entryIds.add(headEntryId2);

        agent.getQueue(subAgent1QueueName).remove(entryIds);

        await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> queueEmpty(subAgent1QueueName));
    }

    private List<String> queueNames() {
        List<String> queueNames = new ArrayList<>();
        agent.getQueueNames().forEach(queueNames::add);
        return queueNames;
    }

    private boolean queueEmpty(String queueName) {
        return agent.getQueue(queueName).getStatus().isEmpty();
    }

    private boolean queueSize(String queueName, int expectedSize) {
        return agent.getQueue(queueName).getStatus().getItemsCount() == expectedSize;
    }

    private DistributionQueueEntry lastEntry(Iterable<DistributionQueueEntry> entries) {
        Iterator<DistributionQueueEntry> iterator = entries.iterator();
        DistributionQueueEntry last = null;
        for (; iterator.hasNext() ; ) {
            last = iterator.next();
        }
        return last;
    }

    private void sendInvalidPackages(int nb)
            throws Exception {
        MessageSender<PackageMessage> sender = clientProvider.createSender();
        for (int i = 0 ; i < nb ; i++) {
            sender.send(TOPIC_PACKAGE, newInvalidPackage(PUB1_AGENT));
        }
    }

    private PackageMessage newInvalidPackage(String agentId) throws IOException {

        final byte[] pkgBinary = new byte[2048];
        new Random().nextBytes(pkgBinary);
        final List<String> paths = Collections.singletonList("/content/invalid");
        final List<String> deepPaths = Collections.emptyList();
        final String pkgId = String.format("package-%s", UUID.randomUUID().toString());

        return PackageMessage.newBuilder()
                .setPubSlingId("slingid")
                .setPkgId(pkgId)
                .setPubAgentName(agentId)
                .setPkgBinary(ByteString.copyFrom(pkgBinary))
                .setPkgType("journal")
                .addAllPaths(paths)
                .setReqType(PackageMessage.ReqType.ADD)
                .addAllDeepPaths(deepPaths)
                .setPkgLength(pkgBinary.length)
                .build();
    }

}
