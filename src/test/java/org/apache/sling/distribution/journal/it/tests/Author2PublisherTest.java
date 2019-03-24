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

import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.Distributor;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.ext.AfterOsgi;
import org.apache.sling.distribution.journal.it.ext.BeforeOsgi;
import org.apache.sling.distribution.journal.it.ext.ExtPaxExam;
import org.apache.sling.distribution.journal.it.kafka.KafkaLocal;

/**
 * Starts one author instance and two publisher instances.
 * The author instance also runs the test code.
 * The test triggers a content distribution and checks it is correctly processed by
 * both publish instances.
 */
@RunWith(ExtPaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class Author2PublisherTest extends DistributionTestSupport {
    private static final String PUB1_AGENT = "agent1";
    private static final String SUB1_AGENT = "subscriber-agent1";
    private static final String SUB2_AGENT = "subscriber-agent2";

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    @Filter(value = "(name=agent1)", timeout = 40000L)
    DistributionAgent agent;
    
    @Inject
    @Filter
    Distributor distributor;

    @Inject
    @Filter
    ResourceResolverFactory resourceResolverFactory;
    
    @Inject
    MessagingProvider clientProvider;
    
    private static TestContainer publisher;

    private static TestContainer publisher2;
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
    public static void startPublishers() throws Exception {
        kafka = new KafkaLocal();
        DistributionTestSupport.createTopics();
        publisher = startPublisher(publisherConfig(8182,  "Author2PublisherTest.publisher1", SUB1_AGENT));
        publisher2 = startPublisher(publisherConfig(8183, "Author2PublisherTest.publisher2", SUB2_AGENT));
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
        //new Thread(container::start).start();
        return container;
    }
    
    @AfterOsgi
    public static void stopPublishers() throws IOException {
        if (publisher != null) {
            publisher.stop();
        }
        if (publisher2 != null) {
            publisher2.stop();
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

    @SuppressWarnings({ "deprecation", "unchecked" })
    @Test
    public void testDistribute() throws Exception {
        Map<String, Object> authinfo = new HashMap<String, Object>();
        try (ResourceResolver resolver = resourceResolverFactory.getAdministrativeResourceResolver(authinfo)) {
            await().until(() -> distribute(resolver), equalTo(true));
        }
        await()
            .atMost(30, TimeUnit.SECONDS)
            .until(this::queueNames, containsInAnyOrder(containsString(SUB1_AGENT),  containsString(SUB2_AGENT)));
        await()
            .atMost(30, TimeUnit.SECONDS)
            .until(this::allQueuesEmpty, equalTo(true));
        System.out.println("Queuenames " + agent.getQueueNames());
    }
    
    private boolean distribute(ResourceResolver resolver) {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/");
        DistributionResponse response = distributor.distribute(PUB1_AGENT, resolver, request);
        log.info(response.getMessage());
        return response.isSuccessful();
    }

    private List<String> queueNames() {
        List<String> queueNames = new ArrayList<>();
        agent.getQueueNames().forEach(queueNames::add);
        return queueNames;
    }
    
    private boolean allQueuesEmpty() {
        return queueNames().stream().allMatch(this::queueEmpty);
    }
    
    private boolean queueEmpty(String queueName) {
        return agent.getQueue(queueName).getStatus().getItemsCount() == 0;
    }
}
