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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.Distributor;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.it.kafka.KafkaLocal;
import org.awaitility.Duration;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.spi.PaxExamRuntime;
import org.ops4j.pax.exam.util.Filter;
import org.ops4j.pax.exam.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributionTestBase extends DistributionTestSupport {
    protected static Logger LOG = LoggerFactory.getLogger(DistributionTestBase.class);

    private static KafkaLocal kafka;

    private static final String RESOURCE_TYPE = "sling:Folder";
    private static final String PUB1_AGENT = "agent1";

    @Inject
    @Filter(value = "(name=agent1)", timeout = 40000L)
    DistributionAgent agent;

    @Inject
    @Filter
    Distributor distributor;

    @Inject
    ResourceResolverFactory resourceResolverFactory;

    @Inject
    MessagingProvider clientProvider;


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

    public static void beforeOsgiBase() throws Exception {
        kafka = new KafkaLocal();
        DistributionTestSupport.createTopics();
    }

    public static void afterOsgiBase() {
        IOUtils.closeQuietly(kafka);
    }

    @Before
    public void beforeBase() {

    }

    @After
    public void afterBase() {

    }


    public static TestContainer startPublishInstance(int httpPort, String agentName, boolean editable, String stageAgentName) {
        ExamSystem testSystem;
        try {
            String workdir = String.format("%s/target/paxexam/%s", PathUtils.getBaseDir(), "publish-" + httpPort + "-" + UUID.randomUUID().toString());
            Option[] config = CoreOptions.options( //
                    new DistributionTestSupport().withHttpPort(httpPort).baseConfiguration(workdir), //
                    defaultOsgiConfigs(), //
                    publishOsgiConfigs(agentName, editable, stageAgentName), //
                    CoreOptions.workingDirectory(workdir)
            );

            testSystem = PaxExamRuntime.createTestSystem(config);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        TestContainer container = PaxExamRuntime.createContainer(testSystem);
        container.start();
        return container;
    }


    public void distribute(String path) {
        try (ResourceResolver resolver = createResolver()) {
            await().until(() -> tryDistribute(resolver, path), equalTo(true));
        }
    }

    private boolean tryDistribute(ResourceResolver resolver, String path) {
        DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, path);
        DistributionResponse response = distributor.distribute(PUB1_AGENT, resolver, request);
        LOG.info("Distribution for path {} ended with message: {} and status: {}", new Object[] { path,
                response.getMessage(), response.isSuccessful() });
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

    @SuppressWarnings({ "deprecation" })
    private ResourceResolver createResolver() {
        try {
            Map<String, Object> authinfo = new HashMap<String, Object>();
            return resourceResolverFactory.getAdministrativeResourceResolver(authinfo);
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createPath(String path) {
        try (ResourceResolver resolver = createResolver()){
            ResourceUtil.getOrCreateResource(resolver, path, RESOURCE_TYPE, RESOURCE_TYPE, true);
        } catch (Exception e) {
            LOG.error("cannot create path", e);
        }
    }

    private static int tryGetPath(int httpPort, String path) {
        String url = String.format("http://localhost:%s%s.json", httpPort, path);
        HttpGet httpGet = new HttpGet(url);
        Header authHeader = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            authHeader = new BasicScheme().authenticate(new UsernamePasswordCredentials("admin", "admin"), httpGet, null);
            httpGet.addHeader(authHeader);


            CloseableHttpResponse response = client.execute(httpGet);
            int status =  response.getStatusLine().getStatusCode();
            LOG.info("try get path {} with status {}", url, status);
            return status;

        } catch (Exception e) {
            LOG.error("cannot get path {}", url, e);
        }
        return  -1;
    }

    protected static void waitPath(int httpPort, String path) {
        await().atMost(30, TimeUnit.SECONDS)
            .until(() -> tryGetPath(httpPort, path), equalTo(200));
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Iterable<String> waitSubQueues(String... queues) {
        Matcher[] matchers = Stream.of(queues).map(q -> containsString(q)).toArray(Matcher[]::new);

        await().atMost(Duration.FIVE_MINUTES)
                .until(this::queueNames, containsInAnyOrder(matchers));

        Iterable<String> queueNames = agent.getQueueNames();
        LOG.info("Subscriber Queues: " + String.join(", ", queueNames));

        return queueNames;
    }

    protected void waitEmptySubQueues() {
        await().atMost(60, TimeUnit.SECONDS)
                .until(this::allQueuesEmpty, equalTo(true));
    }


    static protected void waitQueueItems(int httpPort, String agentName, int count) {
        await().atMost(Duration.FIVE_MINUTES)
                .until(() -> tryGetQueueItems(httpPort, agentName), equalTo(count));
        LOG.info("Items count {} for agent {}", count, agentName + "-" + httpPort);

    }

    static private int tryGetQueueItems(int httpPort, String agentName) {
        String url = String.format("http://localhost:%s/libs/sling/distribution/services/agents/%s/queues.2.json", httpPort, agentName);
        HttpGet httpGet = new HttpGet(url);
        Header authHeader = null;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            authHeader = new BasicScheme().authenticate(new UsernamePasswordCredentials("admin", "admin"), httpGet, null);
            httpGet.addHeader(authHeader);


            CloseableHttpResponse response = client.execute(httpGet);
            String text = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
            if (text == null) {
                return -1;
            }

            String itemsCount = StringUtils.substringBetween(text, "itemsCount\":", ",");
            if (itemsCount == null) {
                return -1;
            }

            return Integer.parseInt(itemsCount.trim());
        } catch (Throwable e) {
            LOG.error("cannot get items count {}", url, e);
        }
        return  -1;
    }


}
