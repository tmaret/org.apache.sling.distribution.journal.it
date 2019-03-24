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

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.sling.distribution.journal.HandlerAdapter.create;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.it.ClusterIdCleaner;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.FileUtil;
import org.apache.sling.distribution.journal.it.kafka.KafkaLocal;
import org.apache.sling.distribution.journal.messages.Messages.DiscoveryMessage;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.Messages.SubscriberState;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.ExamSystem;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.TestContainer;
import org.ops4j.pax.exam.spi.PaxExamRuntime;
import org.ops4j.pax.exam.util.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * - Start a cluster with 1 author and 1 publisher
 * - Distribute one package
 * - Stop and clone publisher
 * - Start 2 publishers with same repo state
 * - Distribute another package
 * - Check both have processed both packages
 */
public class ScaleUpTest {
    private static final String AUTHOR = "CloneInstanceTest.author";
    private static final String PUBLISHER1 = "CloneInstanceTest.publisher1";
    private static final String PUBLISHER2 = "CloneInstanceTest.publisher2";

    private static final String SUB1_AGENT = "subscriber1-agent1";
    private static final String SUB2_AGENT = "subscriber2-agent1";

    private static TestContainer author;
    private static TestContainer publisher1;
    private static TestContainer publisher2;
    
    private static Logger LOG = LoggerFactory.getLogger(ScaleUpTest.class);
    private MessagingProvider provider;
    private volatile Semaphore packageReceived = new Semaphore(0);
    private volatile Semaphore discoveryReceived = new Semaphore(0);
    private long lastPackageOffset;
    private Closeable packagePoller;
    private Closeable discoveryPoller;
    private volatile Map<String, Long> processedOffsets = new ConcurrentHashMap<>();
    private KafkaLocal kafka;

    @Before
    public void before() throws Exception {
        kafka = new KafkaLocal();
        DistributionTestSupport.createTopics();
        this.provider = DistributionTestSupport.createProvider();
        Topics topics = new Topics();
        packagePoller = this.provider.createPoller(topics.getPackageTopic(), Reset.earliest, create(PackageMessage.class, this::handle));
        discoveryPoller = this.provider.createPoller(topics.getDiscoveryTopic(), Reset.earliest, create(DiscoveryMessage.class, this::handleDiscovery));
    }

    @Ignore
    @Test
    public void scaleUp() throws Exception {
        author = startContainer(authorConfig(8182, AUTHOR));
        publisher1 = startContainer(publisherConfig(8183, PUBLISHER1, SUB1_AGENT));
        long offset1 = distribute();
        await().until(this::numDiscoveredPublishers, equalTo(1));
        await().atMost(60, SECONDS).until(() -> allProcessed(offset1));

        Path sourceRepo = Paths.get(workDir(PUBLISHER1) + "-repo");
        Path destRepo = Paths.get(workDir(PUBLISHER2) + "-repo");
        await().atMost(60, SECONDS).until(() -> sourceRepo.toFile().exists());
        publisher1.stop();
        publisher1 = null;
        this.processedOffsets.clear();
        clone(sourceRepo, destRepo);
        
        publisher1 = startContainer(publisherConfig(8183, PUBLISHER1, SUB1_AGENT));
        publisher2 = startContainer(publisherConfig(8184, PUBLISHER2, SUB2_AGENT));
        await().until(this::numDiscoveredPublishers, equalTo(2));
        await().atMost(60, SECONDS).until(() -> allProcessed(offset1));
        long offset2 = distribute();
        await().atMost(60, SECONDS).until(() -> allProcessed(offset2));
    }

    @After
    public void after() {
        closeQuietly(packagePoller, discoveryPoller);
        stopContainer(publisher1);
        stopContainer(publisher2);
        stopContainer(author);
        closeQuietly(kafka);
    }

    private int numDiscoveredPublishers() {
        return processedOffsets.keySet().size();
    }
     
    private boolean allProcessed(long offset) {
        boolean allMatch = processedOffsets.values().stream().allMatch(processedOffset -> processedOffset >= offset);
        return allMatch;
    }

    private void clone(Path sourceRepo, Path destRepo) throws IOException, InvalidFileStoreVersionException {
        FileUtil.deleteDir(destRepo.toFile());
        FileUtil.copyFolder(sourceRepo, destRepo);
        new ClusterIdCleaner(destRepo.toFile()).deleteClusterId();
    }

    private void tryAcquire(Semaphore sem) throws InterruptedException {
        assertTrue(sem.tryAcquire(10, TimeUnit.SECONDS));
    }
    

    
    private long distribute() throws Exception {
        CloseableHttpClient client = HttpClients.createDefault();
        createNode(client, "/content");
        distributeNode(client, "/content");
        client.close();
        tryAcquire(packageReceived);
        return lastPackageOffset;
    }
    
    private void createNode(CloseableHttpClient client, String path)
            throws UnsupportedEncodingException, IOException, ClientProtocolException {
        HttpPost httpPost = new HttpPost("http://localhost:8182" + path);
        httpPost.setEntity(new UrlEncodedFormEntity(asList(new BasicNameValuePair("jcr:primaryType","sling:Folder"))));
        httpPost.addHeader("content-type", "application/x-www-form-urlencoded");
        CloseableHttpResponse response = client.execute(httpPost);
        LOG.info(response.getStatusLine().getReasonPhrase());
        assertThat(response.getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_OK));
    }

    private void distributeNode(CloseableHttpClient client, String path)
            throws UnsupportedEncodingException, AuthenticationException, IOException, ClientProtocolException {
        HttpPost httpPost = new HttpPost("http://localhost:8182/libs/sling/distribution/services/agents/agent1");
        List<BasicNameValuePair> list = Arrays.asList(new BasicNameValuePair("action", "ADD"), new BasicNameValuePair("path", path));
        httpPost.setEntity(new UrlEncodedFormEntity(list));
        addAuthHEader(httpPost);
        httpPost.addHeader("content-type", "application/x-www-form-urlencoded");
     
        CloseableHttpResponse response = client.execute(httpPost);
        LOG.info(response.getStatusLine().getReasonPhrase());
        assertThat(response.getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_ACCEPTED));
    }

    private void addAuthHEader(HttpPost httpPost) throws AuthenticationException {
        httpPost.addHeader(new BasicScheme().authenticate(new UsernamePasswordCredentials("admin", "admin"), httpPost, null));
    }
    
    private void stopContainer(TestContainer container) {
        if (container != null) {
            container.stop();
        }
    }

    
    private static TestContainer startContainer(Option[] config) {
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
    
    private Option[] authorConfig(int httpPort, String instanceName) {
        return new Option[] { //
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                .put("whitelist.bypass", "true").asOption(),
                new DistributionTestSupport().withHttpPort(httpPort).baseConfiguration(), //
                DistributionTestSupport.defaultOsgiConfigs(), //
                DistributionTestSupport.authorOsgiConfigs() //
        };
    }

    private Option[] publisherConfig(int httpPort, String instanceName, String agentName) {
        String workdir = workDir(instanceName);
        FileUtil.deleteDir(new File(workdir));
        return CoreOptions.options( //
                new DistributionTestSupport().withHttpPort(httpPort).baseConfiguration(workdir +"-repo"), //
                DistributionTestSupport.defaultOsgiConfigs(), //
                DistributionTestSupport.publishOsgiConfigs(agentName), //
                CoreOptions.workingDirectory(workdir)
        );
    }

    private String workDir(String instanceName) {
        return String.format("%s/target/paxexam/%s", PathUtils.getBaseDir(), instanceName);
    }
    
    private void handleDiscovery(MessageInfo info, DiscoveryMessage message) {
        List<SubscriberState> stateList = message.getSubscriberStateList();
        String slingId = message.getSubSlingId();
        OptionalLong minOffset = stateList.stream().mapToLong(state -> state.getOffset()).min();
        LOG.info("DiscoveryMessage slingid {} received {} states {}", slingId, minOffset, stateList);
        processedOffsets.put(slingId, minOffset.orElseGet(() -> 0l));
        discoveryReceived .release();
    }
    
    private void handle(MessageInfo info, PackageMessage message) {
        if (message.getReqType() == PackageMessage.ReqType.TEST) {
            return;
        }
        LOG.info("PackageMessage received {}, paths {}", info.getOffset(), message.getPathsList());
        this.lastPackageOffset = info.getOffset();
        packageReceived.release();
    }
    
}
