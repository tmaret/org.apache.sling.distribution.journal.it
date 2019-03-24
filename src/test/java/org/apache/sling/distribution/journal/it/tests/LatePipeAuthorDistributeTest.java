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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeTrue;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.IOException;
import java.util.HashMap;
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
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.kafka.PaxExamWithKafka;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;

/**
 * Test for the journal coming up after the start of an author instance
 * and the journal being stopped and restarted when author instance already runs.
 */
@RunWith(PaxExamWithKafka.class)
@ExamReactorStrategy(PerClass.class)
public class LatePipeAuthorDistributeTest extends DistributionTestSupport {
    private static final String PUB1_AGENT = "agent1";

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    Distributor distributor;

    @Inject
    ResourceResolverFactory resourceResolverFactory;
    
    @Inject
    MessagingProvider clientProvider;

    private ToxiproxyClient proxyClient;
    
    @Configuration
    public Option[] configuration() {
        return new Option[] { //
                //debug(),
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                        .put("whitelist.bypass", "true").asOption(),
                baseConfiguration(), //
                mvn("com.google.code.gson", "gson"),
                mvn("eu.rekawek.toxiproxy", "toxiproxy-java"),
                defaultOsgiConfigs("http://localhost:8083"), //
                authorOsgiConfigs() //
        };
    }

    public Option debug() {
        return CoreOptions.vmOption("-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    }
    
    @Before
    public void before() {
        proxyClient = new ToxiproxyClient();
        try {
            proxyClient.getProxies();
            deleteProxy();
        } catch (IOException e) {
            assumeTrue("Toxiproxy server not present. Ignoring", false);
        }
        
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDistribute() throws Exception {
        Map<String, Object> authinfo = new HashMap<String, Object>();
        try (ResourceResolver resolver = resourceResolverFactory.getAdministrativeResourceResolver(authinfo)) {
            await().until( () -> bundleContext.getServiceReference(JournalAvailable.class), nullValue());
            DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, "/");
            
            // Fail as journal is not present
            DistributionResponse response = distributor.distribute(PUB1_AGENT, resolver, request);
            Assert.assertFalse(response.getMessage(), response.isSuccessful());

            // Succeed as journal is present over proxy
            proxyClient.createProxy("journal", "localhost:8083", "localhost:8082");
            log.info("Created proxy for journal");
            await().until( () -> bundleContext.getServiceReference(JournalAvailable.class), notNullValue());
            await().atMost(30, SECONDS).until(() -> distributor.distribute(PUB1_AGENT, resolver, request), 
                    hasProperty("successful", equalTo(true)));
            
            // After deleting the proxy distribute fails again
            deleteProxy();
            log.info("Deleted proxy for journal");
            await().atMost(30, TimeUnit.SECONDS).until(() -> distributor.distribute(PUB1_AGENT, resolver, request), 
                    hasProperty("successful", equalTo(false)));
            await().until( () -> bundleContext.getServiceReference(JournalAvailable.class), nullValue());
        } finally {
            deleteProxy();
        }
    }

    private void deleteProxy() throws IOException {
        Proxy proxy = proxyClient.getProxyOrNull("journal");
        if (proxy != null) {
            proxy.delete();
        }
    }

}
