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

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;
import static org.osgi.util.converter.Converters.standardConverter;

import java.util.Dictionary;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.shared.JournalAvailableChecker;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.impl.shared.Topics.TopicsConfiguration;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.kafka.KafkaLocal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

/**
 * Check that JournalAvailableChecker correctly tests for presence of the journal and topics
 */
public class JournalAvailableTest {
    
    @Spy
    private MessagingProvider provider;

    @Spy
    Topics topics;
    
    @InjectMocks
    JournalAvailableChecker checker;

    @Mock
    private BundleContext context;

    private KafkaLocal kafka;
    
    @Before
    public void before() throws Exception {
        kafka = new KafkaLocal();
        DistributionTestSupport.createTopics();
        this.provider = DistributionTestSupport.createProvider();
        MockitoAnnotations.initMocks(this);
        topics.activate(topicsConfiguration(singletonMap("packageTopic", "topic_does_not_exist")));
    }

    @After
    public void after() {
        closeQuietly(kafka);
    }
   
    @Test
    public void test() throws Exception {
        mockServiceReg();
        Callable<Boolean> isAvailable = () -> { checker.run(); return checker.isAvailable(); };
        checker.activate(context);
        new Thread(checker).start();
        // Wait a bit as the journal backend from previous test might still be up
        await().atMost(30, TimeUnit.SECONDS).until(isAvailable, equalTo(false));

        topics.activate(topicsConfiguration(emptyMap()));

        await().atMost(30, TimeUnit.SECONDS).until(isAvailable, equalTo(true));

        IOUtils.closeQuietly(kafka);
        await().atMost(60, TimeUnit.SECONDS).until(isAvailable, equalTo(false));

    }

    @SuppressWarnings("unchecked")
    private void mockServiceReg() {
        ServiceRegistration<JournalAvailable> reg = Mockito.mock(ServiceRegistration.class);
        when(context.registerService(Mockito.eq(JournalAvailable.class), Mockito.eq(checker), Mockito.isNull(Dictionary.class))).thenReturn(reg);
    }

    private TopicsConfiguration topicsConfiguration(Map<String,String> props) {
        return standardConverter()
                .convert(props)
                .to(TopicsConfiguration.class);
    }

}
