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


import static java.lang.Boolean.getBoolean;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.admin.AdminClient.create;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.sling.testing.paxexam.SlingOptions.slingDistribution;
import static org.apache.sling.testing.paxexam.SlingOptions.slingQuickstartOak;
import static org.ops4j.pax.exam.Constants.START_LEVEL_SYSTEM_BUNDLES;
import static org.ops4j.pax.exam.CoreOptions.bootDelegationPackage;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.composite;
import static org.ops4j.pax.exam.CoreOptions.frameworkStartLevel;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.systemProperty;
import static org.ops4j.pax.exam.CoreOptions.url;
import static org.ops4j.pax.exam.CoreOptions.vmOption;
import static org.ops4j.pax.exam.CoreOptions.when;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.factoryConfiguration;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;
import static org.osgi.util.converter.Converters.standardConverter;

import java.io.File;
import java.util.Map;

import javax.inject.Inject;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.kafka.KafkaClientProvider;
import org.apache.sling.distribution.journal.kafka.KafkaEndpoint;
import org.apache.sling.distribution.serialization.impl.vlt.VaultDistributionPackageBuilderFactory;
import org.apache.sling.testing.paxexam.SlingOptions;
import org.apache.sling.testing.paxexam.TestSupport;
import org.ops4j.pax.exam.ConfigurationManager;
import org.ops4j.pax.exam.Constants;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.DefaultCompositeOption;
import org.ops4j.pax.exam.options.libraries.JUnitBundlesOption;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationAdmin;

import com.google.common.collect.ImmutableMap;

public class DistributionTestSupport extends TestSupport {
    public static final String TOPIC_PACKAGE = "aemdistribution_package";
    public static final String TOPIC_DISCOVERY = "aemdistribution_discovery";
    public static final String TOPIC_COMMAND = "aemdistribution_command";
    public static final String TOPIC_STATUS = "aemdistribution_status";
    public static final String TOPIC_EVENT = "aemdistribution_event";
    
    @Inject
    protected BundleContext bundleContext;

    @Inject
    protected ConfigurationAdmin configAdmin;

    private int httpPort = 8181;

    public DistributionTestSupport withHttpPort(int httpPort) {
        this.httpPort = httpPort;
        return this;
    }
    
    public Option baseConfiguration() {
        String workingDirectory = workingDirectory();
        FileUtil.deleteDir(new File(workingDirectory));
        return baseConfiguration(workingDirectory);
    }

    public Option baseConfiguration(String baseDirectory) {
        // Patch versions of features provided by SlingOptions
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.commons.mime");
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.commons.metrics");
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.distribution.core");
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.distribution.journal");
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.distribution.journal.messages");
        SlingOptions.versionResolver.setVersionFromProject("org.apache.sling", "org.apache.sling.distribution.journal.kafka");
        SlingOptions.versionResolver.setVersionFromProject("io.dropwizard.metrics", "metrics-core");
        SlingOptions.versionResolver.setVersion("org.slf4j", "log4j-over-slf4j", "1.7.6");

        Option baseOptions = composite(
                super.baseConfiguration(),
                SlingOptions.logback(),
                mavenBundle().groupId("org.slf4j").artifactId("log4j-over-slf4j").version(SlingOptions.versionResolver),
                
                // The base sling Quickstart
                slingQuickstart(baseDirectory),
                mavenBundle().groupId("org.apache.felix").artifactId("org.apache.felix.webconsole.plugins.ds").version(SlingOptions.versionResolver),
                mavenBundle().groupId("org.apache.sling").artifactId("org.apache.sling.commons.metrics").version(SlingOptions.versionResolver),

                mvn("com.google.protobuf", "protobuf-java"),
                kafka(),

                // The bundle built (org.apache.sling.distribution.journal)
                mvn("org.apache.sling", "org.apache.sling.distribution.journal"),
                mvn("org.apache.sling", "org.apache.sling.distribution.journal.messages"),
                mvn("org.apache.sling", "org.apache.sling.distribution.journal.kafka"),

                // distribution bundles
                slingDistribution(),

                // testing
                //slingResourcePresence(), // see https://github.com/apache/sling-org-apache-sling-resource-presence
                jsoup(),
                myJunitBundles()

        );

        // Remote debugger on the forked JVM
        // Run with mvn install -DisDebugEnabled=true
        return getBoolean("isDebugEnabled")
                ? composite(remoteDebug(), baseOptions)
                : baseOptions;
    }

    public static CompositeOption myJunitBundles() {
        return new DefaultCompositeOption(
                composite(defaultTestSystemOptions()),
                new JUnitBundlesOption(), 
                systemProperty("pax.exam.invoker").value("junit"),
                mvn("org.mockito", "mockito-all"),
                mvn("org.apache.servicemix.bundles", "org.apache.servicemix.bundles.hamcrest"),
                mvn("org.awaitility", "awaitility"),
                bundle("link:classpath:META-INF/links/org.ops4j.pax.exam.invoker.junit.link"));
    }
    
    /**
     * Dependencies for mockito 2
     */
    public static Option mockito2() {
        return composite(
                mvn("org.objenesis", "objenesis"),
                mvn("net.bytebuddy", "byte-buddy"),
                mvn("net.bytebuddy", "byte-buddy-agent"),
                mvn("org.mockito", "mockito-core")
                );
    }
    
    /**
     * Standard test system options with just at inject removed as it collides with
     * the one provided in sling 
     */
    private static Option[] defaultTestSystemOptions() {
        ConfigurationManager cm = new ConfigurationManager();
        String logging = cm.getProperty(Constants.EXAM_LOGGING_KEY,
            Constants.EXAM_LOGGING_PAX_LOGGING);

        return new Option[] {
            bootDelegationPackage("sun.*"),
            frameworkStartLevel(Constants.START_LEVEL_TEST_BUNDLE),
            url("link:classpath:META-INF/links/org.ops4j.pax.exam.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.exam.inject.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.extender.service.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.osgi.compendium.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),

            when(logging.equals(Constants.EXAM_LOGGING_PAX_LOGGING)).useOptions(
                url("link:classpath:META-INF/links/org.ops4j.pax.logging.api.link").startLevel(
                    START_LEVEL_SYSTEM_BUNDLES)),

            url("link:classpath:META-INF/links/org.ops4j.base.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.swissbox.core.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.swissbox.extender.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.swissbox.framework.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.swissbox.lifecycle.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            url("link:classpath:META-INF/links/org.ops4j.pax.swissbox.tracker.link").startLevel(
                START_LEVEL_SYSTEM_BUNDLES),
            /*
            url("link:classpath:META-INF/links/org.apache.geronimo.specs.atinject.link")
                .startLevel(START_LEVEL_SYSTEM_BUNDLES) };
                */
        };
    }
    
    protected Option slingQuickstart(String baseDirectory) {
        final String workingDirectory = String.format("%s/instance", baseDirectory);
        final String dataStoreDirectory = String.format("%s/shareddatastore", baseDirectory);
        System.out.println(String.format("Quickstart with workingDirectory %s, port %s", workingDirectory, httpPort));
        return slingQuickstartOakSharedBlobstore(workingDirectory, dataStoreDirectory, httpPort);
    }
    
    public static Option defaultOsgiConfigs() {
        return defaultOsgiConfigs("");
    }

    /**
     * OSGI configurations targeted to author and publish instances
     */
    protected static Option defaultOsgiConfigs(String journalEndpoint) {
        return composite(
                newConfiguration("org.apache.sling.jcr.resource.internal.JcrSystemUserValidator")
                    .put("allow.only.system.user", false).asOption(),
                    
                // For production the users would be: replication-service,content-writer-service
                factoryConfiguration("org.apache.sling.serviceusermapping.impl.ServiceUserMapperImpl.amended")
                        .put("user.mapping", new String[]{"org.apache.sling.distribution.journal:bookkeeper=admin","org.apache.sling.distribution.journal:importer=admin"})
                        .asOption(),

                factoryConfiguration(VaultDistributionPackageBuilderFactory.class.getName())
                        .put("name", "journal")
                        .put("type", "inmemory")
                        .put("useBinaryReferences", "true")
                        .put("aclHandling", "IGNORE")
                        .put("package.filters", new String[]{"/home/users|-.*/.tokens", "/home/users|-.*/rep:cache"})
                        .put("property.filters", new String[]{"/|-^.*/cq:lastReplicated|-^.*/cq:lastReplicatedBy|-^.*/cq:lastReplicationAction"})
                        .asOption(),

                newConfiguration("org.apache.sling.distribution.journal.kafka.KafkaClientProvider")
                    .asOption(),

                newConfiguration("org.apache.sling.distribution.component.impl.DistributionComponentFactoryMap")
                        .put("mapping.agent", new String[]{//
                                "pub:org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory"})
                        .asOption(),

                newConfiguration("org.apache.sling.distribution.journal.impl.shared.JournalAvailableChecker")
                        .put("scheduler.period", 1L)
                        .asOption()

        );

    }

    /**
     * OSGI configurations targeted to the author instances only
     */
    public static Option authorOsgiConfigs() {
        return composite(
                factoryConfiguration("org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory")
                        .put("name", "agent1")
                        .put("packageBuilder.target", "(name=journal)")
                        .asOption(),
                factoryConfiguration("org.apache.sling.distribution.resources.impl.DistributionServiceResourceProviderFactory")
                    .put("kind", "agent")
                    .put("provider.roots", "/libs/sling/distribution/services/agents")
                    .asOption(),
                 factoryConfiguration("org.apache.sling.distribution.resources.impl.DistributionServiceResourceProviderFactory")
                    .put("kind", "exporter")
                    .put("provider.roots", "/libs/sling/distribution/services/exporters")
                    .asOption(),
                 factoryConfiguration("org.apache.sling.distribution.resources.impl.DistributionServiceResourceProviderFactory")
                    .put("kind", "importer")
                    .put("provider.roots", "/libs/sling/distribution/services/importers")
                    .asOption()
        );

    }
    
    protected static Option publishOsgiConfigs() {
        return publishOsgiConfigs("subscriber-agent1");
    }

    /**
     * OSGI configuration targeted to the publish instances only
     */
    public static Option publishOsgiConfigs(String agentName) {
        return publishOsgiConfigs(agentName, true, null);

    }

    protected static Option publishOsgiConfigs(String agentName, boolean editable, String stage) {


        Option subConfig = composite(
                factoryConfiguration("org.apache.sling.distribution.resources.impl.DistributionServiceResourceProviderFactory")
                        .put("kind", "agent")
                        .put("provider.roots", "/libs/sling/distribution/services/agents")
                        .asOption(),

                factoryConfiguration("org.apache.sling.distribution.journal.impl.subscriber.DistributionSubscriberFactory")
                        .put("name", agentName)
                        .put("agentNames", new String[]{"agent1"})
                        .put("packageBuilder.target", "(name=journal)")
                        .put("precondition.target",  stage != null ? "(name=staging)" : "(name=default)")
                        .put("editable", editable)
                        .put("announceDelay", "500")
                        .asOption());

        Option condConfig =  newConfiguration("org.apache.sling.distribution.journal.impl.subscriber.StagingPrecondition")
                .put("subAgentName", stage)
                .asOption();

        return stage != null ? composite(subConfig, condConfig) : subConfig;

    }


    protected static Option remoteDebug() {
        return remoteDebug(5005);
    }

    protected static Option remoteDebug(int debugPort) {
        System.out.println(String.format("Remote debugger on port: %s", debugPort));
        return vmOption("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005");
    }

    private static Option slingQuickstartOakSharedBlobstore(String workingDirectory, String dataStoreDirectory, int httpPort) {

        // Option for a quickstart running an Oak repository with a nodestore and a shared data store.

        String slingHome = String.format("%s/sling", workingDirectory);
        String repositoryHome = String.format("%s/repository", slingHome);
        String localIndexDir = String.format("%s/index", repositoryHome);
        return composite(

                slingQuickstartOak(),

                mavenBundle()
                        .groupId("org.apache.jackrabbit")
                        .artifactId("oak-lucene")
                        .version(SlingOptions.versionResolver),

                mavenBundle()
                        .groupId("org.apache.jackrabbit")
                        .artifactId("oak-segment-tar")
                        .version(SlingOptions.versionResolver),

                mavenBundle()
                        .groupId("org.apache.sling")
                        .artifactId("org.apache.sling.jcr.oak.server")
                        .version(SlingOptions.versionResolver),

                newConfiguration("org.apache.felix.http")
                        .put("org.osgi.service.http.port", httpPort).asOption(),

                newConfiguration("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService")
                        .put("customBlobStore", true)
                        .put("repository.home", repositoryHome)
                        .put("name", "NodeStore with custom blob store").asOption(),

                newConfiguration("org.apache.jackrabbit.oak.plugins.blob.datastore.FileDataStore")
                        .put("path", dataStoreDirectory)
                        .put("minRecordLength", 16384).asOption(),

                newConfiguration("org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService")
                        .put("localIndexDir", localIndexDir).asOption()
        );
    }
    
    private static Option kafka() {
        return composite(
                mvn("com.fasterxml.jackson.core", "jackson-core"),
                mvn("com.fasterxml.jackson.core", "jackson-annotations"),
                mvn("com.fasterxml.jackson.core", "jackson-databind"),
                mvn("org.apache.servicemix.bundles", "org.apache.servicemix.bundles.kafka-clients")
        );
    }
    
    public static Option mvn(String groupId, String artifactId) {
        return mavenBundle().groupId(groupId).artifactId(artifactId).versionAsInProject();
    }

    private static Option jsoup() {
        return mavenBundle().groupId("org.jsoup").artifactId("jsoup").versionAsInProject();
    }

    public static void createTopic(String topicName) {
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        try (AdminClient admin = create(singletonMap(BOOTSTRAP_SERVERS_CONFIG,
                standardConverter().convert(kafkaProperties()).to(KafkaEndpoint.class).kafkaBootstrapServers()))) {
            CreateTopicsResult result = admin.createTopics(singletonList(newTopic));
            result.values().get(topicName).get();
        } catch (Exception e) {
            throw new RuntimeException(format("Failed to create topic %s", topicName), e);
        }
    }

    public static void createTopics() {
        createTopic(TOPIC_DISCOVERY);
        createTopic(TOPIC_PACKAGE);
        createTopic(TOPIC_COMMAND);
        createTopic(TOPIC_STATUS);
        createTopic(TOPIC_EVENT);
    }

    public static MessagingProvider createProvider() {
        KafkaClientProvider provider = new KafkaClientProvider();
        provider.activate(standardConverter().convert(kafkaProperties()).to(KafkaEndpoint.class));
        return provider;
    }

    private static Map<String,String> kafkaProperties() {
         return ImmutableMap.of(
                "kafkaDefaultApiTimeout", "5000",
                "kafkaConnectTimeout", "32000");
    }

}
