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

import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.notNullValue;
import static org.ops4j.pax.exam.cm.ConfigurationAdminOptions.newConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.distribution.DistributionRequest;
import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.SimpleDistributionRequest;
import org.apache.sling.distribution.agent.spi.DistributionAgent;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.it.DistributionTestSupport;
import org.apache.sling.distribution.journal.it.kafka.PaxExamWithKafka;
import org.apache.sling.distribution.journal.messages.Messages;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Starts a publish instance and checks that it can receive and process a PackageMessage from the journal
 */
@RunWith(PaxExamWithKafka.class)
@ExamReactorStrategy(PerClass.class)
public class PublisherReceiveTest extends DistributionTestSupport {
    private static final String RESOURCE_PATH = "/my";

    private static final String RESOURCE_TYPE = "sling:Folder";

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Inject
    ResourceResolverFactory resourceResolverFactory;
    
    @Inject
    @Filter("(name=journal)")
    private DistributionPackageBuilder packageBuilder;
    
    @Inject
    @Filter("(name=subscriber-agent1)")
    DistributionAgent subscriber;
    
    @Inject
    MessagingProvider provider;
    /*
    @Inject
    ServiceUserMapper serviceUserMapper;
    */
    
    @Configuration
    public Option[] configuration() {
        return new Option[] { //
                //debug(),
                newConfiguration("org.apache.sling.jcr.base.internal.LoginAdminWhitelist")
                        .put("whitelist.bypass", "true").asOption(),
                baseConfiguration(), //
                defaultOsgiConfigs(), //
                publishOsgiConfigs() //
        };
    }

    public Option debug() {
        return CoreOptions.vmOption("-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    }
    
    @Test
    public void testReceive() throws Exception {
    	Arrays.asList(bundleContext.getBundles()).stream()
    	.forEach(bundle -> log.info(bundle.getSymbolicName() + ":" + bundle.getVersion()));
        DistributionPackage pkg = createDistPackage(RESOURCE_PATH);
        Messages.PackageMessage pkgMsg = toPackageMessage(pkg, "agent1");
        provider.createSender().send(TOPIC_PACKAGE, pkgMsg);
        await().until(() -> getResource(RESOURCE_PATH), notNullValue());
    }

    /**
     * Create a resource at the given path, build a DistributionPackage from it and delete the resource again.
     */
    private DistributionPackage createDistPackage(String path)
            throws PersistenceException, DistributionException {
        try (ResourceResolver resolver = createResolver()){
            Resource myRes = ResourceUtil.getOrCreateResource(resolver, path, RESOURCE_TYPE, RESOURCE_TYPE, true);
            log.info("Created resource with path " + myRes.getPath());
            DistributionRequest request = new SimpleDistributionRequest(DistributionRequestType.ADD, myRes.getPath());
            DistributionPackage pkg = packageBuilder.createPackage(resolver, request);
            resolver.delete(myRes);
            resolver.commit();
            return pkg;
        }
    }

    private Resource getResource(String path) {
        try (ResourceResolver resolver = createResolver()) {
            return resolver.getResource(path);
        }
    }
    
    @SuppressWarnings("deprecation")
    private ResourceResolver createResolver() {
        try {
            Map<String, Object> authinfo = new HashMap<String, Object>();
            return resourceResolverFactory.getAdministrativeResourceResolver(authinfo);
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    private PackageMessage toPackageMessage(org.apache.sling.distribution.packaging.DistributionPackage pkg, String agentId) throws IOException {
        final byte[] pkgBinary = IOUtils.toByteArray(pkg.createInputStream());
        final DistributionPackageInfo pkgInfo = pkg.getInfo();
        final List<String> paths = Arrays.asList(pkgInfo.getPaths());
        final List<String> deepPaths = Arrays.asList(pkgInfo.get(PROPERTY_REQUEST_DEEP_PATHS, String[].class));
        final String pkgId = pkg.getId();

        return PackageMessage.newBuilder()
                .setPubSlingId("slingid")
                .setPkgId(pkgId)
                .setPubAgentName(agentId)
                .setPkgBinary(ByteString.copyFrom(pkgBinary))
                .setPkgType(pkg.getType())
                .addAllPaths(paths)
                .setReqType(PackageMessage.ReqType.ADD)
                .addAllDeepPaths(deepPaths)
                .setPkgLength(pkgBinary.length)
                .build();
    }

}
