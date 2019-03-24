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
package org.apache.sling.distribution.journal.it.ext;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.InitializationError;
import org.ops4j.pax.exam.junit.PaxExam;

/**
 * Shows how to extend the PaxExam runner to implement hooks that are executed outside of OSGi in the parent
 * junit process 
 */
public class ExtPaxExam extends PaxExam {

    private Optional<Method> before;
    private Optional<Method> after;

    public ExtPaxExam(Class<?> klass) throws InitializationError {
        super(klass);
        this.before = getStaticMethodWith(klass, BeforeOsgi.class);
        this.after = getStaticMethodWith(klass, AfterOsgi.class);
    }

    @Override
    public void run(RunNotifier notifier) {
        /**
         *  If a test is run more than once from the ide the by default the pax exam dir is not deleted.
         *  This causes bundles to be installed multiple times. Deleting the pax exam dir to avoid this.
         */
        deleteDir(new File("target/paxexam"));
        this.before.ifPresent(this::invoke);
        try {
            super.run(notifier);
        } finally {
            this.after.ifPresent(this::invoke);
        }
    }
    
    private void deleteDir(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private Optional<Method> getStaticMethodWith(Class<?> klass, Class<? extends Annotation> annotation) {
        Optional<Method> foundMethod = asList(klass.getMethods()).stream()
            .filter(method -> method.getAnnotation(annotation) != null)
            .findFirst();
        if (foundMethod.isPresent()) {
            Method m = foundMethod.get();
            if (!Modifier.isStatic(m.getModifiers())) {
                throw new IllegalStateException("Method " + m.getName() + " must be static to be used as " + annotation.getName());
            }
        }
        return foundMethod;
    }

    private void invoke(Method method) {
        try {
            method.invoke(null);
        } catch (Exception e) {
            throw new RuntimeException("Error calling method " + method, e);
        }
    }
}
