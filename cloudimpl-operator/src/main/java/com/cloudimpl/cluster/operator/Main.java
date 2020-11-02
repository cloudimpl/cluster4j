/*
 * Copyright 2020 nuwansa.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.cluster.operator;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import java.io.InputStream;
import java.util.Collections;

/**
 *
 * @author nuwansa
 */
public class Main {
    public static void main(String[] args) {
      InputStream stream =  Main.class.getClassLoader().getResourceAsStream("/cfb-crd.yaml");
      Deployment dep = new DeploymentBuilder()
                .withNewMetadata().withName("cloudfunction" + "testservice" + "-deployment")
                .addToLabels("app", "nginx")
                .withNamespace("default")
                .addNewOwnerReference().withController(true).withKind("CloudFunctionBundle")
                .withApiVersion("cluster.cloudimpl.com/v1alpha1")
                .withName("testservice")
                .withNewUid("1234").endOwnerReference()
                .endMetadata()
                .withNewSpec()
                .withReplicas(0)
                .withNewSelector()
                .withMatchLabels(Collections.singletonMap("app", "nginx"))
                .endSelector()
                .withNewTemplate()
                .withNewMetadata().addToLabels("app", "nginx").endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName("nginx")
                .withImage("nginx:1.7.9")
                .addNewPort().withContainerPort(80).endPort()
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        System.out.println(dep);
    }
}
