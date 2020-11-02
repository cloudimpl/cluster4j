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
package com.cloudimpl.cluster.operator.controller;

import com.cloudimpl.cluster.operator.crd.CloudFunctionBundle;
import com.cloudimpl.cluster.operator.crd.Container;
import com.cloudimpl.cluster.operator.utils.Utils;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.coreImpl.CloudEngine;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v1.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;

/**
 *
 * @author nuwansa
 */
public class CloudFunctionHandler {

    private final KubernetesClient client;
    private Deployment deployment;
    private HorizontalPodAutoscaler podScaler;
    private CloudFunctionBundle cfb;
    public static final String RESOURCE_PREFIX = "com-cloudimpl-cluster-cfb-";
    private Disposable hnd;
    private final ILogger logger;
    private final Map<String, PodHandler> podHandlers = new ConcurrentHashMap<>();
    private final CloudEngine engine;
    public CloudFunctionHandler(CloudEngine engine,KubernetesClient client, CloudFunctionBundle cfb, ILogger logger) {
        this.client = client;
        this.cfb = cfb;
        this.engine = engine;
        this.logger = logger.createSubLogger("CloudFunctionHandler", cfb.getMetadata().getName());
        this.logger.info("cloudfunction handler created.");
    }

    public CloudFunctionHandler update(CloudFunctionBundle cfb) {
        this.cfb = cfb;
        this.logger.info("cloudfunction handler updated.");
        return this;
    }

    public synchronized void run() {
        if (hnd != null) {
            hnd.dispose();
        }
        hnd = Mono.just(true).publishOn(Schedulers.parallel())
                .doOnNext(b -> createDeployment(cfb))
                .doOnError(err -> logger.exception(err, "error running CloudFunction handler"))
                .retryWhen(Retry
                        .any()
                        .exponentialBackoffWithJitter(Duration.ofSeconds(1), Duration.ofSeconds(20))
                )
                .subscribe();

    }

    public synchronized void close() {
        if (this.hnd != null) {
            this.hnd.dispose();
        }
        podHandlers.values().forEach(h->h.close());
        podHandlers.clear();
        this.hnd = null;
    }

    public void updatePod(Pod pod) {
        getPodHandler(pod).run();
    }

    public void closePod(Pod pod){
        PodHandler h = podHandlers.remove(pod.getMetadata().getUid());
        if(h != null)
            h.close();
    }
    
    
    public PodHandler getPodHandler(Pod pod)
    {
        return podHandlers.computeIfAbsent(pod.getMetadata().getUid(), p->new PodHandler(engine, pod, this, logger)).update(pod);
    }
    
    public static String getPodIdentifierAnnotation() {
        return RESOURCE_PREFIX + "pod-type-cloud-function";
    }

    
    private void createDeployment(CloudFunctionBundle cfb) {

        System.out.println("containersX size : " + cfb.getSpec().getContainers().size());
        Optional<Container> containerRef = cfb.getSpec().getContainers().stream().findFirst();
        Container container;
        if (containerRef.isEmpty()) {
            logger.error("container not present in the CloudFunctionBundle {0}", cfb);
            return;
        } else {
            container = containerRef.get();
            logger.info("container present : name :{0}  ,image {1}", container.getName(), container.getImage());
        }
        Deployment dep = new DeploymentBuilder()
                .withNewMetadata().withName(RESOURCE_PREFIX + cfb.getMetadata().getName() + "-deployment")
                .addToLabels(RESOURCE_PREFIX + "name", cfb.getMetadata().getName())
                .withNamespace(cfb.getMetadata().getNamespace())
                .addNewOwnerReference().withController(true).withKind(cfb.getKind()).withBlockOwnerDeletion(Boolean.TRUE)
                .withApiVersion(cfb.getApiVersion())
                .withName(cfb.getMetadata().getName())
                .withNewUid(cfb.getMetadata().getUid()).endOwnerReference()
                .endMetadata()
                .withNewSpec()
                .withReplicas(cfb.getSpec().getMinReplicas())
                .withNewSelector()
                .withMatchLabels(Collections.singletonMap(RESOURCE_PREFIX + "name", cfb.getMetadata().getName()))
                .endSelector()
                .withNewTemplate()
                .withNewMetadata().addToLabels(RESOURCE_PREFIX + "name", cfb.getMetadata().getName())
                .addToAnnotations(RESOURCE_PREFIX + "pod-type-cloud-function", cfb.getMetadata().getUid())
                .endMetadata()
                .withNewSpec()
                .withNodeSelector(Collections.singletonMap("zone", "services"))
                .addNewContainer()
                .withName(container.getName())
                .withImage(container.getImage())
                .withResources(Utils.getResourceReq(container))
                .addNewPort().withContainerPort(80).endPort()
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        logger.info("deployment {0} ready to deploy.", dep.getMetadata().getName());
        this.client.apps().deployments().createOrReplace(dep);
        logger.info("deployment {0} done.", dep.getMetadata().getName());
        this.deployment = dep;
    }
}
