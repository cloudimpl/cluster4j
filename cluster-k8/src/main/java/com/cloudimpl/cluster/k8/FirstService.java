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
package com.cloudimpl.cluster.k8;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import java.util.function.BiFunction;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "KubeService")
@Router(routerType = RouterType.ROUND_ROBIN)
public class FirstService implements Function<CloudMessage, Mono<String>> {

    //  ILogger logger;
    public FirstService(@Named("RRHnd") BiFunction<String, Object, Mono> rrHnd) {
        //     logger.info("xxxxxxxxxxxx");
//        try (KubernetesClient client = new DefaultKubernetesClient()) {
//            client.pods().inNamespace("default").list().getItems().forEach(
//                    pod -> System.out.println("yyyy:" + pod.getMetadata().getName())
//            );
//            client.apps().replicaSets().inNamespace("default").list().getItems().stream().forEach(rs -> System.out.println("rs : " + rs));
//            // System.out.println("example replica : "+client.apps().replicaSets().inNamespace("default").list().withName("example").get().getSpec().getReplicas());
//
////            Deployment deploy = client.apps().deployments()
////            .inNamespace("default")
////            .withName("example")
////            .get();
//        } catch (Exception ex) {
//            // Handle exception
//            System.out.println("ex: " + ex.getMessage());
//            //   logger.exception(ex, "error on kubeclient");
//            ex.printStackTrace();
//        }
        watchDeployments();
    }

    @Override
    public Mono<String> apply(CloudMessage t) {
        try (KubernetesClient client = new DefaultKubernetesClient()) {

            client.pods().inNamespace("default").list().getItems().forEach(
                    pod -> System.out.println(pod.getMetadata().getName())
            );

        } catch (KubernetesClientException ex) {
            // Handle exception
            ex.printStackTrace();
        }
        return Mono.just("hello");
    }

    private void watchDeployments() {
        KubernetesClient client = new DefaultKubernetesClient();
        // Get Informer Factory
        SharedInformerFactory sharedInformerFactory = client.informers();

        // Create instance for Pod Informer
        SharedIndexInformer<Deployment> deploymentInformer = sharedInformerFactory.sharedIndexInformerFor(Deployment.class, DeploymentList.class, 30 * 1000L);
        SharedIndexInformer<Pod> podInformer = sharedInformerFactory.sharedIndexInformerFor(Pod.class, PodList.class, 30 * 1000L);
        System.out.println("Informer factory initialized.");

        // Add Event Handler for actions on all Pod events received
        deploymentInformer.addEventHandler(
                new ResourceEventHandler<Deployment>() {
            @Override
            public void onAdd(Deployment pod) {

                System.out.println("Deployment " + pod.getMetadata().getName() + " got added");
            }

            @Override
            public void onUpdate(Deployment oldPod, Deployment newPod) {
                System.out.println("Deployment " + oldPod.getMetadata().getName() + " got updated");
            }

            @Override
            public void onDelete(Deployment pod, boolean deletedFinalStateUnknown) {
                System.out.println("Deployment " + pod.getMetadata().getName() + " got deleted");
            }
        }
        );

        podInformer.addEventHandler(
                new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod pod) {
                System.out.println("pod details: "+pod.getSpec().toString());
                System.out.println("Pod " + pod.getMetadata().getName() + " got added");
            }

            @Override
            public void onUpdate(Pod oldPod, Pod newPod) {
                System.out.println("Pod " + oldPod.getMetadata().getName() + " got updated");
            }

            @Override
            public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
                System.out.println("Pod " + pod.getMetadata().getName() + " got deleted");
            }
        }
        );

        sharedInformerFactory.startAllRegisteredInformers();
        System.out.println("Starting all registered informers");

        // Wait for 1 minute
        //  Thread.sleep(60 * 1000L);
    }
}
