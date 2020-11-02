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

import com.cloudimpl.cluster.operator.controller.CloudFunctionController;
import com.cloudimpl.cluster.operator.crd.CloudFunctionBundle;
import com.cloudimpl.cluster.operator.crd.CloudFunctionBundleList;
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.coreImpl.CloudEngine;
import com.cloudimpl.cluster4j.logger.Logger;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import static java.lang.Thread.sleep;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "ClusterOperator")
@Router(routerType = RouterType.ROUND_ROBIN)
public class ClusterOperator implements Function<CloudMessage, Mono<CloudMessage>> {

    private final ILogger logger;
    private Executor executor;
    private final CloudEngine engine;
    @Inject
    public ClusterOperator(Logger logger,CloudEngine engine) {
        this.engine = engine;
        this.logger = logger.createSubLogger(ClusterOperator.class);
        this.executor = Executors.newSingleThreadExecutor();
        run();
    }

    @Override
    public Mono<CloudMessage> apply(CloudMessage t) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void run()
    {
        executor.execute(runOperator());
    }
    
    public Runnable runOperator() {
        return () -> {
            try (KubernetesClient client = new DefaultKubernetesClient()) {
                String namespace = client.getNamespace();
                if (namespace == null) {
                    logger.info("No namespace found via config, assuming default.");
                    namespace = "default";
                }

                logger.info("Using namespace : {0}", namespace);
                logger.info("pod name : {0}", System.getenv("MY_POD_NAME"));
                CustomResourceDefinition cloudFunctionBundleDef = client.apiextensions().v1().customResourceDefinitions()
                        .load(ClusterOperator.class.getResourceAsStream("/cfb-crd.yaml")).get();
                logger.info("loading custom resource definition {0}", cloudFunctionBundleDef);
                CustomResourceDefinitionContext cloudFunctionBundleDefContext = new CustomResourceDefinitionContext.Builder()
                        .withName("cloudfunctionbundles.cluster.cloudimpl.com")
                        .withVersion("v1alpha1")
                        .withScope("Namespaced")
                        .withGroup("cluster.cloudimpl.com")
                        .withPlural("cloudfunctionbundles")
                        .build();

           //    client.apiextensions().v1().customResourceDefinitions().create(cloudFunctionBundleDef);
                SharedInformerFactory informerFactory = client.informers();

                //  MixedOperation<CloudFunctionBundle, CloudFunctionBundleList, DoneableCloudFunctionBundle, Resource<CloudFunctionBundle, DoneableCloudFunctionBundle>> podSetClient = client.customResources(cloudFunctionBundleDef, CloudFunctionBundle.class, CloudFunctionBundleList.class, DoneableCloudFunctionBundle.class);
                SharedIndexInformer<CloudFunctionBundle> cfbSharedIndexInformer = informerFactory.sharedIndexInformerForCustomResource(cloudFunctionBundleDefContext,CloudFunctionBundle.class, CloudFunctionBundleList.class, 10 * 60 * 1000);
                SharedIndexInformer<Pod> podSharedIndexInformer = informerFactory.sharedIndexInformerFor(Pod.class, PodList.class, 10 * 60 * 1000);
                CloudFunctionController cfbController = new CloudFunctionController(engine,logger.createSubLogger(CloudFunctionController.class), client, cfbSharedIndexInformer,podSharedIndexInformer, namespace);

                cfbController.create();
                informerFactory.startAllRegisteredInformers();
                informerFactory.addSharedInformerEventListener(exception -> logger.exception(exception, "Exception occurred, but caught"));
                while(true)
                {
                    try {
                        sleep(1);
                    } catch (InterruptedException ex) {
                        java.util.logging.Logger.getLogger(ClusterOperator.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (KubernetesClientException exception) {
                logger.exception(exception, "Kubernetes Client Exception");
            }
        };
    }
}
