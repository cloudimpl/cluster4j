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

import static com.cloudimpl.cluster.operator.controller.CloudFunctionHandler.RESOURCE_PREFIX;
import com.cloudimpl.cluster.operator.crd.CloudFunctionBundle;
import com.cloudimpl.cluster4j.common.GsonCodec;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.coreImpl.CloudEngine;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author nuwansa
 */
public class CloudFunctionController {

    private final KubernetesClient kubeClient;
    private final SharedIndexInformer<CloudFunctionBundle> cfbInformer;
    private final SharedIndexInformer<Pod> podSharedIndexInformer;
    private final Lister<CloudFunctionBundle> cfbLister;
    private final String namespace;
    private final BlockingQueue<String> workqueue;
    private final ILogger logger;
    private final Map<String, CloudFunctionHandler> cfbHandlers;
    private final CloudEngine engine;

    public CloudFunctionController(CloudEngine engine, ILogger logger, KubernetesClient kubeClient, SharedIndexInformer<CloudFunctionBundle> cfbInformer,
            SharedIndexInformer<Pod> podSharedIndexInformer, String namespace) {
        this.kubeClient = kubeClient;
        this.cfbInformer = cfbInformer;
        this.podSharedIndexInformer = podSharedIndexInformer;
        this.namespace = namespace;
        this.cfbLister = new Lister<>(cfbInformer.getIndexer(), namespace);
        this.workqueue = new LinkedBlockingQueue<>();
        this.logger = logger;
        this.cfbHandlers = new ConcurrentHashMap<>();
        this.engine = engine;
    }

    public void create() {
        cfbInformer.addEventHandler(new ResourceEventHandler<CloudFunctionBundle>() {
            @Override
            public void onAdd(CloudFunctionBundle cfb) {
                logger.debug("CloudFunctionBundle onAdd {0}", cfb);
                logger.info("CloudFunctionBundle added  . {0}", cfb.getMetadata().getName());
                getHandler(cfb).run();
            }

            @Override
            public void onUpdate(CloudFunctionBundle old, CloudFunctionBundle newcfb) {
                logger.debug("CloudFunctionBundle onUpdate  : old {0} , new {1}", old, newcfb);
                logger.info("CloudFunctionBundle updated  . {0}", newcfb.getMetadata().getName());
                getHandler(newcfb).run();
            }

            @Override
            public void onDelete(CloudFunctionBundle cfb, boolean deletedFinalStateUnknown) {
                // Do nothing
                logger.info("CloudFunctionBundle deleted {0}", cfb.getMetadata().getName());
                closeHandler(cfb);
            }
        });

        this.podSharedIndexInformer.addEventHandler(new ResourceEventHandler<Pod>() {
            @Override
            public void onAdd(Pod pod) {
                logger.debug("pod added : {0}", GsonCodec.encodePretty(pod));
                logger.info("pod added  . {0}", pod.getMetadata().getName());
                getCloudHandler(pod).ifPresent(h -> h.updatePod(pod));
            }

            @Override
            public void onUpdate(Pod old, Pod newPod) {
                logger.debug("pod updated old : {0} , new {1}", GsonCodec.encodePretty(old), GsonCodec.encodePretty(newPod));
                logger.info("pod updated  . {0}", newPod.getMetadata().getName());
                getCloudHandler(newPod).ifPresent(h -> h.updatePod(newPod));
            }

            @Override
            public void onDelete(Pod pod, boolean bln) {
                logger.info("pod deleted . {0}",pod.getMetadata().getName());
                getCloudHandlerByUid(getUidOfCloudFunctionHandler(pod)).ifPresent(hnd -> hnd.closePod(pod));
            }
        });
    }

    public void run() throws InterruptedException {
        while (!cfbInformer.hasSynced() || !this.podSharedIndexInformer.hasSynced()) {
            // Wait till Informer syncs
            logger.info("waiting for cloudfunctionbundle informer to sync");
            Thread.sleep(1000);
        }
    }

    private CloudFunctionHandler getHandler(CloudFunctionBundle cfb) {
        return cfbHandlers.computeIfAbsent(cfb.getMetadata().getUid(), uid -> new CloudFunctionHandler(engine, kubeClient, cfb, logger))
                .update(cfb);
    }

    private Optional<CloudFunctionHandler> getCloudHandlerByUid(String uid) {
        CloudFunctionHandler hnd = cfbHandlers.get(uid);
        logger.info("loading cloud handler by uid {0} - instance {1}",uid,hnd);
        if (hnd != null) {
            return Optional.of(hnd);
        } else {
            return Optional.empty();
        }
    }

    private String getUidOfCloudFunctionHandler(Pod pod)
    {
        String uid =  pod.getMetadata().getAnnotations().get(RESOURCE_PREFIX + "pod-type-cloud-function");
        logger.info("uid of cloudhandler of pod {0} is {1}",pod.getMetadata().getName(),uid );
        return uid;
    }
    
    private Optional<CloudFunctionHandler> getCloudHandler(Pod pod) {
        String uid = getUidOfCloudFunctionHandler(pod);
        Optional<CloudFunctionHandler> handler = null;
        if (uid != null && (handler = getCloudHandlerByUid(uid)).isPresent()) {
            return handler;
        }
        return Optional.empty();
    }

    private void closeHandler(CloudFunctionBundle cfb) {
        CloudFunctionHandler hnd = cfbHandlers.remove(cfb.getMetadata().getUid());
        if (hnd != null) {
            hnd.close();
        }
    }
}
