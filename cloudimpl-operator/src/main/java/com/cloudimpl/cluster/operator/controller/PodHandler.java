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

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.coreImpl.CloudEngine;
import com.cloudimpl.fn.core.impl.FaasServiceHeaders;
import com.cloudimpl.fn.core.msgs.PodDetails;
import io.fabric8.kubernetes.api.model.Pod;
import java.time.Duration;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.retry.Retry;

/**
 *
 * @author nuwansa
 */
public class PodHandler {
    private final CloudFunctionHandler cfh;
    private final CloudEngine engine;
    private Disposable hnd;
    private final ILogger logger;
    private Pod pod;
    public PodHandler(CloudEngine engine,Pod pod,CloudFunctionHandler cfh,ILogger logger) {
        this.cfh = cfh;
        this.engine = engine;
        this.pod = pod;
        this.logger = logger.createSubLogger("PodHandler",pod.getMetadata().getName());
        this.logger.info("pod handler created");
    }
    
    public PodHandler update(Pod pod)
    {
        this.pod = pod;
        this.logger.info("pod handler updated");
        return this;
    }
    
    public void close()
    {
        if(hnd != null)
            hnd.dispose();
        hnd = null;
    }
    
    public synchronized void run()
    {
        if(hnd != null)
            hnd.dispose();
        hnd = Mono.just(true).publishOn(Schedulers.parallel())
                .flatMap(b->sendPodDetails())
                .doOnError(err->logger.exception(err, "error running pod handler"))
                .retryWhen(Retry
                        .any()
                        .exponentialBackoffWithJitter(Duration.ofSeconds(1), Duration.ofSeconds(20))
                )
                .subscribe();
    }
    
    private Mono<?> sendPodDetails()
    {
        CloudMessage podDetails = createPodDetails(pod);
        logger.info("pod details {0} sending to nodeid {1}", podDetails.data(),podDetails.getKey());
        return this.engine.requestReply(FaasServiceHeaders.FAAS_SERVICE_NAME, createPodDetails(pod));
    }
    
    private CloudMessage createPodDetails(Pod pod)
    {
        String podName = pod.getMetadata().getName();
        String podIp = pod.getStatus().getPodIP();
        String status = pod.getStatus().getPhase();
        return CloudMessage.builder().withData(new PodDetails(podName, podIp, status)).withKey(pod.getSpec().getNodeName()).build();
    }
}
