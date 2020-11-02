/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.coreImpl;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudFunction;
import com.cloudimpl.cluster4j.core.CloudServiceDescriptor;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Injector;
import com.cloudimpl.cluster4j.logger.Logger;
import com.cloudimpl.cluster4j.node.NodeConfig;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.logging.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class CloudEngineImpl implements CloudEngine {

    private final CloudRouterRepository routerRepository;
    private final Logger rootLogger;
    private final Injector injector;
    private final CloudServiceRegistry serviceRegistry;
    private final String id;
    private final CorrelationIdGenerator idGen;
    private final NodeConfig config;
    private final Supplier<String> memberIdProvider;
    public CloudEngineImpl(Supplier<String> memberIdProvider,String id, Injector injector, NodeConfig config) {
        this.injector = injector;
        this.id = id;
        this.memberIdProvider = memberIdProvider;
        idGen = new CorrelationIdGenerator(id);
        this.config = config;
        rootLogger = injector.inject(Logger.class);
        this.serviceRegistry = new CloudServiceRegistry(rootLogger);
        injector.bind(CloudServiceRegistry.class).to(serviceRegistry);

        injector.bind(Logger.class).to(rootLogger);
        injector.bind(Injector.class).to(injector);
        injector.bind(CloudEngine.class).to(this);
        this.routerRepository = injector.inject(CloudRouterRepository.class);
        this.routerRepository.subscribe(serviceRegistry.flux());
        try {
            HTTPServer server = new HTTPServer(1234);
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(CloudEngineImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        registerHandlers();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public <T> Mono<T> requestReply(String topic, Object request) {
        try {
            CloudMessage cloudMsg = buildMsg(topic, request);
            return routerRepository.router(topic).route(cloudMsg).flatMap(service -> service
                    .requestReply(cloudMsg.withAttr(CloudMsgHdr.SERVICE_ID, service.id())));
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public <T> Flux<T> requestStream(String topic, Object request) {
        try {
            CloudMessage cloudMsg = buildMsg(topic, request);
            return routerRepository.router(topic).route(cloudMsg).flatMapMany(service -> service
                    .requestStream(cloudMsg.withAttr(CloudMsgHdr.SERVICE_ID, service.id())));
        } catch (Exception ex) {
            return Flux.error(ex);
        }
    }

    @Override
    public Mono<Void> send(String topic, Object data) {
        try {
            CloudMessage cloudMsg = buildMsg(topic, data);
            return routerRepository.router(topic).route(cloudMsg).flatMap(service -> service
                    .send(cloudMsg.withAttr(CloudMsgHdr.SERVICE_ID, service.id())));
        } catch (Exception ex) {
            return Mono.error(ex);
        }
    }

    @Override
    public CloudServiceRegistry getServiceRegistry() {
        return serviceRegistry;
    }

    @Override
    public void registerService(String name, CloudFunction cloudFunc) {
        CloudServiceDescriptor serviceDesc = CloudServiceDescriptor.builder().withFunctionType(cloudFunc.getFunctionType())
                .withInputType(cloudFunc.getInputType())
                .withName(name)
                .withRouterDescriptor(cloudFunc.getRouterDesc())
                .withServiceId(cloudFunc.getId().isEmpty()?idGen.nextCid():idGen.getId(cloudFunc.getId()))
                .withServicePort(config.getNodePort())
                .withHostAddress(CloudUtil.getHostIpAddr()).build();

        serviceRegistry.register(new LocalCloudService(memberIdProvider,id, injector, serviceDesc));
    }

    private CloudMessage buildMsg(String topic, Object req) {
        if (req instanceof CloudMessage) {
            return CloudMessage.class.cast(req)
                    .withAttr(CloudMsgHdr.TOPIC, topic);
                    
        } else {
            return CloudMessage.builder().withData(req).withAttr(CloudMsgHdr.TOPIC, topic).build();
        }
    }

    private void registerHandlers() {
        BiFunction<String, Object, Mono> rrHnd = this::requestReply;
        injector.nameBind("RRHnd", rrHnd);

        BiFunction<String, Object, Flux> rsHnd = this::requestStream;
        injector.nameBind("RSHnd", rsHnd);
    }
}
