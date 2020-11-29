/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.coreImpl;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.CloudServiceDescriptor;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Injector;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class LocalCloudService implements CloudService {

    protected final String id;
    protected final Supplier<String> memberIdProvider;
    protected final String nodeId;
    protected final String name;
    protected Function<CloudMessage, Publisher> function;
    protected final CloudServiceDescriptor descriptor;
    protected final Injector injector;

    public LocalCloudService(Supplier<String> memberIdProvider, String nodeId, Injector injector, CloudServiceDescriptor descriptor) {
        this.memberIdProvider = memberIdProvider;
        this.id = descriptor.getServiceId();
        this.name = descriptor.getName();
        this.function = null;
        this.descriptor = descriptor;
        this.injector = injector;
        this.nodeId = nodeId;
    }

    @Override
    public void init() {
        Injector inject = injector.with("@srvId", id).with("@srvName", name);
        this.function = CloudUtil.newInstance(inject, descriptor.getFunctionType());
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public String memberId() {
        return memberIdProvider.get();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public <T> Mono<T> requestReply(CloudMessage msg) {
        return Mono.from(function.apply(msg));
    }

    @Override
    public <T> Flux<T> requestStream(CloudMessage msg) {
        return Flux.from(function.apply(msg));
    }

    @Override
    public <T> Mono<Void> send(CloudMessage msg) {
        return Mono.from(function.apply(msg));
    }

    @Override
    public CloudServiceDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public String toString() {
        return "LocalCloudService{" + "id=" + id + ", nodeId=" + nodeId() + ", name=" + name + '}';
    }

}
