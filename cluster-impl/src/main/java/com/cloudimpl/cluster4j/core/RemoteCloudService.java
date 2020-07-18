/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import com.cloudimpl.cluster4j.common.CloudMessage;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.DefaultPayload;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class RemoteCloudService implements CloudService {

  private final String id;
  private final String nodeId;
  private final String name;
  private final Supplier<Mono<RSocket>> transportSupplier;
  private final CloudServiceDescriptor descriptor;

  public RemoteCloudService(String nodeId, Supplier<Mono<RSocket>> transportSupplier,
      CloudServiceDescriptor descriptor) {
    this.id = descriptor.getServiceId();
    this.nodeId = nodeId;
    this.name = descriptor.getName();
    this.transportSupplier = transportSupplier;
    this.descriptor = descriptor;
  }

  @Override
  public void init() {

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
  public String name() {
    return name;
  }

  @Override
  public <T> Mono<T> requestReply(CloudMessage msg) {
    return transportSupplier.get().flatMap(rsocket -> rsocket.requestResponse(createPayload(msg))).map(this::decode);
  }

  @Override
  public <T> Flux<T> requestStream(CloudMessage msg) {
    return transportSupplier.get().flatMapMany(rsocket -> rsocket.requestStream(createPayload(msg))).map(this::decode);
  }

  @Override
  public <T> Mono<Void> send(CloudMessage msg) {
    return transportSupplier.get().flatMap(rsocket -> rsocket.fireAndForget(createPayload(msg)));
  }

  private Payload createPayload(CloudMessage msg) {
    return DefaultPayload.create(MessageCodecImpl.instance().encode(msg));
  }

  private <T> T decode(Payload payload) {
    CloudMessage cmsg = MessageCodecImpl.instance().decode(CloudMessage.class, payload.sliceData());
    // payload.release();
    return cmsg.data();
  }

  @Override
  public CloudServiceDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public String toString() {
    return "RemoteCloudService{" + "id=" + id + ", nodeId=" + nodeId + ", name=" + name + '}';
  }


}
