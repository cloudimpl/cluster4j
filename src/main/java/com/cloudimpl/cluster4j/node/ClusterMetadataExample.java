package com.cloudimpl.cluster4j.node;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.Member;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import reactor.core.Exceptions;

/**
 * Using Cluster metadata: metadata is set of custom parameters that may be used by application developers to attach
 * additional business information and identifications to cluster members.
 *
 * <p>
 * in this example we see how to attach logical name to a cluster member we nick name Joe
 *
 * @author ronen_h, Anton Kharenko
 */
public class ClusterMetadataExample {

  public static Object decode(ByteBuffer byteBuffer) {
    try {
      return DefaultObjectMapper.OBJECT_MAPPER.readValue(
          new ByteBufferBackedInputStream(byteBuffer), Map.class);
      // return GsonCodec.decode(Map.class, new String(byteBuffer.array()));
    } catch (Exception e) {
      // logger.exception(e, "Failed to read metadata: ");
      return null;
    }
  }

  public static ByteBuffer encode(Object input) {
    Map<String, String> serviceEndpoint = (Map<String, String>) input;
    try {
      return ByteBuffer.wrap(
          DefaultObjectMapper.OBJECT_MAPPER
              .writeValueAsString(serviceEndpoint)
              .getBytes(StandardCharsets.UTF_8));
      // return ByteBuffer.wrap(GsonCodec.encode(serviceEndpoint).getBytes());
    } catch (Exception e) {
      // logger.exception(e, "Failed to write metadata: ");
      throw Exceptions.propagate(e);
    }
  }

  public static class MessageCodecImpl implements MessageCodec {

    @Override
    public Message deserialize(InputStream stream) throws Exception {
      return DefaultObjectMapper.OBJECT_MAPPER.readValue(stream, Message.class);
    }

    @Override
    public void serialize(Message message, OutputStream stream) throws Exception {
      DefaultObjectMapper.OBJECT_MAPPER.writeValue(stream, message);
    }
  }

  /** Main method. */
  public static void main(String[] args) throws Exception {
    // Start seed cluster member Alice
    Cluster alice =
        new ClusterImpl().config(
            opt -> opt.metadataDecoder(ClusterMetadataExample::decode).metadataEncoder(ClusterMetadataExample::encode)
                .transport(op -> op.messageCodec(new MessageCodecImpl())))
            .startAwait();

    // Join Joe to cluster with metadata and listen for incoming messages and print them to stdout
    // noinspection unused
    Cluster joe =
        new ClusterImpl()
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Joe"))
                .metadataDecoder(ClusterMetadataExample::decode).metadataEncoder(ClusterMetadataExample::encode)
                .transport(op -> op.messageCodec(new MessageCodecImpl())))
            .membership(opts -> opts.seedMembers(alice.address()))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMessage(Message message) {
                      System.out.println("joe.listen(): " + message.data());
                    }
                  };
                })
            .startAwait();

    // Scan the list of members in the cluster and find Joe there
    Optional<Member> joeMemberOptional =
        alice.otherMembers().stream()
            .filter(
                member -> {
                  // noinspection unchecked
                  Map<String, String> metadata = (Map<String, String>) alice.metadata(member).get();
                  return "Joe".equals(metadata.get("name"));
                })
            .findAny();

    System.err.println("### joeMemberOptional: " + joeMemberOptional);

    // Send hello to Joe
    joeMemberOptional.ifPresent(
        member -> alice
            .send(member, Message.withData("Hello Joe").build())
            .subscribe(
                null,
                e -> {
                  // no-op
                }));

    TimeUnit.SECONDS.sleep(3);
  }
}
