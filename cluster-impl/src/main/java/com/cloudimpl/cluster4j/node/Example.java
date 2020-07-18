package com.cloudimpl.cluster4j.node;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Using Cluster metadata: metadata is set of custom parameters that may be used by application developers to attach
 * additional business information and identifications to cluster members.
 *
 * <p>
 * in this example we see how to attach logical name to a cluster member we nick name Joe
 *
 * @author ronen_h, Anton Kharenko
 */
public class Example {

  /** Main method. **/
  public static void main(String[] args) throws Exception {
    ClusterConfig clusterConfig = ClusterConfig.defaultLanConfig();

    // Start seed cluster member Alice
    Cluster alice =
        new ClusterImpl().config(
            opt -> opt.metadataDecoder(ClusterMetadataExample::decode).metadataEncoder(ClusterMetadataExample::encode)
                .transport(op -> op.messageCodec(new ClusterMetadataExample.MessageCodecImpl())))
            // .config(opts -> opts.memberPort(5001))
            .config(opts -> opts.memberAlias("Alice"))
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Alice")))
            .handler(
                cluster -> {
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                      System.out.println(" Alice received: " + event);
                      System.out.println(cluster.members());
                    }
                  };
                })
            .startAwait();

    System.out.println(alice.address());

    // Join Joe to cluster with metadata and listen for incoming messages and print them to stdout
    // noinspection unused
    Cluster joe =
        new ClusterImpl()
            .membership(opts -> opts.seedMembers(alice.address()))
            .config(
                opt -> opt.metadataDecoder(ClusterMetadataExample::decode)
                    .metadataEncoder(ClusterMetadataExample::encode)
                    .transport(op -> op.messageCodec(new ClusterMetadataExample.MessageCodecImpl())))
            .config(opts -> opts.memberAlias("joe"))
            .config(opts -> opts.metadata(Collections.singletonMap("name", "Joe")))
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

    System.out.println("alice: " + alice.members());
    System.out.println("joe: " + joe.members());
    TimeUnit.SECONDS.sleep(30);
    System.out.println("alice: " + alice.members());
    System.out.println("joe: " + joe.members());
  }
}
