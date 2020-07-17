package com.cloudimpl.cluster4j.test;

/// *
// * To change this license header, choose License Headers in Project Properties. To change this template file, choose
// * Tools | Templates and open the template in the editor.
// */
// package com.cloudimpl.cloudengine.test;
//
// import com.cloudimpl.cloudengine.CloudUtil;
// import com.cloudimpl.common.GsonCodec;
// import io.scalecube.cluster.Cluster;
// import io.scalecube.cluster.ClusterConfig;
// import java.util.Collections;
// import java.util.Map;
//
/// **
// *
// * @author nuwansa
// */
// public class Node2 {
// public static void main(String[] args) throws InterruptedException {
// // Join Carol to cluster with metadata
// Map<String, String> metadata = Collections.singletonMap("name", "Carol");
// ClusterConfig config = ClusterConfig.builder().port(20000).metadata(metadata)
// .seedMembers(Address.create(CloudUtil.getHostIpAddr(), 10000)).build();
// Cluster carol = Cluster.joinAwait(config);
// carol.listenMembership().subscribe(e -> System.out
// .println(e + "old " + GsonCodec.encode(e.oldMetadata()) + " new " + GsonCodec.encode(e.newMetadata())));
// Thread.sleep(1000000);
//
// }
// }
