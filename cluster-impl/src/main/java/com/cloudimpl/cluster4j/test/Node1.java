package com.cloudimpl.cluster4j.test;

/// *
// * To change this license header, choose License Headers in Project Properties. To change this template file, choose
// * Tools | Templates and open the template in the editor.
// */
// package com.cloudimpl.cloudengine.test;
//
// import io.scalecube.cluster.Cluster;
// import io.scalecube.cluster.ClusterConfig;
// import java.util.Collections;
//
/// **
// *
// * @author nuwansa
// */
// public class Node1 {
// public static void main(String[] args) throws InterruptedException {
//
// // Start seed member Alice
// Cluster alice = Cluster.joinAwait(ClusterConfig.builder().port(10000).build());
// System.out.println(alice.address());
// alice.listenMembership().subscribe(e -> System.out.println(e));
// Thread.sleep(10000);
// System.out.println("11111111");
// alice.updateMetadata(Collections.singletonMap("fooo", "temp")).subscribe();
// Thread.sleep(5000);
// System.out.println("11111111");
// alice.updateMetadata(Collections.singletonMap("fooo", "temp1")).subscribe();
// Thread.sleep(5000);
// System.out.println("11111111");
// alice.removeMetadataProperty("fooo").subscribe();
// Thread.sleep(1000000);
// }
// }
