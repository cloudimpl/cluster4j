/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.test;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudFunction;
import com.cloudimpl.cluster4j.core.CloudRouterDescriptor;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Injector;
import com.cloudimpl.cluster4j.logger.ConsoleLogWriter;
import com.cloudimpl.cluster4j.logger.LogWriter;
import com.cloudimpl.cluster4j.node.CloudNode;
import com.cloudimpl.cluster4j.node.NodeConfig;
import com.cloudimpl.cluster4j.routers.DynamicRouter;
import com.cloudimpl.cluster4j.routers.RoundRobinRouter;
import io.scalecube.net.Address;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class CloudNode2 {

  public static final class TestFunction implements Function<CloudMessage, Mono<String>> {

    long rnd = System.currentTimeMillis();

    @Override
    public Mono<String> apply(CloudMessage t) {
      return Mono.just(t.data() + "-" + rnd);
    }

  }

  public static final class TestFunction2 implements Function<CloudMessage, Mono<String>> {

    long rnd = System.currentTimeMillis();

    @Override
    public Mono<String> apply(CloudMessage t) {
      return Mono.just(t.data() + "-node2");
    }

  }

  public static void main(String[] args) throws InterruptedException {


    Injector injector = new Injector();
    injector.bind(LogWriter.class).to(new ConsoleLogWriter());

    CloudNode node = new CloudNode(injector, NodeConfig.builder().withGossipPort(17000).withNodePort(9007)
        .withSeedNodes(Address.create(CloudUtil.getHostIpAddr(), 12000)).build());
    node.registerService("TestService", CloudFunction.builder().withFunction(TestFunction.class)
        .withRouter(CloudRouterDescriptor.builder().withRouterType(RoundRobinRouter.class).build()).build());
    node.registerService("TestService2", CloudFunction.builder().withFunction(TestFunction2.class)
        .withRouter(CloudRouterDescriptor.builder().withRouterType(DynamicRouter.class)
            .withLoadBalancer("TopicLoadBalancer").build())
        .build());
    node.start();


    int i = 0;
    while (true) {
      node.requestReply("TestService", CloudMessage.builder().withData("Hello").withKey("" + i).build())
          .doOnError(e -> System.out.println(e.getMessage()))
          .subscribe(System.out::println);
      i++;
      Thread.sleep(1000);
    }

  }
}
