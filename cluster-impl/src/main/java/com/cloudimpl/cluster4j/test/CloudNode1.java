/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.test;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudFunction;
import com.cloudimpl.cluster4j.core.CloudRouterDescriptor;
import com.cloudimpl.cluster4j.core.Injector;
import com.cloudimpl.cluster4j.lb.TopicLoadBalancer;
import com.cloudimpl.cluster4j.logger.ConsoleLogWriter;
import com.cloudimpl.cluster4j.logger.LogWriter;
import com.cloudimpl.cluster4j.node.CloudNode;
import com.cloudimpl.cluster4j.node.NodeConfig;
import com.cloudimpl.cluster4j.routers.DynamicRouter;
import com.cloudimpl.cluster4j.routers.RoundRobinRouter;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class CloudNode1 {

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
      return Mono.just(t.data() + "-node1");
    }

  }

  public static void main(String[] args) throws InterruptedException {


    Injector injector = new Injector();
    injector.bind(LogWriter.class).to(new ConsoleLogWriter());

    // CloudServiceRegistry reg = new CloudServiceRegistry();

    CloudNode node = new CloudNode(injector, NodeConfig.builder().build());
    node.registerService("TestService", CloudFunction.builder().withFunction(CloudNode1.TestFunction.class)
        .withRouter(CloudRouterDescriptor.builder().withRouterType(RoundRobinRouter.class).build()).build());
    node.registerService("TestService2", CloudFunction.builder().withFunction(TestFunction2.class)
        .withRouter(CloudRouterDescriptor.builder().withRouterType(DynamicRouter.class)
            .withLoadBalancer("TopicLoadBalancer").build())
        .build());
    node.registerService("TopicLoadBalancer", CloudFunction.builder().withFunction(TopicLoadBalancer.class)
        .withRouter(CloudRouterDescriptor.builder().withRouterType(RoundRobinRouter.class).build()).build());
    node.start();

    Thread.sleep(1000);

    Thread.sleep(1000000);
  }
}
