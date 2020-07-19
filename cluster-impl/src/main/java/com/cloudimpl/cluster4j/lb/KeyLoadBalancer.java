/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.lb;

import com.cloudimpl.cluster4j.core.lb.LBResponse;
import com.cloudimpl.cluster4j.core.lb.LBRequest;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class KeyLoadBalancer implements Function<LBRequest, Mono<LBResponse>> {

  public KeyLoadBalancer(CloudServiceRegistry reg) {

  }


  @Override
  public Mono<LBResponse> apply(LBRequest t) {
    return null;
  }

}
