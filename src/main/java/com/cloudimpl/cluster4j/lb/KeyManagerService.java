/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.lb;

import com.cloudimpl.cluster4j.common.CloudMessage;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class KeyManagerService implements Function<CloudMessage, Mono<KeyManagerResponse>> {

  @Override
  public Mono<KeyManagerResponse> apply(CloudMessage t) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

}
