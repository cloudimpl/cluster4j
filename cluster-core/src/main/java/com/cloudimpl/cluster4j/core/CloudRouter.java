/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import com.cloudimpl.cluster4j.common.CloudMessage;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface CloudRouter {
  Mono<CloudService> route(CloudMessage msg);
}