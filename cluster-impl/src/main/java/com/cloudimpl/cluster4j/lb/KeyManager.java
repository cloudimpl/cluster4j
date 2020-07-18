/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.lb;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class KeyManager {
  private final Set<String> keys = new ConcurrentSkipListSet<>();
  private final String name;
  private Listener listener;

  public KeyManager(String name) {
    this.name = name;
  }

  public boolean isKeyValid(String key) {
    return keys.contains(key);
  }

  public void attachListener(Listener listner) {
    this.listener = listner;
  }

  public Mono<Collection<String>> attachKeys(Collection<String> keys) {
    if (listener == null)
      return Mono.just(keys).doOnNext(ks -> keys.addAll(ks));
    else
      return listener.onAquire(keys).doOnNext(ks -> {
        keys.addAll(ks);
      });
  }

  public Mono<Collection<String>> releaseKeys(Collection<String> keys) {
    if (listener == null)
      return Mono.just(keys).doOnNext(ks -> keys.removeAll(ks));
    else
      return listener.onRelease(keys).doOnNext(ks -> {
        keys.removeAll(ks);
      });
  }


  public static interface Listener {
    Mono<Collection<String>> onAquire(Collection<String> keys);

    Mono<Collection<String>> onRelease(Collection<String> keys);
  }

}
