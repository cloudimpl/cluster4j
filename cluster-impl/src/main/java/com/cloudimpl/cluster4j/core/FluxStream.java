/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 * @param <V>
 */
public interface FluxStream<K, V> {

  Flux<Event<K, V>> flux();

  public static class Event<K, V> {

    public enum Type {
      ADD,
      UPDATE,
      REMOVE
    }

    private final Type type;
    private final K key;
    private final V value;

    public Event(Type type, K key, V value) {
      this.type = type;
      this.key = key;
      this.value = value;
    }

    public Type getType() {
      return type;
    }

    public K getKey() {
      return key;
    }


    public V getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "Event{" + "type=" + type + ", key=" + key + ", value=" + value + '}';
    }



  }
}
