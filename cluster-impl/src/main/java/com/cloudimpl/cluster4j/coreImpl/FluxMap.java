/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.coreImpl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 *
 * @author nuwansa
 * @param <K>
 * @param <V>
 */
public class FluxMap<K, V> implements FluxStream<K, V>, ConcurrentMap<K, V> {

  private final Map<K, V> map;
  private final Flux<Event<K, V>> flux;
  private List<FluxSink<Event<K, V>>> emitters = new CopyOnWriteArrayList<>();

  public FluxMap(Map<K, V> map) {
    this.map = map;
    Iterable<Entry<K, V>> ite = () -> map.entrySet().iterator();
    Flux<Event<K, V>> source = Flux.fromIterable(ite)
        .map(e -> new Event<>(Event.Type.ADD, e.getKey(), e.getValue()));
    Flux creator = Flux.<Event<K, V>>create(emitter -> {
      emitters.add(emitter);
      emitter.onCancel(() -> removeEmitter(emitter));
      emitter.onDispose(() -> removeEmitter(emitter));
    });
    flux = source.concatWith(creator);
  }

  public FluxMap() {
    this(new ConcurrentHashMap<>());
  }

  @Override
  public Flux<Event<K, V>> flux() {
    return flux;
  }

  @Override
  public Collection<V> values() {
    return map.values();
  }

  @Override
  public V put(K key, V value) {
    V old = map.put(key, value);
    if (old == null)
      sinkNext(new Event<>(Event.Type.ADD, key, value));
    else
      sinkNext(new Event<>(Event.Type.UPDATE, key, value));
    return old;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    V old = map.putIfAbsent(key, value);
    if (old == null) {
      sinkNext(new Event<>(Event.Type.ADD, key, value));
    }
    return old;
  }

  @Override
  public V get(Object key) {
    return map.get(key);
  }

  private void sinkNext(Event<K, V> event) {
    emitters.forEach(emitter -> emitter.next(event));
  }

  private void removeEmitter(FluxSink sink) {
    emitters.remove(sink);
  }

  @Override
  public V remove(Object key) {
    V value = map.remove(key);
    if (value != null) {
      sinkNext(new Event<>(Event.Type.REMOVE, (K) key, (V) value));
    }
    return value;
  }

  @Override
  public boolean remove(Object key, Object value) {
    boolean ok = map.remove(key, value);
    if (ok) {
      sinkNext(new Event<>(Event.Type.REMOVE, (K) key, (V) value));
    }
    return ok;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    boolean ok = map.replace(key, oldValue, newValue);
    if (ok) {
      sinkNext(new Event<>(Event.Type.UPDATE, key, newValue));
    }
    return ok;
  }

  @Override
  public V replace(K key, V value) {
    V old = map.replace(key, value);
    if (old != null)
      sinkNext(new Event<>(Event.Type.UPDATE, key, value));
    return old;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    map.putAll(m);
    m.entrySet().forEach(e -> sinkNext(new Event<>(Event.Type.ADD, e.getKey(), e.getValue())));
  }

  @Override
  public void clear() {
    map.entrySet().forEach(e -> sinkNext(new Event<>(Event.Type.REMOVE, e.getKey(), e.getValue())));
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return map.entrySet();
  }

}
