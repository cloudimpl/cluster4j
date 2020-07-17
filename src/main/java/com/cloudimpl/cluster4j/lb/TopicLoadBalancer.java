/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.lb;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudServiceRegistry;
import com.cloudimpl.cluster4j.core.FluxStream;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class TopicLoadBalancer implements Function<CloudMessage, Mono<LBResponse>> {

  private final Map<String, TopicHandler> handlers = new ConcurrentHashMap<>();
  private final CloudServiceRegistry reg;

  public TopicLoadBalancer(CloudServiceRegistry reg) {
    this.reg = reg;
  }


  @Override
  public Mono<LBResponse> apply(CloudMessage msg) {
    LBRequest t = msg.data();
    return hnd(t.getTopic()).map(h -> h.assign(t.getKey())).map(r -> new LBResponse(t.getTopic(), r, t.getKey()));
  }


  private Mono<TopicHandler> hnd(String name) {
    return Mono.just(handlers.computeIfAbsent(name, n -> new TopicHandler(n, reg)));
  }

  public static final class TopicHandler {

    private Map<String, Bucket> mapBuckets = new ConcurrentHashMap<>();

    private final String name;

    public TopicHandler(String name, CloudServiceRegistry reg) {
      this.name = name;
      reg.flux().filter(e -> e.getValue().name().equals(name))
          .filter(e -> e.getType() == FluxStream.Event.Type.ADD || e.getType() == FluxStream.Event.Type.UPDATE)
          .map(e -> e.getValue())
          .doOnNext(srv -> mapBuckets.computeIfAbsent(srv.id(), id -> new Bucket(id)))
          .subscribe();

      reg.flux().filter(e -> e.getValue().name().equals(name))
          .filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
          .map(e -> e.getValue())
          .doOnNext(srv -> mapBuckets.remove(srv.id()))
          .subscribe();
    }

    private Optional<Bucket> search(String key) {
      return mapBuckets.values().stream().filter(b -> b.hasKey(key)).findFirst();
    }

    public String assign(String key) {
      if (key == null)
        throw new TopicLoadBalancerException("key is null");
      Optional<Bucket> bucket = search(key);
      if (bucket.isPresent()) {
        return bucket.get().getId();
      }

      bucket = mapBuckets.values().stream().min(Comparator.comparing(Bucket::size));
      if (bucket.isPresent()) {
        bucket.get().add(key);
        return bucket.get().getId();
      } else {
        throw new TopicLoadBalancerException("no service found to loadbalance for topic " + name);
      }
    }
  }

  public static final class Bucket {

    private final Set<String> keySet = new ConcurrentSkipListSet<>();
    private final String id;

    public Bucket(String id) {
      this.id = id;
    }

    public void add(String key) {
      this.keySet.add(key);
    }

    public boolean hasKey(String key) {
      return this.keySet.contains(key);
    }

    public int size() {
      return this.keySet.size();
    }

    public String getId() {
      return id;
    }

  }
}
