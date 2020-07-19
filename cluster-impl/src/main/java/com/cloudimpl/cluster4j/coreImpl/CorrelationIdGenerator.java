package com.cloudimpl.cluster4j.coreImpl;


import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class CorrelationIdGenerator {
  private final String cidPrefix;
  private final AtomicLong counter = new AtomicLong(0);

  public CorrelationIdGenerator(String cidPrefix) {
    this.cidPrefix = Objects.requireNonNull(cidPrefix, "cidPrefix");
  }

  public String nextCid() {
    return cidPrefix + "-" + counter.incrementAndGet();
  }
}
