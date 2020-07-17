/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import java.util.Map;
import java.util.NavigableMap;

/**
 *
 * @author nuwansa
 */
public class Collection {

  private final CollectionProvider provider;

  private Collection(CollectionProvider provider) {
    this.provider = provider;
  }

  public <K, V> Map<K, V> map(String identifier, String... valComparator) {
    return this.provider.createMap(identifier, valComparator);
  }

  public <K, V> NavigableMap<K, V> sortedMap(String keyField, String valueField, String identifier,
      String valComparator) {
    return this.provider.createSortedMap(keyField, valueField, identifier, valComparator);
  }

  public void close() {
    this.provider.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private CollectionProvider provider;

    public Builder withProvider(CollectionProvider provider) {
      this.provider = provider;
      return this;
    }

    public Collection build() {
      return new Collection(provider);
    }
  }
}
