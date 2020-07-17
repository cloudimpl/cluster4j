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
public interface CollectionProvider {

  <K, V> Map<K, V> createMap(String identifier, String... valComparator);

  <K, V> NavigableMap<K, V> createSortedMap(String keyField, String valueField, String identifier,
      String... valComparator);

  void close();
}
