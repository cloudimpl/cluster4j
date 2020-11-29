/*
 * Copyright 2020 nuwansa.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.cluster.collection.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.cloudimpl.cluster4j.common.GsonCodec;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

/**
 *
 * @author nuwansa
 */
public class AwsDynamodbNavigableMap<V> extends AwsDynamodbMap<V> implements NavigableMap<String, V> {

    public AwsDynamodbNavigableMap(AmazonDynamoDB ddb, String tableName, String name) {
        super(ddb, tableName, name);
    }

    @Override
    public Entry<String, V> lowerEntry(String key) {
        ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                .withScanIndexForward(false).withHashKey("Xkey", this.name).withRangeKeyCondition(new RangeKeyCondition("Xvalue").lt(key)).withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public String lowerKey(String key) {
        Entry<String, ?> entry = lowerEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<String, V> floorEntry(String key) {
        ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                .withScanIndexForward(false).withHashKey("Xkey", this.name).withRangeKeyCondition(new RangeKeyCondition("Xvalue").le(key)).withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public String floorKey(String key) {
        Entry<String, ?> entry = floorEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<String, V> ceilingEntry(String key) {
        ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                .withHashKey("Xkey", this.name)
                .withRangeKeyCondition(new RangeKeyCondition("Xvalue").ge(key)).withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public String ceilingKey(String key) {
        Entry<String, ?> entry = ceilingEntry(key);
        if (entry == null) {
            return null;
        }
        return entry.getKey();
    }

    @Override
    public Entry<String, V> higherEntry(String key) {
        ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                .withHashKey("Xkey", this.name)
                .withRangeKeyCondition(new RangeKeyCondition("Xvalue").gt(key)).withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public String higherKey(String key) {
        Entry<String,?> entry = higherEntry(key);
        if(entry == null)
            return null;
        return entry.getKey();
    }

    @Override
    public Entry<String, V> firstEntry() {
         ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                 .withHashKey("Xkey", this.name)
                .withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public Entry<String, V> lastEntry() {
         ItemCollection<QueryOutcome> items = table.query(new QuerySpec()
                 .withHashKey("Xkey", this.name)
                 .withScanIndexForward(true)
                .withMaxResultSize(1));
        IteratorSupport<Item, QueryOutcome> ite = items.iterator();
        if (ite.hasNext()) {
            Item item = ite.next();
            return new EntryImpl<>(item.getString("Xvalue"), (V) GsonCodec.decode(item.getString("Xjson")));
        }
        return null;
    }

    @Override
    public Entry<String, V> pollFirstEntry() {
        Entry<String,V> entry = firstEntry();
        if(entry != null)
             remove(entry.getKey());
        return entry;              
    }

    @Override
    public Entry<String, V> pollLastEntry() {
        Entry<String,V> entry = lastEntry();
        if(entry != null)
             remove(entry.getKey());
        return entry;    
    }

    @Override
    public NavigableMap<String, V> descendingMap() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NavigableSet<String> navigableKeySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NavigableSet<String> descendingKeySet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NavigableMap<String, V> subMap(String fromKey, boolean fromInclusive, String toKey, boolean toInclusive) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NavigableMap<String, V> headMap(String toKey, boolean inclusive) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public NavigableMap<String, V> tailMap(String fromKey, boolean inclusive) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<String, V> subMap(String fromKey, String toKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<String, V> headMap(String toKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<String, V> tailMap(String fromKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Comparator<? super String> comparator() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String firstKey() {
        Entry<String,?> entry = firstEntry();
        if(entry != null)
            return entry.getKey();
        return null;
    }

    @Override
    public String lastKey() {
       Entry<String,?> entry = lastEntry();
        if(entry != null)
            return entry.getKey();
        return null;
    }

}
