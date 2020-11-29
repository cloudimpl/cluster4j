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
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.cloudimpl.cluster4j.common.GsonCodec;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * @author nuwansa
 */
public class AwsDynamodbMap<V> implements Map<String, V> {

    protected final AmazonDynamoDB ddb;
    protected final String tableName;
    protected final String name;
    protected final Table table;
    protected final DynamoDB dynamoDB;

    public AwsDynamodbMap(AmazonDynamoDB ddb, String tableName, String name) {
        this.tableName = tableName;
        this.dynamoDB = new DynamoDB(ddb);
        this.name = name;
        this.ddb = ddb;
        this.table = this.dynamoDB.getTable(tableName);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isEmpty() {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name);
        return out.iterator().hasNext();
    }

    @Override
    public boolean containsKey(Object key) {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name, new RangeKeyCondition("Xvalue").eq((String) key));
        return out.iterator().hasNext();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean replace(String key, V oldValue, V newValue) {
        Item item = new Item().withPrimaryKey("Xkey", name, "Xvalue", key).withString("Xjson", GsonCodec.encodeWithType(newValue));
        PutItemSpec itemSpec = new PutItemSpec().withConditionExpression("(Xjson = :jsonVal and attribute_exists(Xvalue))")
                .withValueMap(new ValueMap()
                        .withString(":jsonVal", GsonCodec.encodeWithType(oldValue))
                );
        try {
            this.table.putItem(itemSpec.withItem(item));
            return true;
        } catch (ConditionalCheckFailedException ex) {
            return false;
        }
    }

    @Override
    public V get(Object key) {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name, new RangeKeyCondition("Xvalue").eq((String) key));
        IteratorSupport<Item, QueryOutcome> ite = out.iterator();
        if (ite.hasNext()) {
            return (V) GsonCodec.decode(ite.next().getString("Xjson"));
        }
        return null;
    }

    @Override
    public V put(String key, V value) {
        Item item = new Item().withPrimaryKey("Xkey", name, "Xvalue", key).withString("Xjson", GsonCodec.encodeWithType(value));
        PutItemOutcome out = this.table.putItem(new PutItemSpec().withItem(item).withReturnValues(ReturnValue.ALL_OLD));
        item = out.getItem();
        if (item == null) {
            return null;
        }
        String json = item.getString("Xjson");
        if (json == null) {
            return null;
        }
        return (V) GsonCodec.decode(json);
    }

    @Override
    public V putIfAbsent(String key, V value) {
        PutItemSpec itemSpec = new PutItemSpec()
                .withExpected(new Expected("Xvalue").ne(key));
        Item item = new Item().withPrimaryKey("Xkey", name, "Xvalue", key).withString("Xjson", GsonCodec.encodeWithType(value));
        itemSpec = itemSpec.withItem(item);
        try {
            this.table.putItem(itemSpec);
            return null;
        }catch(ConditionalCheckFailedException ex)
        {
            return get(key);
        }
    }

    @Override
    public V remove(Object key) {
        DeleteItemOutcome out = this.table.deleteItem(new DeleteItemSpec().withPrimaryKey("Xkey", name, "Xvalue", key).withReturnValues(ReturnValue.ALL_OLD));
        Item item = out.getItem();
        String json = item.getString("Xjson");
        if (json == null) {
            return null;
        }
        return (V) GsonCodec.decode(json);
    }

    @Override
    public void putAll(Map<? extends String, ? extends V> m) {
        m.entrySet().forEach(e -> put(e.getKey(), e.getValue()));
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<String> keySet() {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name);
        Set<String> hash = new HashSet<>();
        out.iterator().forEachRemaining(item -> hash.add(item.getString("Xvalue")));
        return hash;
    }

    @Override
    public Collection<V> values() {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name);
        List<V> list = new LinkedList<>();
        out.iterator().forEachRemaining(item -> list.add((V) GsonCodec.decode(item.getString("Xjson"))));
        return list;
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        ItemCollection<QueryOutcome> out = this.table.query("Xkey", this.name);
        Set<Entry<String, V>> list = new HashSet<>();
        out.iterator().forEachRemaining(item -> list.add(new EntryImpl<>(item.getString("Xkey"), (V) GsonCodec.decode(item.getString("Xjson")))));
        return list;
    }

}
