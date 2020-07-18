/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.cloudimpl.cluster4j.collection.error.CollectionException;
import com.cloudimpl.cluster4j.common.GsonCodec;
import com.cloudimpl.cluster4j.core.TimeUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 *
 * @author nuwansa
 */
public class DynamodbSortedMap<K, V> implements NavigableMap<K, V> {

  private final Table table;
  private final String identifier;
  private final Set<String> fields;
  private final String keyField;
  private final String valueField;
  private final boolean descending;

  private DynamodbSortedMap() {
    throw CollectionException.CONSTRUCTOR_NOT_SUPPORTED(err -> {
    });
  }

  public DynamodbSortedMap(String keyField, String valueField, String identifier, Table table,
      String... valueComparator) {
    this(keyField, valueField, identifier, table, false, valueComparator);
  }

  public DynamodbSortedMap(String keyField, String valueField, String identifier, Table table, boolean descending,
      String... valueComparator) {
    this.table = table;
    this.descending = descending;
    this.identifier = identifier;
    this.keyField = keyField;
    this.valueField = valueField;
    fields = Arrays.asList(valueComparator).stream().collect(Collectors.toSet());
  }

  @Override
  public int size() {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "size")));
  }

  @Override
  public boolean isEmpty() {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "isEmpty")));
  }

  @Override
  public boolean containsKey(Object key) {
    String strKey = GsonCodec.encode(key);
    GetItemSpec itemSpec = new GetItemSpec().withPrimaryKey(keyField, identifier, valueField, strKey)

        .withConsistentRead(true);
    Item item = table.getItem(itemSpec);
    return item != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "containsValue")));

  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {

    String strKey = GsonCodec.encode(key);
    JsonObject json = GsonCodec.encodeToJson(oldValue).getAsJsonObject();
    JsonObject newJson = GsonCodec.encodeToJson(newValue).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD)
        .withExpected(new Expected(valueField).eq(strKey));

    itemSpec = checkEqual(itemSpec, json);

    Item item = new Item().withPrimaryKey(keyField, identifier, valueField, strKey).with("data", newJson.toString())
        .with("type", newValue.getClass().getName()).with("creationTime", TimeUtils.currentTimeMillis());

    item = fillFields(item, newJson);

    itemSpec.withItem(item);
    try {
      table.putItem(itemSpec);
      return true;
    } catch (Exception ex) {
      return false;
    }

  }

  private PutItemSpec checkEqual(PutItemSpec itemSpec, JsonObject json) {
    for (String field : fields) {
      JsonElement el = json.get(field);
      if (el == null)
        itemSpec = itemSpec.withExpected(new Expected(field).notExist());
      else
        itemSpec = itemSpec.withExpected(new Expected(field).eq(el.getAsString()));
    }
    return itemSpec;
  }

  @Override
  public V get(Object key) {
    String strKey = GsonCodec.encode(key);
    GetItemSpec itemSpec = new GetItemSpec().withPrimaryKey(keyField, identifier, valueField, strKey)
        .withConsistentRead(true);
    Item item = table.getItem(itemSpec);
    if (item == null) {
      return null;
    }
    return GsonCodec.decode(type(item.getString("type")), item.getString("data"));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    String strKey = GsonCodec.encode(key);
    JsonObject json = GsonCodec.encodeToJson(value).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD)
        .withExpected(new Expected("value").ne(strKey));


    Item item = new Item().withPrimaryKey(keyField, identifier, valueField, strKey).with("data", json.toString())
        .with("type", value.getClass().getName()).with("creationTime", TimeUtils.currentTimeMillis());

    item = fillFields(item, json);

    itemSpec.withItem(item);
    try {
      table.putItem(itemSpec);
    } catch (Exception ex) {
      throw CollectionException.KEY_VIOLATION(err -> err.wrap(ex));
    }
    return null;
  }

  private Item fillFields(Item item, JsonObject json) {
    for (String field : fields) {
      JsonElement val = json.get(field);
      if (val != null)
        item = item.with(field, val.getAsString());
      else
        item = item.withNull(field);

    }
    return item;
  }

  @Override
  public V put(K key, V value) {
    String strKey = GsonCodec.encode(key);
    JsonObject json = GsonCodec.encodeToJson(value).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD);
    Item item = new Item().withPrimaryKey(keyField, identifier, valueField, strKey).with("data", json.toString())
        .with("type", value.getClass().getName()).with("creationTime", TimeUtils.currentTimeMillis());

    item = fillFields(item, json);

    itemSpec.withItem(item);
    PutItemOutcome result = table.putItem(itemSpec);
    item = result.getItem();
    if (item == null) {
      return null;
    }

    String data = item.getString("data");
    if (data != null) {
      return GsonCodec.decode(type(item.getString("type")), data);
    }
    return null;
  }

  @Override
  public V remove(Object key) {
    String strKey = GsonCodec.encode(key);
    DeleteItemSpec itemSpec = new DeleteItemSpec().withPrimaryKey(keyField, identifier, valueField, strKey)
        .withReturnValues(ReturnValue.ALL_OLD);
    DeleteItemOutcome rs = table.deleteItem(itemSpec);
    Item item = rs.getItem();
    String data = item.getString("data");
    if (data != null) {
      return GsonCodec.decode(type(item.getString("type")), data);
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "putAll")));

  }

  @Override
  public void clear() {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "clear")));

  }

  @Override
  public Set<K> keySet() {
    QuerySpec spec = new QuerySpec()
        .withAttributesToGet(valueField)
        .withHashKey(keyField, identifier)
        .withScanIndexForward(!descending);
    // .withQueryFilters(new QueryFilter(keyField).eq(identifier));
    // .withKeyConditionExpression(keyField + " = :v_key")
    // .withValueMap(new ValueMap()
    // .withString(":v_key", identifier));

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    Set<K> keys = new HashSet<>();
    while (ite.hasNext()) {
      Item i = ite.next();
      keys.add((K) i.getString(valueField));
    }
    return keys;
  }

  @Override
  public java.util.Collection<V> values() {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "values")));

  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "entrySet")));
  }

  private <T> Class<T> type(String className) {
    try {
      return (Class<T>) Class.forName(className);
    } catch (ClassNotFoundException ex) {
      throw CollectionException.CLASS_NOT_FOUND(err -> err.setClassName(className).wrap(ex));
    }
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    String strKey = GsonCodec.encode(key);
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key and " + valueField + " < :v_value")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier)
            .withString(":v_value", strKey))
        .withScanIndexForward(false)
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public K lowerKey(K key) {
    return lowerEntry(key).getKey();
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    String strKey = GsonCodec.encode(key);
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key and " + valueField + " <= :v_value")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier)
            .withString(":v_value", strKey))
        .withScanIndexForward(false)
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public K floorKey(K key) {
    return floorEntry(key).getKey();
  }

  @Override
  public Map.Entry<K, V> ceilingEntry(K key) {
    String strKey = GsonCodec.encode(key);
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key and " + valueField + " >= :v_value")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier)
            .withString(":v_value", strKey))
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public K ceilingKey(K key) {
    return ceilingEntry(key).getKey();
  }

  @Override
  public Map.Entry<K, V> higherEntry(K key) {
    String strKey = GsonCodec.encode(key);
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key and " + valueField + " > :v_value")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier)
            .withString(":v_value", strKey))
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public K higherKey(K key) {
    return higherEntry(key).getKey();
  }

  @Override
  public Map.Entry<K, V> firstEntry() {
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier))
        .withScanIndexForward(!descending)
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public Map.Entry<K, V> lastEntry() {
    QuerySpec spec = new QuerySpec()
        .withKeyConditionExpression(keyField + " = :v_key")
        .withValueMap(new ValueMap()
            .withString(":v_key", identifier))
        .withScanIndexForward(descending)
        .withMaxResultSize(1);

    ItemCollection<QueryOutcome> items = table.query(spec);
    Iterator<Item> ite = items.iterator();
    if (ite.hasNext()) {
      Item i = ite.next();
      return new EntryImpl(i.get(valueField), GsonCodec.decode(type(i.getString("type")), i.getString("data")));
    }
    return null;
  }

  @Override
  public Map.Entry<K, V> pollFirstEntry() {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public Map.Entry<K, V> pollLastEntry() {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public NavigableMap<K, V> descendingMap() {
    return new DynamodbSortedMap<>(keyField, valueField, identifier, table, true,
        fields.toArray(new String[fields.size()]));
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public Comparator<? super K> comparator() {
    throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose Tools
                                                                   // | Templates.
  }

  @Override
  public K firstKey() {
    return firstEntry().getKey();
  }

  @Override
  public K lastKey() {
    return lastEntry().getKey();
  }


  public static final class EntryImpl<K, V> implements Map.Entry<K, V> {
    private final K k;
    private final V v;

    public EntryImpl(K k, V v) {
      this.k = k;
      this.v = v;
    }

    @Override
    public K getKey() {
      return k;
    }

    @Override
    public V getValue() {
      return v;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                     // Tools | Templates.
    }

    @Override
    public String toString() {
      return "EntryImpl{" + "k=" + k + ", v=" + v + '}';
    }



  }
  public static class Item2 {
    private String s;

    public Item2(String s) {
      this.s = s;
    }

    @Override
    public String toString() {
      return "Item2{" + "s=" + s + '}';
    }



  }

  private static CreateTableResult createTable(AmazonDynamoDB ddb, String tableName, String hashKeyName,
      String rangeKey) {
    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition(hashKeyName, ScalarAttributeType.S));
    attributeDefinitions.add(new AttributeDefinition(rangeKey, ScalarAttributeType.S));

    List<KeySchemaElement> ks = new ArrayList<>();
    ks.add(new KeySchemaElement(hashKeyName, KeyType.HASH));
    ks.add(new KeySchemaElement(rangeKey, KeyType.RANGE));

    ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);

    CreateTableRequest request = new CreateTableRequest()
        .withTableName(tableName)
        .withAttributeDefinitions(attributeDefinitions)
        .withKeySchema(ks)
        .withProvisionedThroughput(provisionedthroughput);

    return ddb.createTable(request);
  }

//  public static void main(String[] args) {
//
//    System.setProperty("sqlite4java.library.path", "native-libs");
//
//
//    AmazonDynamoDBLocal local = DynamoDBEmbedded.create();
//    AmazonDynamoDB dynamodb = local.amazonDynamoDB();
//    createTable(dynamodb, "test", "id", "val");
//    Collection col = Collection.builder()
//        .withProvider(AwsCollectionProvider.localEmbedded(dynamodb, "test"))
//        .build();
//
//    DynamodbSortedMap<String, Item2> map = (DynamodbSortedMap) col.sortedMap("id", "val", "test", "s");
//
//    map.putIfAbsent("test", new Item2("dasdasd"));
//    System.out.println(map.get("test"));
//    // map.putIfAbsent("test", new Item2("sfsaf"));
//    map.put("test", new Item2("nuwan snajeewa"));
//
//    // col.close();
//    local.shutdown();
//  }
}
