/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author nuwansa
 */
public class DynamodbMap<K, V> implements Map<K, V> {

  private final Table table;
  private final String identifier;
  private final Set<String> fields;

  public DynamodbMap(String identifier, Table table, String... valueComparator) {
    this.table = table;
    this.identifier = identifier;
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

  private String createKey(String key) {
    return String.join(":", identifier, key);
  }

  @Override
  public boolean containsKey(Object key) {
    String strKey = createKey(GsonCodec.encode(key));
    GetItemSpec itemSpec = new GetItemSpec().withPrimaryKey("key", strKey)
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

    String strKey = createKey(GsonCodec.encode(key));
    JsonObject json = GsonCodec.encodeToJson(oldValue).getAsJsonObject();
    JsonObject newJson = GsonCodec.encodeToJson(newValue).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD)
        .withExpected(new Expected("key").eq(strKey));

    itemSpec = checkEqual(itemSpec, json);

    Item item = new Item().withPrimaryKey("key", strKey, "value", strKey).with("_data", newJson.toString())
        .with("_type", newValue.getClass().getName());

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
    String strKey = createKey(GsonCodec.encode(key));
    GetItemSpec itemSpec = new GetItemSpec().withPrimaryKey("key", strKey, "value", strKey)
        .withConsistentRead(true);
    Item item = table.getItem(itemSpec);
    if (item == null) {
      return null;
    }
    return GsonCodec.decode(type(item.getString("_type")), item.getString("_data"));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    String strKey = createKey(GsonCodec.encode(key));
    JsonObject json = GsonCodec.encodeToJson(value).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD)
        .withExpected(new Expected("key").ne(strKey));


    Item item = new Item().withPrimaryKey("key", strKey, "value", strKey).with("_data", json.toString())
        .with("_type", value.getClass().getName());

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
    String strKey = createKey(GsonCodec.encode(key));
    JsonObject json = GsonCodec.encodeToJson(value).getAsJsonObject();

    PutItemSpec itemSpec = new PutItemSpec().withReturnValues(ReturnValue.ALL_OLD);
    Item item = new Item().withPrimaryKey("key", strKey, "value", strKey).with("_data", json.toString())
        .with("_type", value.getClass().getName());

    item = fillFields(item, json);

    itemSpec.withItem(item);
    PutItemOutcome result = table.putItem(itemSpec);
    item = result.getItem();
    if (item == null) {
      return null;
    }

    String data = item.getString("_data");
    if (data != null) {
      return GsonCodec.decode(type(item.getString("_type")), data);
    }
    return null;
  }

  @Override
  public V remove(Object key) {
    String strKey = createKey(GsonCodec.encode(key));
    DeleteItemSpec itemSpec = new DeleteItemSpec().withPrimaryKey("key", strKey)
        .withReturnValues(ReturnValue.ALL_OLD);
    DeleteItemOutcome rs = table.deleteItem(itemSpec);
    Item item = rs.getItem();
    String data = item.getString("_data");
    if (data != null) {
      return GsonCodec.decode(type(item.getString("_type")), data);
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
    throw CollectionException
        .OPERATION_NOT_SUPPORTED(err -> err.setOpName(String.join(".", this.getClass().getName(), "keySet")));

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

  public static void main(String[] args) {

    System.setProperty("sqlite4java.library.path", "native-libs");


    // AmazonDynamoDBLocal local = DynamoDBEmbedded.create();
    // AmazonDynamoDB dynamodb = local.amazonDynamoDB();
    AwsCollectionProvider provider = AwsCollectionProvider.local("http://localhost:8000", "Test");
    // createTable(provider.getClient(), "Test", "key", "value");
    Collection col = Collection.builder()
        .withProvider(provider)
        .build();

    Map<String, Item2> map = col.map("test", "s");

    // map.putIfAbsent("test", new Item2("dasdasd"));
    System.out.println(map.get("test"));
    // map.putIfAbsent("test", new Item2("sfsaf"));
    map.put("test", new Item2("nuwan snajeewa2"));
    System.out.println(map.get("test"));

    // col.close();
    provider.close();
  }
}
