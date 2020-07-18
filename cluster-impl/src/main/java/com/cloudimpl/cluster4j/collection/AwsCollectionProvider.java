/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author nuwansa
 */
public class AwsCollectionProvider implements CollectionProvider {

  private final AmazonDynamoDB client;
  private final DynamoDB dynamodb;
  private final String defaultTableName;
  private final Table defaultTable;
  private final Map<String, String> mapToTable = new ConcurrentHashMap<>();
  private final Map<String, Table> tables = new ConcurrentHashMap<>();

  private AwsCollectionProvider(String defaultTableName, AwsClientBuilder.EndpointConfiguration endpoint,
      AWSCredentialsProvider credentialProvider) {

    client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(endpoint)
        .withCredentials(credentialProvider).build();
    dynamodb = new DynamoDB(client);
    this.defaultTableName = defaultTableName;
    this.defaultTable = dynamodb.getTable(defaultTableName);
  }

  public void registerTable(String identifier, String table) {
    mapToTable.put(identifier, table);
  }

  private Table getTable(String identifier) {
    String table = mapToTable.get(identifier);
    if (table == null)
      return defaultTable;
    else
      return tables.computeIfAbsent(identifier, id -> dynamodb.getTable(id));
  }

  public DynamoDB getDynamodb() {
    return dynamodb;
  }

  public AmazonDynamoDB getClient() {
    return client;
  }

  private AwsCollectionProvider(String defaultTableName, AmazonDynamoDB client) {
    this.client = null;
    this.dynamodb = new DynamoDB(client);
    this.defaultTableName = defaultTableName;
    this.defaultTable = dynamodb.getTable(defaultTableName);
  }


  @Override
  public <K, V> Map<K, V> createMap(String identifier, String... valComparator) {
    return new DynamodbMap<>(identifier, getTable(identifier), valComparator);
  }

  @Override
  public <K, V> NavigableMap<K, V> createSortedMap(String keyField, String valueField, String identifier,
      String... valComparator) {
    return new DynamodbSortedMap<>(keyField, valueField, identifier, getTable(identifier), valComparator);
  }

  @Override
  public void close() {
    dynamodb.shutdown();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static AwsCollectionProvider localEmbedded(AmazonDynamoDB client, String table) {
    return new AwsCollectionProvider(table, client);
  }

  public static AwsCollectionProvider local(String endpoint, String defaultTableName) {
    return AwsCollectionProvider.builder()
        .withCredentialProvider(new AWSStaticCredentialsProvider(new AWSCredentials() {
          @Override
          public String getAWSAccessKeyId() {
            return "xxxx";
          }

          @Override
          public String getAWSSecretKey() {
            return "yyyy";
          }
        })).withEndpoint(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
        .withDefaultTableName(defaultTableName)
        .build();
  }

  public static final class Builder {

    private AwsClientBuilder.EndpointConfiguration endpoint;
    private AWSCredentialsProvider credentialProvider;
    private String defaultTableName;

    public Builder withEndpoint(AwsClientBuilder.EndpointConfiguration endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public Builder withCredentialProvider(AWSCredentialsProvider credentialProvider) {
      this.credentialProvider = credentialProvider;
      return this;
    }

    public Builder withDefaultTableName(String tableName) {
      this.defaultTableName = tableName;
      return this;
    }

    public AwsCollectionProvider build() {
      return new AwsCollectionProvider(defaultTableName, endpoint, credentialProvider);
    }
  }
}
