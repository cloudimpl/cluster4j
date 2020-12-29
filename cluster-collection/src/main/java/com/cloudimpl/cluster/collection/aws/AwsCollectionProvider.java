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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.cloudimpl.cluster.collection.CollectionOptions;
import com.cloudimpl.cluster.collection.CollectionProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 * @author nuwansa
 */
public class AwsCollectionProvider implements CollectionProvider {

    private final AmazonDynamoDB ddb;

    public AwsCollectionProvider(AmazonDynamoDB ddb) {
        this.ddb = ddb;
    }
     
    public AwsCollectionProvider(String endpoint) {
        ddb = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_EAST_1.name()))
                // .withRegion(Regions.US_EAST_1)
                .build();
    }

    @Override
    public <V> Map<String, V> createHashMap(String name, CollectionOptions options) {
        return new AwsDynamodbMap<>(ddb, options.<String>get("TableName").get(), name);
    }

    @Override
    public <V> NavigableMap<String, V> createNavigableMap(String name, CollectionOptions options) {
        return new AwsDynamodbNavigableMap<>(ddb, options.<String>get("TableName").get(), name);
    }

    public void createMapTable(String tableName) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Xkey").withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Xvalue").withAttributeType("S"));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Xkey").withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName("Xvalue").withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        ddb.createTable(request);

    }
    
    public boolean isTableExit(String tableName){
        try{
            ddb.describeTable(tableName);
            return true;
        }catch(ResourceNotFoundException ex)
        {
            return false;
        }
        
    }
}
