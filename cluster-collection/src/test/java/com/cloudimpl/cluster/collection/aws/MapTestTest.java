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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import cloud.localstack.LocalstackTestRunner;
import cloud.localstack.awssdkv1.TestUtils;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.cloudimpl.cluster.collection.CollectionOptions;
import java.util.Map;
import org.junit.runner.RunWith;
/**
 *
 * @author nuwansa
 */
//@RunWith(LocalstackTestRunner.class)
//@LocalstackDockerProperties(services = { "s3", "sqs", "kinesis" })
//public class MapTestTest {
//    
//    AwsCollectionProvider provider = null;
//    public MapTestTest() {
//        provider = new AwsCollectionProvider(TestUtils.getClientDynamoDB());
//        provider.createMapTable("Test");
//    }
//
//  
//    /**
//     * Test of main method, of class MapTest.
//     */
//   @org.junit.Test
//    @org.junit.jupiter.api.Test
//    public void testMain() {
//       // AwsCollectionProvider provider = new AwsCollectionProvider(TestUtils.getClientDynamoDB());
//       // provider.createMapTable("Test");
//       Map<String,MapTest.Student> map = provider.createHashMap("test", CollectionOptions.builder().withOption("TableName", "Test").build());
//       MapTest.Student s = map.put("test",new MapTest.Student("aa"));
//       assertTrue(map.containsKey("test"));
//       MapTest.Student r = map.remove("test");
//       assertFalse(map.containsKey("test"));
//    }
//    
//}
