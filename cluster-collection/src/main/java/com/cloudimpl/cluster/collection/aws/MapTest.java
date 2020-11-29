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

import com.cloudimpl.cluster.collection.CollectionOptions;
import java.util.Map;
import java.util.NavigableMap;

/**
 *
 * @author nuwansa
 */
public class MapTest {
    public static void main(String[] args) {
        AwsCollectionProvider provider = new AwsCollectionProvider("http://localhost:4566");
        provider.createMapTable("Test");
       Map<String,Student> map = provider.createHashMap("test", CollectionOptions.builder().withOption("TableName", "Test").build());
       Student s = map.put("test",new Student("aa"));
        System.out.println("contain key : "+map.containsKey("test"));
       Student r = map.remove("test");
        System.out.println("out:"+s + " removed: "+r);
        
        NavigableMap<String,Student> map2 = provider.createNavigableMap("test2", CollectionOptions.builder().withOption("TableName", "Test").build());
        map2.put("5", new Student("aaa"));
        map2.put("1", new Student("aaa"));
        map2.put("3", new Student("aaa"));
       // map2.put("8", new Student("aaa"));
        
        boolean passed = map2.replace("8", new Student("aaa"),new Student("bbb"));
        System.out.println("map2.get: "+map2.get("8"));
        System.out.println("passed: "+passed);
        System.out.println(map2.keySet());
    }
    
   public static final class Student
   {
       private final String name;

        public Student(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Student{" + "name=" + name + '}';
        }
       
        
   }
}
