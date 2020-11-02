/**
 * Copyright 2012-2015 Niall Gallagher
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.metrics.lib;

import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import static com.googlecode.cqengine.codegen.AttributeBytecodeGenerator.createAttributes;
import com.googlecode.cqengine.query.Query;
import static com.googlecode.cqengine.query.QueryFactory.*;
import com.googlecode.cqengine.query.parser.sql.SQLParser;
import com.googlecode.cqengine.resultset.ResultSet;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates generating attributes on-the-fly using reflection for fields in a POJO, building indexes on those
 * attributes on-the-fly, and then running queries against fields in the POJO.
 *
 * @author ngallagher
 * @since 2013-07-05 11:54
 */
public class DynamicExample {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws InterruptedException {
        // Generate attributes dynamically for fields in the given POJO...
        Map<String, Attribute<Car, Comparable>> attributes = DynamicIndexer.generateAttributesForPojo(Car.class);
        // Build indexes on the dynamically generated attributes...
        IndexedCollection<Car> cars = DynamicIndexer.newAutoIndexedCollection(attributes.values());

        List<String> manufacturer = new LinkedList<>();
        manufacturer.addAll(Arrays.asList("ford","honda","toyota","benz"));
        List<String> model = new LinkedList<>();
        model.addAll(Arrays.asList("A","A","A","A","B","B","B","B","C","C","C","C","D","D","D","D"));
        // Add some objects to the collection...
        int i = 0;
        while(i < 1000_000)
        {
            cars.add(new Car(i, manufacturer.get(i % 4),model.get(i % 16), i , i * 1000));
            i++;
        }
        
      
        Query<Car> query = and(
                equal(attributes.get("manufacturer"), "ford"),
                lessThan(attributes.get("doors"), value(5)),
                greaterThan(attributes.get("horsepower"), value(3000))
        );
        ResultSet<Car> results = cars.retrieve(query);

        System.out.println("Ford cars with less than 5 doors and horsepower greater than 3000:- ");
        System.out.println("Using NavigableIndex: " + (results.getRetrievalCost() == 40));
        for (Car car : results) {
            System.out.println(car);
        }
        
        SQLParser<Car> parser = SQLParser.forPojoWithAttributes(Car.class, createAttributes(Car.class));
        long s = System.nanoTime();
        ResultSet<Car> rs = parser.retrieve(cars, "SELECT * FROM cars order by manufacturer,model");
        System.out.println("time : "+(System.nanoTime() - s));
      //  rs.forEach(System.out::println); 
        Thread.sleep(10000000);
        // Prints:
        //    Ford cars with less than 5 doors and horsepower greater than 3000:-
        //    Using NavigableIndex: true
        //    Car{carId=1, manufacturer='ford', model='focus', doors=4, horsepower=9000}
    }



    // This method is required for compatibility with Java 8 compiler (not required for Java 6 or 7 compiler)...
    static Comparable value(Comparable c) {
        return c;
    }
}