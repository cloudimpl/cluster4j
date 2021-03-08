/*
 * Copyright 2021 nuwan.
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
package com.cloudimpl.msglib.example;

import com.cloudimpl.msg.lib.JsonMsg;
import com.cloudimpl.msg.lib.XJsonParser;

/**
 *
 * @author nuwan
 */
public class Main {
    public static void main(String[] args) throws ClassNotFoundException {
//        Class.forName(TestMsg.class.getName());
 //       System.out.println(TestMsg.class.getName());
        XJsonParser parser = new XJsonParser(new XJsonParser.Listener() {
            @Override
            public void onMsg(JsonMsg msg) {
    //            System.out.println("msg:"+msg);
            }
        });
        
       String json = "{\n" +
               "  \"_type\": \"com.cloudimpl.msglib.example.TestMsg\",\n" +
"  \"orderType\": \"MARKET\",\n" +
"  \"session\": \"NORMAL\",\n" +
"  \"duration\": \"DAY\",\n" +
"  \"orderStrategyType\": \"SINGLE\",\n" +
"  \"orderLegCollection\": [\n" +
"    {\n" +
"      \"instruction\": \"Buy\",\n" +
"      \"quantity\": 15,\n" + 
"      \"instrument\": {\n" +
"        \"symbol\": \"XYZ\",\n" +
"        \"assetType\": \"EQUITY\"\n" +
"      }\n" +
"    }\n" +
"  ]\n" +
"}";
       long start = System.currentTimeMillis();
       long rate = 0;
       while(true)
       {
           parser.parse(json);
           long end = System.currentTimeMillis();
           rate++;
           if(end - start >= 1000)
           {
               System.out.println(rate);
               start = System.currentTimeMillis();
               rate = 0;
           }
       }
       
      
    }
}
