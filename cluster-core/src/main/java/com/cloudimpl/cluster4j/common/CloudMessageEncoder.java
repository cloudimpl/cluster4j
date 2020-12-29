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
package com.cloudimpl.cluster4j.common;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;

/**
 *
 * @author nuwansa
 */
public class CloudMessageEncoder implements JsonSerializer<CloudMessage>{

    @Override
    public JsonElement serialize(CloudMessage t, Type type, JsonSerializationContext jsc) {
         System.out.println(" encoding "+t.getClass());
         JsonObject json = new JsonObject();
         json.addProperty("key", t.getKey());
         json.add("data", GsonCodec.encodeToJsonWithType(t.data()));
         json.add("meta",GsonCodec.encodeToJson(t.meta));
        return json;
    }
    
}
