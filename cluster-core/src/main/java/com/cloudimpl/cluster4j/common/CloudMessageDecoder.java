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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 * @author nuwansa
 */
public class CloudMessageDecoder implements JsonDeserializer<CloudMessage> {

    @Override
    public CloudMessage deserialize(JsonElement je, Type type, JsonDeserializationContext jdc) throws JsonParseException {
        System.out.println(" decoding " + je.toString());
        JsonObject json = je.getAsJsonObject();
        JsonElement key = json.get("key");
        if (key == null) {
            return new CloudMessage(GsonCodec.decode(json.get("data")), null,
                     (Map<String, String>) GsonCodec.decode(json.get("meta")));
        } else {
            return new CloudMessage(GsonCodec.decode(json.get("data")), key == null ? null : key.getAsString(),
                     (Map<String, String>) GsonCodec.decode(json.get("meta")));
        }
    }

}
