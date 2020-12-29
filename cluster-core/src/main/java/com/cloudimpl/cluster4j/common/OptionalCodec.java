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
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 *
 * @author nuwansa
 */
public class OptionalCodec implements JsonDeserializer<Optional<?>>, JsonSerializer<Optional<?>> {

    @Override
    public Optional<?> deserialize(JsonElement je, Type type, JsonDeserializationContext jdc) throws JsonParseException {
        JsonElement el = je.getAsJsonObject().get("value");
        if(el == JsonNull.INSTANCE)
            return Optional.empty();
        else
            return Optional.of(GsonCodec.decode(el));
    }

    @Override
    public JsonElement serialize(Optional<?> t, Type type, JsonSerializationContext jsc) {
        JsonObject optional = new JsonObject();
        optional.addProperty("_type", Optional.class.getName());
        if(t.isPresent())
        {
           optional.add("value", GsonCodec.encodeToJsonWithType(t.get())); 
        }
        else
            optional.add("value", JsonNull.INSTANCE);
        return optional;
    }
    
}
