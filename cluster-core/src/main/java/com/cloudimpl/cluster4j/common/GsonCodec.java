package com.cloudimpl.cluster4j.common;

import com.cloudimpl.cluster4j.core.CloudUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.google.gson.internal.LinkedTreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class GsonCodec {

    private static final ThreadLocal<Gson> THR_GSON = ThreadLocal.withInitial(() -> createGson(false));
    private static final ThreadLocal<Gson> THR_GSON_PRINTER = ThreadLocal.withInitial(() -> createGson(true));
    private static final ThreadLocal<JsonParser> THR_GSON_PARSER = ThreadLocal.withInitial(() -> new JsonParser());
    private static final Map<Class<?>, Supplier<JsonSerializer<?>>> serializers = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Supplier<JsonDeserializer<?>>> deSerializers = new ConcurrentHashMap<>();

    private GsonCodec() {

    }

    public static void registerTypeAdaptor(Class<?> cls, Supplier<JsonDeserializer<?>> supplierDeserializer, Supplier<JsonSerializer<?>> supplierSerializer) {
        serializers.put(cls, supplierSerializer);
        deSerializers.put(cls, supplierDeserializer);
    }
//  private static Gson getGson() {
//    Gson gson = THR_GSON.get();
//    if (gson == null) {
//      gson = new GsonBuilder().create();
//      THR_GSON.set(gson);
//    }
//    return gson;
//  }
//
//  private static JsonParser getGsonParser() {
//    JsonParser gsonParser = THR_GSON_PARSER.get();
//    if (gsonParser == null) {
//      gsonParser = new JsonParser();
//      THR_GSON_PARSER.set(gsonParser);
//    }
//    return gsonParser;
//  }

    private static Gson createGson(boolean pretty) {
        GsonBuilder builder = new GsonBuilder();
        if(pretty)
            builder.setPrettyPrinting();
        serializers.entrySet().forEach(s -> builder.registerTypeAdapter(s.getKey(), s.getValue().get()));
        deSerializers.entrySet().forEach(s -> builder.registerTypeAdapter(s.getKey(), s.getValue().get()));
        return builder.create();
    }

    public static String encode(Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        }
        return THR_GSON.get().toJson(obj);
    }

    public static Gson getGson()
    {
        return THR_GSON.get();
    }
    
    public static String encodePretty(Object obj) {
        return THR_GSON_PRINTER.get().toJson(obj);
    }

    public static String encodeWithType(Object obj) {
        if (obj instanceof String) {
            return (String) obj;
        }
        JsonElement el = encodeToJson(obj);
        if (el.isJsonObject()) {
            JsonObject json = el.getAsJsonObject();
            json.addProperty("_type", obj.getClass().getName());
            el = json;
        }
        return THR_GSON.get().toJson(el);
    }

    public static String encodeWithType2(Object obj) {
        JsonElement el = encodeToJson(obj);
        if (el.isJsonObject()) {
            JsonObject json = el.getAsJsonObject();
            json.addProperty("_type", obj.getClass().getName());
            el = json;
        }
        return THR_GSON.get().toJson(el);
    }
    
    public static JsonElement encodeToJsonWithType(Object obj) {
        if (obj instanceof String) {
            return new JsonPrimitive((String) obj);
        }
        JsonElement el = encodeToJson(obj);
        if (el.isJsonObject()) {
            el.getAsJsonObject().addProperty("_type", obj.getClass().getName());
        } else {
            JsonObject json = new JsonObject();
            json.add("el", el);
            json.addProperty("_type", obj.getClass().getName());
            el = json;
        }
        return el;
    }

    public static JsonElement encodeToJson(Object obj) {
        if (obj instanceof String) {
            return new JsonPrimitive((String) obj);
        }

        return THR_GSON.get().toJsonTree(obj);
    }

    public static <T> T decode(Class<T> clazz, String data) {
        return THR_GSON.get().fromJson(data, clazz);
    }

    public static JsonObject toJsonObject(String data) {
        return THR_GSON_PARSER.get().parse(data).getAsJsonObject();
    }
    
    public static JsonElement toJsonElement(String data) {
        return THR_GSON_PARSER.get().parse(data);
    }

    public static <T> T decode(Class<T> clazz, Map<String, String> data) {
        if (clazz == String.class) {
            return (T) data.keySet().stream().filter(key -> !key.startsWith("@")).findFirst().orElse(null);
        } else if (clazz == int.class) {
            return (T) Integer.valueOf(data.keySet().iterator().next());
        }
        JsonElement jsonElement = THR_GSON.get().toJsonTree(data);
        return THR_GSON.get().fromJson(jsonElement, clazz);
    }

    public static <T> T decodeTree(Class<T> clazz, LinkedTreeMap data) {
        JsonElement jsonElement = THR_GSON.get().toJsonTree(data);
        return THR_GSON.get().fromJson(jsonElement, clazz);
    }

    public static Object decode(String json)
    {
        if(isJsonEl(json.charAt(0)))
        {
            JsonElement el = JsonParser.parseString(json);
            return decode(el);
        }else
            return json;
    }
    
    
    private static boolean isJsonEl(char c)
    {
        return c == '{' || c == '}' || c == '[' || c == ']';
    }
    
    public static Object decode(JsonElement elem) {

        if (elem.isJsonObject()) {
            JsonElement msgTypeElement = elem.getAsJsonObject().get("_type");
            if (msgTypeElement != null) {
                String msgType = msgTypeElement.getAsString();
                Class<?> claz = CloudUtil.classForName(msgType);
                return THR_GSON.get().fromJson(elem, claz);
            } else {
                return THR_GSON.get().fromJson(elem, Map.class);
            }
        } else if (elem.isJsonNull()) {
            return null;
        } else if (elem.isJsonArray()) {
            List lst = new LinkedList<>();
            JsonArray array = elem.getAsJsonArray();
            for (int i = 0; i < array.size(); ++i) {
                lst.add(decode(array.get(i)));
            }
            return lst;
        } else if (elem.isJsonPrimitive()) {
            JsonPrimitive p = elem.getAsJsonPrimitive();
            if (p.isBoolean()) {
                return p.isBoolean();
            } else if (p.isNumber()) {
                return p.getAsNumber();
            } else {
                return p.getAsString();
            }
        }
        return null;
    }
}
