package com.cloudimpl.cluster4j.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LinkedTreeMap;
import java.util.Map;

public class GsonCodec {

  private static final ThreadLocal<Gson> THR_GSON = ThreadLocal.withInitial(()->new GsonBuilder().create());
  private static final ThreadLocal<JsonParser> THR_GSON_PARSER = ThreadLocal.withInitial(()->new JsonParser());

  private GsonCodec() {

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

  public static String encode(Object obj) {
    if (obj instanceof String) {
      return (String) obj;
    }
    return THR_GSON.get().toJson(obj);
  }

   public static String encodeWithType(Object obj) {
    if (obj instanceof String) {
      return (String) obj;
    }
    JsonElement el = encodeToJson(obj);
    if(el.isJsonObject())
    {
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
        if(el.isJsonObject())
        {
            el.getAsJsonObject().addProperty("_type", obj.getClass().getName());
        }
        else
        {
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
}
