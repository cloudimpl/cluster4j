
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.Map;

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

/**
 *
 * @author nuwansa
 */
public class Bean {
    private String clazz;
    private Map<String,String> configs;

    public String getClazz() {
        return clazz;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }

    @Override
    public String toString() {
        return "Bean{" + "clazz=" + clazz + ", configs=" + configs + '}';
    }
    
    
    public Object toObj()
    {
        try {
            Gson gson = new Gson();
            JsonElement el = gson.toJsonTree(configs);
            return gson.fromJson(el, Class.forName(clazz));
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }

    }
    
}
