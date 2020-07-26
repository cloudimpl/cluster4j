
import java.util.List;

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
public class AppConfig {
    private int port;
    private String name;
    private List<Bean> list;

    public int getPort() {
        return port;
    }

    public String getName() {
        return name;
    }

    public List<Bean> getList() {
        return list;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setList(List<Bean> list) {
        this.list = list;
    }
    
    
}
