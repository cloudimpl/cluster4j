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
package test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author nuwan
 */
public interface MemoryLayoutEx {
    
    String name();
    
    int byteSize();
    
    MemoryLayoutEx parent();
    
    public static final class GroupLayout implements MemoryLayoutEx
    {
        private final Map<String,MemoryLayoutEx> map = new HashMap<>();
        private final String name;
        public GroupLayout(String name) {
            this.name = name;
        }
        
        
        protected void register(MemoryLayoutEx layout)
        {
            MemoryLayoutEx old = map.putIfAbsent(layout.name(), layout);
            if(old != null)
                throw new RuntimeException("duplicate layout "+layout.name());
        }
        
        public MemoryLayoutEx layout(String name)
        {
            return map.get(name);
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public int byteSize() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public MemoryLayoutEx parent() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
