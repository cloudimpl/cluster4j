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
package com.cloudimpl.cluster.collection;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author nuwansa
 */
public class CollectionOptions {

    private final Map<String, Object> options;

    public CollectionOptions(Map<String, Object> options) {
        this.options = options;
    }

    public void put(String optional, Object value) {
        this.options.put(optional, value);
    }

    public <T> Optional<T> get(String option) {
        Object val = options.get(option);
        if (val == null) {
            return Optional.empty();
        } else {
            return Optional.of((T) val);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }
    
    public static final class Builder {

        private final Map<String, Object> options = new ConcurrentHashMap<>();

        public Builder withOption(String option, Object value) {
            this.options.put(option, value);
            return this;
        }

        public CollectionOptions build() {
            return new CollectionOptions(Collections.unmodifiableMap(options));
        }
    }
}
