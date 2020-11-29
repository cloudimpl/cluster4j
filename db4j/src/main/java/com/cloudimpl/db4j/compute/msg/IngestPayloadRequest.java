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
package com.cloudimpl.db4j.compute.msg;

import java.util.List;

/**
 *
 * @author nuwansa
 */
public class IngestPayloadRequest {
    private final String organization;
    private final String key;
    private final String collectionName;
    private final List<String> messages;

    public IngestPayloadRequest(String organization, String key, String collectionName, List<String> messages) {
        this.organization = organization;
        this.key = key;
        this.collectionName = collectionName;
        this.messages = messages;
    }

    public String getOrganization() {
        return organization;
    }

    public String getKey() {
        return key;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<String> getMessages() {
        return messages;
    }
   
}
