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
package com.cloudimpl.fn.core.msgs;

/**
 *
 * @author nuwansa
 */
public class FaasMeta {
    private final String podIp;
    private final int podPort;

    public FaasMeta(String podIp, int podPort) {
        this.podIp = podIp;
        this.podPort = podPort;
    }

    public String getPodIp() {
        return podIp;
    }
    
    public int getPodPort() {
        return podPort;
    }
    
    
}
