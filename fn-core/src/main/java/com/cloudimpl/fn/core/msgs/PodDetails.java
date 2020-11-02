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
public class PodDetails {
    private final String podName;
    private final String podIp;
    private final String podStatus;
    public PodDetails(String podName, String podIp,String podStatus) {
        this.podName = podName;
        this.podIp = podIp;
        this.podStatus = podStatus;
    }

    public String getPodName() {
        return podName;
    }

    public String getPodIp() {
        return podIp;
    }

    public String getPodStatus() {
        return podStatus;
    }
    
    
    
}
