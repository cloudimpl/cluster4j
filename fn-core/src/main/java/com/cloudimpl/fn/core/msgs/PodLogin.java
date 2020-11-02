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
public class PodLogin {
    private final String myPodId;
    private final String myPodIp;
    private final String myPodSecret;

    public PodLogin(String myPodId, String myPodIp, String myPodSecret) {
        this.myPodId = myPodId;
        this.myPodIp = myPodIp;
        this.myPodSecret = myPodSecret;
    }

    public String getMyPodId() {
        return myPodId;
    }

    public String getMyPodIp() {
        return myPodIp;
    }

    public String getMyPodSecret() {
        return myPodSecret;
    }
    
    
}
