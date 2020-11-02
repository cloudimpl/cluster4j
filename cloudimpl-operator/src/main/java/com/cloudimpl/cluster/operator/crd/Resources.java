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
package com.cloudimpl.cluster.operator.crd;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;

/**
 *
 * @author nuwansa
 */
@JsonDeserialize(using = JsonDeserializer.None.class)
public  class Resources implements KubernetesResource {

    private Requests requests;
    private Limits limits;

    public Resources() {

    }

    public Limits getLimits() {
        return limits;
    }

    public Requests getRequests() {
        return requests;
    }

    public void setLimits(Limits limits) {
        this.limits = limits;
    }

    public void setRequests(Requests request) {
        this.requests = request;
    }

    @Override
    public String toString() {
        return "Resources{" + "requests=" + requests + ", limits=" + limits + '}';
    }

}
