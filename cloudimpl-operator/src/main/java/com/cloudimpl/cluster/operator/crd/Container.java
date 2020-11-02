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
public  class Container implements KubernetesResource {

    private String name;
    private String image;
    private Resources resources;

    public Container() {

    }

    public void setImage(String image) {
        this.image = image;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setResources(Resources resources) {
        this.resources = resources;
    }

    public String getName() {
        return name;
    }

    public String getImage() {
        return image;
    }

    public Resources getResources() {
        return resources;
    }

    @Override
    public String toString() {
        return "Container{" + "name=" + name + ", image=" + image + ", resources=" + resources + '}';
    }

}
