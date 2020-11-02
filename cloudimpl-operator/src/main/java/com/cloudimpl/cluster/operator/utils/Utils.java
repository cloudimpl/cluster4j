package com.cloudimpl.cluster.operator.utils;


import com.cloudimpl.cluster.operator.crd.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;

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
public class Utils {

    public static ResourceRequirements getResourceReq(Container container) {
        return new ResourceRequirementsBuilder() //
                .addToLimits("cpu", new Quantity(container.getResources().getLimits().getCpu())) //
                .addToLimits("memory", new Quantity(container.getResources().getLimits().getMemory())) //
                .addToRequests("cpu", new Quantity(container.getResources().getRequests().getCpu())) //
                .addToRequests("memory", new Quantity(container.getResources().getRequests().getMemory())) //
                .build();
    }
}
