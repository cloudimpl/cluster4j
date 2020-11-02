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

import com.cloudimpl.cluster4j.common.GsonCodec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;

/**
 *
 * @author nuwansa
 */
public class CloudFunctionBundle extends CustomResource{
 
    private CloudFunctionBundleSpec spec;
    
     public CloudFunctionBundleSpec getSpec() {
        return spec;
    }

    public void setSpec(CloudFunctionBundleSpec spec) {
        this.spec = spec;
    }

    @Override
    public ObjectMeta getMetadata() {
        return super.getMetadata();
    }

    @Override
    public String toString() {
        return GsonCodec.encodePretty(this);
    }
    
    
}
