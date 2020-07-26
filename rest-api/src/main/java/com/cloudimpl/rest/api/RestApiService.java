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
package com.cloudimpl.rest.api;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "RestApiService")
public  class RestApiService implements Function<CloudMessage, Mono<Object>>{
    private final Set<Class<?>> resources = new HashSet<>();
    private final RestApiRuntime runtime;
    
    @Inject
    public RestApiService(RestApiRuntime runtime) {
        this.runtime = runtime;
        ScanResult rs = new ClassGraph().enableAnnotationInfo().enableClassInfo().scan();
        ClassInfoList list = rs.getClassesImplementing(RestApiResource.class.getName());
        list.stream().forEach(c->resources.add(c.loadClass()));
        resources.forEach(c->runtime.register(c));
    }

    @Override
    public Mono<Object> apply(CloudMessage t) {
        return Mono.empty();
    }
    
    
}
