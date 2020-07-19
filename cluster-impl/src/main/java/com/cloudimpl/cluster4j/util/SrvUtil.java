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
package com.cloudimpl.cluster4j.util;

import com.cloudimpl.cluster4j.app.ServiceMeta;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import java.util.Objects;

/**
 *
 * @author nuwansa
 */
public class SrvUtil {
    public static ServiceMeta serviceMeta(Class<?> funcType)
    {
        CloudFunction func = funcType.getAnnotation(CloudFunction.class);
        Objects.requireNonNull(func);
        Router router = funcType.getAnnotation(Router.class);
        Objects.requireNonNull(func);
        return new ServiceMeta(funcType,func, router);
    }
}
