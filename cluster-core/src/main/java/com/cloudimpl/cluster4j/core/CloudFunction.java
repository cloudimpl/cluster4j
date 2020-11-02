/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import java.util.function.Function;
import org.reactivestreams.Publisher;

/**
 *
 * @author nuwansa
 */
public class CloudFunction {

    private final String functionType;
    private final String inputType;
    private final String id;
    private final CloudRouterDescriptor routerDesc;

    public CloudFunction(String id, String functionType, String inputType, CloudRouterDescriptor routerDesc) {
        this.id = id;
        this.functionType = functionType;
        this.inputType = inputType;
        this.routerDesc = routerDesc;
    }

    public String getFunctionType() {
        return functionType;
    }

    public String getInputType() {
        return inputType;
    }

    public String getId() {
        return id;
    }

    public CloudRouterDescriptor getRouterDesc() {
        return routerDesc;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Class<? extends Function> functionType;
        private String inputType;
        private CloudRouterDescriptor routerDesc;
        private String id;
        public Builder withFunction(Class<? extends Function<?, ? extends Publisher>> functionType) {
            this.functionType = functionType;
            this.inputType = CloudUtil.extractGenericParameter(functionType, Function.class, 0).getName();
            return this;
        }

        public Builder withRouter(CloudRouterDescriptor routerDesc) {
            this.routerDesc = routerDesc;
            return this;
        }

        public Builder withId(String id){
            this.id = id;
            return this;
        }
        
        public CloudFunction build() {
            return new CloudFunction(this.id,functionType.getName(), inputType, routerDesc);
        }
    }
}
