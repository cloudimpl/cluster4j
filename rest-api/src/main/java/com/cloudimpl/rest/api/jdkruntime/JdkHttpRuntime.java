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
package com.cloudimpl.rest.api.jdkruntime;

import com.cloudimpl.cluster4j.core.CloudException;
import com.cloudimpl.rest.api.RestApiRuntime;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.jboss.resteasy.plugins.server.sun.http.HttpContextBuilder;

/**
 *
 * @author nuwansa
 */
public class JdkHttpRuntime implements RestApiRuntime {

    private final int port;
    private final int backLog;
    private final HttpServer httpServer;
    private final HttpContextBuilder contextBuilder;
    private HttpContext context;
    private JdkHttpRuntime(Builder builder) {
        this.port = builder.port;
        this.backLog = builder.backLog;
        this.httpServer = createServer(port, backLog);
        this.contextBuilder = new HttpContextBuilder();
    }

    private HttpServer createServer(int port, int backlog) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), backlog);
            return server;
        } catch (IOException ex) {
            throw new CloudException(ex);
        }
    }

    @Override
    public void register(Class<?> resource) {
        this.contextBuilder.getDeployment().getActualResourceClasses().add(resource);
        
    }

    @Override
    public void start() {
       this.context = this.contextBuilder.bind(httpServer);
    }

    public static final class Builder {

        private int port;
        private int backLog;

        public Builder() {
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withBacklog(int backlog) {
            this.backLog = backlog;
            return this;
        }

        public JdkHttpRuntime build() {
            return new JdkHttpRuntime(this);
        }
    }
}
