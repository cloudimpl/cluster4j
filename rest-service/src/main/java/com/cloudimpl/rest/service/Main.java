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
package com.cloudimpl.rest.service;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.jboss.resteasy.plugins.server.sun.http.HttpContextBuilder;

/**
 *
 * @author nuwansa
 */
public class Main {
    public static final class Node
    {
        
    }
    public static Node node = new Node();
    public static void main(String[] args) throws IOException, InterruptedException {
       HttpServer httpServer = HttpServer.create(new InetSocketAddress(12345), 10);
       HttpContextBuilder contextBuilder = new HttpContextBuilder();
      contextBuilder.getDeployment().getActualResourceClasses().add(Resource.class);
      contextBuilder.getDeployment().getActualResourceClasses().add(FruitResource.class);
      HttpContext context = contextBuilder.bind(httpServer);
      
      context.getAttributes().put("some.config.info",node);
      httpServer.start();
        Thread.sleep(10000000);
      contextBuilder.cleanup();
      httpServer.stop(0);
    }
}
