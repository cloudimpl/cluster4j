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
package com.cloudimpl.cluster4j.app;

import com.cloudimpl.cluster4j.node.NodeConfig;
import io.scalecube.net.Address;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine;

/**
 *
 * @author nuwansa
 */
public class AppConfig implements Callable<Integer> {

    @CommandLine.Option(names = "-gp", required = false, description = "cluster gossip port")
    int gossipPort = -1;

    @CommandLine.Option(names = "-sp", required = false, description = "service port")
    int servicePort = -1;

    @CommandLine.Option(names = "-sd", required = false, description = "seeds nodes")
    List<String> seeds;
    
    List<Address> endpoints = Collections.EMPTY_LIST;

    @Override
    public Integer call() throws Exception {
        if(seeds == null)
            return 0;
        endpoints = seeds.stream().map(s -> s.split(":")).map(arr -> Address.create(arr[0], Integer.valueOf(arr[1]))).collect(Collectors.toList());
        return endpoints.size();
    }

    public int getGossipPort() {
        return gossipPort;
    }

    public Address[] getEndpoints() {
        return endpoints.toArray(Address[]::new);
    }

    public int getServicePort() {
        return servicePort;
    }

    public NodeConfig getNodeConfig()
    {
        NodeConfig.Builder builder = NodeConfig.builder();
        if(servicePort > 0)
            builder.withNodePort(servicePort);
        if(gossipPort > 0)
            builder.withGossipPort(gossipPort);
        if(endpoints.size() > 0)
            builder.withSeedNodes(getEndpoints());
        return builder.build();
    }
}
