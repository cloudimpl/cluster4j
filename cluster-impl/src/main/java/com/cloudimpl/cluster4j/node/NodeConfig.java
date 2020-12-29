/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.node;

import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.coreImpl.ServiceEndpointPlugin;
import io.scalecube.net.Address;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author nuwansa
 */
public class NodeConfig {

    private final List<Address> seeds;
    private final String gossipAddress;
    private final int gossipPort;
    private final int nodePort;
    private final int clientPort;
    private final List<Class<? extends ServiceEndpointPlugin>> serviceEndpoints;

    public NodeConfig(NodeConfig.Builder builder) {
        this.seeds = builder.seeds;
        this.gossipAddress = builder.gossipAddress;
        this.gossipPort = builder.gossipPort;
        this.nodePort = builder.nodePort;
        this.clientPort = builder.clientPort;
        this.serviceEndpoints = builder.serviceEndpoints;
    }

    /**
     * @return the seeds
     */
    public List<Address> getSeeds() {
        return seeds;
    }

    /**
     * @return the gossipAddress
     */
    public String getGossipAddress() {
        return gossipAddress;
    }

    /**
     * @return the gosiipPort
     */
    public int getGossipPort() {
        return gossipPort;
    }

    /**
     *
     * @return node service port
     */
    public int getNodePort() {
        return nodePort;
    }

    /**
     *
     * @return client service port
     */
    public int getClientPort() {
        return clientPort;
    }

    public List<Class<? extends ServiceEndpointPlugin>> getServiceEndpoints() {
        return serviceEndpoints;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private List<Address> seeds = new LinkedList<>();
        private String gossipAddress = CloudUtil.getHostIpAddr();
        private int gossipPort = 12000;
        private int nodePort = 10000;
        private int clientPort = 11000;
        private List<Class<? extends ServiceEndpointPlugin>> serviceEndpoints = new LinkedList<>();

        public Builder withSeedNodes(Address... address) {
            seeds.addAll(Arrays.asList(address));
            return this;
        }

        public Builder withGossipAddress(String gossipAddress) {
            this.gossipAddress = gossipAddress;
            return this;
        }

        public Builder withGossipPort(int gossipPort) {
            this.gossipPort = gossipPort;
            return this;
        }

        public Builder withNodePort(int nodePort) {
            this.nodePort = nodePort;
            return this;
        }

        public Builder withClientPort(int clientPort) {
            this.clientPort = clientPort;
            return this;
        }

        public Builder withServiceEndpoints(List<Class<? extends ServiceEndpointPlugin>> list) {
            this.serviceEndpoints = list;
            return this;
        }

        public NodeConfig build() {
            return new NodeConfig(this);
        }
    }

}
