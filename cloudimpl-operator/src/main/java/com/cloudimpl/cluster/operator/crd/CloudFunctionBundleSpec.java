package com.cloudimpl.cluster.operator.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import java.util.List;

/*
 */
@JsonDeserialize(using = JsonDeserializer.None.class)
@JsonInclude(value = com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(value = {"loadBalancer", "minReplicas", "maxReplicas", "containers"})
public class CloudFunctionBundleSpec implements KubernetesResource {

    @JsonProperty(value = "loadBalancer")
    private String loadBalancer;
    @JsonProperty(value = "minReplicas")
    private int minReplicas = 0;
    @JsonProperty(value = "maxReplicas")
    private int maxReplicas = 0;
    @JsonProperty(value = "containers")
    private List<Container> containers;
    
    public String getLoadBalancer() {
        return loadBalancer;
    }
  
    public void setLoadBalancer(String loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    public void setMinReplicas(int minReplicas) {
        this.minReplicas = minReplicas;
    }

    public void setMaxReplicas(int maxReplicas) {
        this.maxReplicas = maxReplicas;
    }
    
    public void setContainers(List<Container> containers)
    {
       this.containers = containers;
    }

    public int getMaxReplicas() {
        return maxReplicas;
    }

    public int getMinReplicas() {
        return minReplicas;
    }
    
    @JsonProperty(value = "containers")
    public List<Container> getContainers()
    {
        return containers;
    }
    
    @Override
    public String toString() {
        return "CloudFunctionBundleSpec{" + "loadBalancer=" + loadBalancer + ", containers=" + containers + '}';
    }
    
}
