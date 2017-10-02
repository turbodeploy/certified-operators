package com.vmturbo.repository.graph.result;

import com.google.common.base.MoreObjects;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;

import java.util.ArrayList;
import java.util.List;

public class SupplyChainInstancesType {
    private List<ServiceEntityRepoDTO> instances;
    private String type;

    public SupplyChainInstancesType() {
        this.instances = new ArrayList<>();
        this.type = "";
    }

    public SupplyChainInstancesType(final List<ServiceEntityRepoDTO> instances, final String type) {
        this.instances = instances;
        this.type = type;
    }

    public List<ServiceEntityRepoDTO> getInstances() {
        return instances;
    }

    public void setInstances(List<ServiceEntityRepoDTO> instances) {
        this.instances = instances;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", getType())
                .add("instances", getInstances())
                .toString();
    }
}
