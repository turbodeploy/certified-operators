package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.NamespaceInfo;

/**
 * Class that encapsulates the namespace data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class NamespaceInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    /**
     * The average CPU frequency of all nodes in the cluster where the namespace runs.
     * The units on this is MHz/core.
     */
    private double averageNodeCpuFrequency = 1.0;

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
            @Nonnull ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasNamespace()) {
            return;
        }

        averageNodeCpuFrequency = typeSpecificInfo.getNamespace().getAverageNodeCpuFrequency();

        serviceEntityRepoDTO.setNamespaceInfoRepoDTO(this);
    }

    @Nonnull
    @Override
    public TypeSpecificInfo createTypeSpecificInfo() {
        final NamespaceInfo.Builder builder = NamespaceInfo.newBuilder();
        builder.setAverageNodeCpuFrequency(averageNodeCpuFrequency);

        return TypeSpecificInfo.newBuilder().setNamespace(builder).build();
    }

    /**
     * Get the averageNodeCpuFrequency.
     *
     * @return the averageNodeCpuFrequency of all nodes in the cluster where the namespace is running.
     *         Units on this value are in MHz/core.
     */
    public double getAverageNodeCpuFrequency() {
        return averageNodeCpuFrequency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NamespaceInfoRepoDTO that = (NamespaceInfoRepoDTO)o;
        return Objects.equals(averageNodeCpuFrequency, that.averageNodeCpuFrequency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(averageNodeCpuFrequency);
    }

    @Override
    public String toString() {
        return NamespaceInfoRepoDTO.class.getSimpleName()
            + '{' + "averageNodeCpuFrequency=" + averageNodeCpuFrequency + '}';
    }
}
