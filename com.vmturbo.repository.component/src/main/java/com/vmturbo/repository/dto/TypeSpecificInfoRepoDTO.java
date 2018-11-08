package com.vmturbo.repository.dto;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;

/**
 *
 */
public interface TypeSpecificInfoRepoDTO {

    /**
     * Copy fields from a {@link TypeSpecificInfo} into a {@link ServiceEntityRepoDTO} for storage
     * in ArangoDB.
     *
     * @param typeSpecificInfo the {@link TypeSpecificInfo} structure of a TopologyDTO to copy from
     * @param serviceEntityRepoDTO the {@link ServiceEntityRepoDTO} to populate from the
     * {@link TypeSpecificInfo}
     */
    void fillFromTypeSpecificInfo(@Nonnull TypeSpecificInfo typeSpecificInfo,
                                  @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO);

    /**
     * Create a new instance of {@link TypeSpecificInfo} with the 'oneof' for the necessary
     * type-specific info (e.g. {@link VirtualMachineInfo}.
     *
     * @return a new {@link TypeSpecificInfo} protobuf with the 'oneof' populated for a
     * single type-specific info (e.g. {@link VirtualMachineInfo})
     */
    @Nonnull
    TypeSpecificInfo createTypeSpecificInfo();
}
