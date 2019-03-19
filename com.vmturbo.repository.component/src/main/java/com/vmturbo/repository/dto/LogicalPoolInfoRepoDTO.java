package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.LogicalPoolInfo;

/**
 * Class that encapsulates the LogicalPool data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class LogicalPoolInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private DiskTypeInfoRepoDTO diskTypeInfoRepoDTO;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {

        if (!typeSpecificInfo.hasLogicalPool()) {
            return;
        }

        final LogicalPoolInfo lpInfo = typeSpecificInfo.getLogicalPool();
        setDiskTypeInfoRepoDTO(lpInfo.hasDiskTypeInfo()
            ? new DiskTypeInfoRepoDTO(lpInfo.getDiskTypeInfo())
            : null);

        serviceEntityRepoDTO.setLogicalPoolInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final LogicalPoolInfo.Builder lpBuilder = LogicalPoolInfo.newBuilder();
        if (getDiskTypeInfoRepoDTO() != null) {
            lpBuilder.setDiskTypeInfo(getDiskTypeInfoRepoDTO().createDiskTypeInfo());
        }
        return TypeSpecificInfo.newBuilder()
            .setLogicalPool(lpBuilder)
            .build();
    }

    public DiskTypeInfoRepoDTO getDiskTypeInfoRepoDTO() {
        return diskTypeInfoRepoDTO;
    }

    public void setDiskTypeInfoRepoDTO(final DiskTypeInfoRepoDTO diskTypeInfoRepoDTO) {
        this.diskTypeInfoRepoDTO = diskTypeInfoRepoDTO;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final LogicalPoolInfoRepoDTO that = (LogicalPoolInfoRepoDTO) o;
        return Objects.equals(diskTypeInfoRepoDTO, that.diskTypeInfoRepoDTO);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskTypeInfoRepoDTO);
    }

    @Override
    public String toString() {
        return "LogicalPoolInfoRepoDTO{" +
                "diskTypeInfoRepoDTO=" + diskTypeInfoRepoDTO +
                '}';
    }
}
