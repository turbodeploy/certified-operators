package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DiskArrayInfo;

/**
 * Class that encapsulates the DiskArray data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class DiskArrayInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private DiskTypeInfoRepoDTO diskTypeInfoRepoDTO;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {

        if (!typeSpecificInfo.hasDiskArray()) {
            return;
        }

        final DiskArrayInfo daInfo = typeSpecificInfo.getDiskArray();
        setDiskTypeInfoRepoDTO(daInfo.hasDiskTypeInfo()
            ? new DiskTypeInfoRepoDTO(daInfo.getDiskTypeInfo())
            : null);

        serviceEntityRepoDTO.setDiskArrayInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final DiskArrayInfo.Builder daBuilder = DiskArrayInfo.newBuilder();
        if (getDiskTypeInfoRepoDTO() != null) {
            daBuilder.setDiskTypeInfo(getDiskTypeInfoRepoDTO().createDiskTypeInfo());
        }
        return TypeSpecificInfo.newBuilder()
            .setDiskArray(daBuilder)
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
        final DiskArrayInfoRepoDTO that = (DiskArrayInfoRepoDTO) o;
        return Objects.equals(diskTypeInfoRepoDTO, that.diskTypeInfoRepoDTO);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskTypeInfoRepoDTO);
    }

    @Override
    public String toString() {
        return "DiskArrayInfoRepoDTO{" +
                "diskTypeInfoRepoDTO=" + diskTypeInfoRepoDTO +
                '}';
    }
}
