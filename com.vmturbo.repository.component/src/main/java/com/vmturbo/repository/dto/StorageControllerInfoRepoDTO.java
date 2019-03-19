package com.vmturbo.repository.dto;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageControllerInfo;

/**
 * Class that encapsulates the StorageController data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class StorageControllerInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private DiskTypeInfoRepoDTO diskTypeInfoRepoDTO;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {

        if (!typeSpecificInfo.hasStorageController()) {
            return;
        }

        final StorageControllerInfo scInfo = typeSpecificInfo.getStorageController();
        setDiskTypeInfoRepoDTO(scInfo.hasDiskTypeInfo()
            ? new DiskTypeInfoRepoDTO(scInfo.getDiskTypeInfo())
            : null);

        serviceEntityRepoDTO.setStorageControllerInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final StorageControllerInfo.Builder scBuilder = StorageControllerInfo.newBuilder();
        if (getDiskTypeInfoRepoDTO() != null) {
            scBuilder.setDiskTypeInfo(getDiskTypeInfoRepoDTO().createDiskTypeInfo());
        }
        return TypeSpecificInfo.newBuilder()
            .setStorageController(scBuilder)
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
        final StorageControllerInfoRepoDTO that = (StorageControllerInfoRepoDTO) o;
        return Objects.equals(diskTypeInfoRepoDTO, that.diskTypeInfoRepoDTO);
    }

    @Override
    public int hashCode() {
        return Objects.hash(diskTypeInfoRepoDTO);
    }

    @Override
    public String toString() {
        return "StorageControllerInfoRepoDTO{" +
                "diskTypeInfoRepoDTO=" + diskTypeInfoRepoDTO +
                '}';
    }
}
