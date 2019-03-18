package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.StorageType;

/**
 * Class that encapsulates the Storage data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class StorageInfoRepoDTO implements TypeSpecificInfoRepoDTO {
    // some number of different external names for this storage as known to the provider(s)
    private List<String> externalNames;

    // What basic type of storage does this represent, e.g. GENERIC_BLOCK, ISCSI, FIBER_CHANNEL, etc
    private String  storageType;

    public List<String> getExternalNames() {
        return externalNames;
    }

    private boolean local;

    public boolean getLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {

        if (!typeSpecificInfo.hasStorage()) {
            return;
        }

        final StorageInfo storageInfo = typeSpecificInfo.getStorage();
        if (storageInfo.hasStorageType()) {
            setStorageType(storageInfo.getStorageType().name());
        }
        setExternalNames(storageInfo.getExternalNameList());
        setLocal(storageInfo.getIsLocal());
        serviceEntityRepoDTO.setStorageInfoRepoDTO(this);
    }

    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {
        final StorageInfo.Builder storageInfoBuilder = StorageInfo.newBuilder();
        if (getStorageType() != null) {
            storageInfoBuilder.setStorageType(StorageType.valueOf(getStorageType()));
        }
        if (getExternalNames() != null) {
            storageInfoBuilder.addAllExternalName(getExternalNames());
        }
        return TypeSpecificInfo.newBuilder()
                .setStorage(storageInfoBuilder)
                .build();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("externalName", externalNames)
                .append("storageType", storageType)
                .append("local", local)
                .toString();
    }

    public void setExternalNames(final List<String> externalNames) {
        this.externalNames = externalNames;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(final String storageType) {
        this.storageType = storageType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof StorageInfoRepoDTO)) return false;

        final StorageInfoRepoDTO that = (StorageInfoRepoDTO) o;

        return Objects.equals(externalNames, that.externalNames) &&
                Objects.equals(storageType, that.storageType) &&
                Objects.equals(local, that.local);
    }

    @Override
    public int hashCode() {
        return Objects.hash(externalNames, storageType, local);
    }
}
