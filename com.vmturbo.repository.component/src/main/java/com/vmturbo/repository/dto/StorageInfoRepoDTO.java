package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

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

    private Boolean local;

    // Do not generate delete wasted files actions for this Storage
    // Deprecated since this flag is no longer used in TopologyEntityDTO.
    @Deprecated
    private boolean ignoreWastedFiles;

    public List<String> getExternalNames() {
        return externalNames;
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

    public Boolean getLocal() {
        return local;
    }

    public void setLocal(Boolean local) {
        this.local = local;
    }

    /**
     * Getter for ignoreWastedFiles.
     *
     * @return True if wasted files actions should be suppressed for this storage.
     */
    @Deprecated
    public boolean getIgnoreWastedFiles() {
        return ignoreWastedFiles;
    }

    /**
     * Setter for ignoreWastedFiles.  Set to true for storages that we don't want to generate
     * wasted files actions for.
     *
     * @param ignoreWastedFiles flag indicating that we shouldn't generate wasted files actions.
     */
    @Deprecated
    public void setIgnoreWastedFiles(final boolean ignoreWastedFiles) {
        this.ignoreWastedFiles = ignoreWastedFiles;
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
        if (getLocal() != null) {
            storageInfoBuilder.setIsLocal(getLocal());
        }
        return TypeSpecificInfo.newBuilder()
                .setStorage(storageInfoBuilder)
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StorageInfoRepoDTO that = (StorageInfoRepoDTO) o;
        return Objects.equals(externalNames, that.externalNames) &&
                Objects.equals(storageType, that.storageType) &&
                Objects.equals(local, that.local) &&
                Objects.equals(ignoreWastedFiles, that.ignoreWastedFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(externalNames, storageType, local, ignoreWastedFiles);
    }

    @Override
    public String toString() {
        return "StorageInfoRepoDTO{" +
                "externalNames=" + externalNames +
                ", storageType='" + storageType + '\'' +
                ", local=" + local +
                ", ignoreWastedFiles=" + ignoreWastedFiles +
                '}';
    }
}
