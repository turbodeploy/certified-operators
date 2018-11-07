package com.vmturbo.repository.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;

/**
 * Class that encapsulates the virtual volume data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualVolumeInfoRepoDTO {

    private Float storageAccessCapacity;

    private Float storageAmountCapacity;

    private Integer redundancyType;

    public Float getStorageAccessCapacity() {
        return storageAccessCapacity;
    }

    public void setStorageAccessCapacity(Float storageAccessCapacity) {
        this.storageAccessCapacity = storageAccessCapacity;
    }

    public Float getStorageAmountCapacity() {
        return storageAmountCapacity;
    }

    public void setStorageAmountCapacity(Float storageAmountCapacity) {
        this.storageAmountCapacity = storageAmountCapacity;
    }

    public Integer getRedundancyType() {
        return redundancyType;
    }

    public void setRedundancyType(Integer redundancyType) {
        this.redundancyType = redundancyType;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
                .add("storageAccessCapacity", storageAccessCapacity)
                .add("storageAmountCapacity", storageAmountCapacity)
                .add("redundancyType", redundancyType)
                .toString();
    }
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VirtualVolumeInfoRepoDTO that = (VirtualVolumeInfoRepoDTO) o;

        return (Objects.equals(storageAccessCapacity, that.storageAccessCapacity) &&
                Objects.equals(storageAmountCapacity, that.storageAmountCapacity) &&
                Objects.equals(redundancyType, that.redundancyType));
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageAccessCapacity, storageAmountCapacity, redundancyType);
    }
}
