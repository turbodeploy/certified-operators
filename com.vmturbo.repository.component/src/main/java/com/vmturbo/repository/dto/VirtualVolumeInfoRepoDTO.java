package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Class that encapsulates the virtual volume data from TopologyEntityDTO.TypeSpecificInfo
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualVolumeInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private Float storageAccessCapacity;

    private Float storageAmountCapacity;

    private Integer redundancyType;

    private List<VirtualVolumeFileRepoDTO> files;

    @Override
    public void fillFromTypeSpecificInfo(@Nonnull final TypeSpecificInfo typeSpecificInfo,
                                         @Nonnull final ServiceEntityRepoDTO serviceEntityRepoDTO) {
        if (!typeSpecificInfo.hasVirtualVolume()) {
            return;
        }
        VirtualVolumeInfo virtualVolumeInfo = typeSpecificInfo.getVirtualVolume();
        if (virtualVolumeInfo.hasStorageAccessCapacity()) {
            setStorageAccessCapacity(virtualVolumeInfo.getStorageAccessCapacity());
        }
        if (virtualVolumeInfo.hasStorageAmountCapacity()) {
            setStorageAmountCapacity(virtualVolumeInfo.getStorageAmountCapacity());
        }
        if (virtualVolumeInfo.hasRedundancyType()) {
            setRedundancyType(virtualVolumeInfo.getRedundancyType().getNumber());
        }
        setFiles(virtualVolumeInfo.getFilesList());
        serviceEntityRepoDTO.setVirtualVolumeInfoRepoDTO(this);
    }

    public @Nonnull TypeSpecificInfo createTypeSpecificInfo() {
        final VirtualVolumeInfo.Builder virtualVolumeInfoBuilder = VirtualVolumeInfo.newBuilder();
        if (getStorageAccessCapacity() != null) {
            virtualVolumeInfoBuilder.setStorageAccessCapacity(getStorageAccessCapacity());
        }
        if (getStorageAmountCapacity() != null) {
            virtualVolumeInfoBuilder.setStorageAmountCapacity(getStorageAmountCapacity());
        }
        if (getRedundancyType() != null) {
            virtualVolumeInfoBuilder.setRedundancyType(RedundancyType.forNumber(getRedundancyType()));
        }
        if (getFiles() != null) {
            virtualVolumeInfoBuilder.addAllFiles(files.stream()
                .map(VirtualVolumeFileRepoDTO::toFileDescriptor)
                .collect(Collectors.toList()));
        }
        return TypeSpecificInfo.newBuilder()
            .setVirtualVolume(virtualVolumeInfoBuilder)
            .build();
    }

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

    public List<VirtualVolumeFileRepoDTO> getFiles() {
        return files;
    }

    public void setFiles(List<VirtualVolumeFileDescriptor> fileDescriptors) {
        files = Lists.newArrayList(fileDescriptors.stream()
            .map(filedDescriptor ->
                VirtualVolumeFileRepoDTO.createFromFileDescriptor(filedDescriptor))
            .collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).omitNullValues()
            .add("storageAccessCapacity", storageAccessCapacity)
            .add("storageAmountCapacity", storageAmountCapacity)
            .add("redundancyType", redundancyType)
            .add("files", files)
            .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VirtualVolumeInfoRepoDTO that = (VirtualVolumeInfoRepoDTO) o;

        return (Objects.equals(storageAccessCapacity, that.storageAccessCapacity) &&
            Objects.equals(storageAmountCapacity, that.storageAmountCapacity) &&
            Objects.equals(redundancyType, that.redundancyType) &&
            Objects.equals(files, that.files));
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageAccessCapacity, storageAmountCapacity, redundancyType, files);
    }
}
