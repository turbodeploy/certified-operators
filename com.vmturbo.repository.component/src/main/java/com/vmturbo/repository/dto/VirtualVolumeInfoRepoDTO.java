package com.vmturbo.repository.dto;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Class that encapsulates the virtual volume data from TopologyEntityDTO.TypeSpecificInfo.
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualVolumeInfoRepoDTO implements TypeSpecificInfoRepoDTO {

    private Float storageAccessCapacity;

    private Float storageAmountCapacity;

    private Float ioThroughputCapacity;

    private Integer redundancyType;

    private String snapshotId;

    private List<VirtualVolumeFileRepoDTO> files;

    private Boolean encryption;

    private Integer attachmentState;

    private Boolean ephemeral;

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
        if (virtualVolumeInfo.hasIoThroughputCapacity()) {
            setIoThroughputCapacity(virtualVolumeInfo.getIoThroughputCapacity());
        }
        if (virtualVolumeInfo.hasRedundancyType()) {
            setRedundancyType(virtualVolumeInfo.getRedundancyType().getNumber());
        }
        if (virtualVolumeInfo.hasSnapshotId()) {
            setSnapshotId(virtualVolumeInfo.getSnapshotId());
        }
        if (virtualVolumeInfo.hasEncryption()) {
            setEncryption(virtualVolumeInfo.getEncryption());
        }
        if (virtualVolumeInfo.hasAttachmentState()) {
            setAttachmentState(virtualVolumeInfo.getAttachmentState().getNumber());
        }
        if (virtualVolumeInfo.hasIsEphemeral()) {
            setEphemeral(virtualVolumeInfo.getIsEphemeral());
        }
        setFiles(virtualVolumeInfo.getFilesList());
        serviceEntityRepoDTO.setVirtualVolumeInfoRepoDTO(this);
    }

    @Override
    @Nonnull
    public TypeSpecificInfo createTypeSpecificInfo() {
        final VirtualVolumeInfo.Builder virtualVolumeInfoBuilder = VirtualVolumeInfo.newBuilder();
        if (getStorageAccessCapacity() != null) {
            virtualVolumeInfoBuilder.setStorageAccessCapacity(getStorageAccessCapacity());
        }
        if (getStorageAmountCapacity() != null) {
            virtualVolumeInfoBuilder.setStorageAmountCapacity(getStorageAmountCapacity());
        }
        if (getIoThroughputCapacity() != null) {
            virtualVolumeInfoBuilder.setIoThroughputCapacity(getIoThroughputCapacity());
        }
        if (getRedundancyType() != null) {
            virtualVolumeInfoBuilder.setRedundancyType(RedundancyType.forNumber(getRedundancyType()));
        }
        if (getFiles() != null) {
            virtualVolumeInfoBuilder.addAllFiles(files.stream()
                .map(VirtualVolumeFileRepoDTO::toFileDescriptor)
                .collect(Collectors.toList()));
        }
        if (getSnapshotId() != null) {
            virtualVolumeInfoBuilder.setSnapshotId(getSnapshotId());
        }
        if (getAttachmentState() != null) {
            virtualVolumeInfoBuilder.setAttachmentState(AttachmentState.forNumber(getAttachmentState()));
        }
        if (getEncryption() != null) {
            virtualVolumeInfoBuilder.setEncryption(getEncryption());
        }
        if (getEphemeral() != null) {
            virtualVolumeInfoBuilder.setIsEphemeral(ephemeral);
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

    public Float getIoThroughputCapacity() {
        return ioThroughputCapacity;
    }

    public void setIoThroughputCapacity(Float ioThroughputCapacity) {
        this.ioThroughputCapacity = ioThroughputCapacity;
    }

    public Integer getRedundancyType() {
        return redundancyType;
    }

    public void setRedundancyType(Integer redundancyType) {
        this.redundancyType = redundancyType;
    }

    public String getSnapshotId() { return snapshotId; }

    public void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
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

    public Boolean getEncryption() {
        return encryption;
    }

    public void setEncryption(final Boolean encryption) {
        this.encryption = encryption;
    }

    /**
     * Returns whether volume is ephemeral or not.
     *
     * @return whether volume is ephemeral or not.
     */
    @Nullable
    public Boolean getEphemeral() {
        return ephemeral;
    }

    /**
     * Sets ephemeral flag for the volume.
     *
     * @param ephemeral value to set.
     */
    public void setEphemeral(@Nullable Boolean ephemeral) {
        this.ephemeral = ephemeral;
    }

    public Integer getAttachmentState() {
        return attachmentState;
    }

    public void setAttachmentState(final Integer attachmentState) {
        this.attachmentState = attachmentState;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VirtualVolumeInfoRepoDTO that = (VirtualVolumeInfoRepoDTO) o;

        return (Objects.equals(storageAccessCapacity, that.storageAccessCapacity)
                && Objects.equals(storageAmountCapacity, that.storageAmountCapacity)
                && Objects.equals(ioThroughputCapacity, that.ioThroughputCapacity)
                && Objects.equals(redundancyType, that.redundancyType)
                && Objects.equals(files, that.files)
                && Objects.equals(snapshotId, that.snapshotId)
                && Objects.equals(attachmentState, that.attachmentState)
                && Objects.equals(encryption, that.encryption)
                && Objects.equals(ephemeral, that.ephemeral));
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageAccessCapacity, storageAmountCapacity, ioThroughputCapacity,
            redundancyType, files, snapshotId, attachmentState, encryption, ephemeral);
    }

    @Override
    public String toString() {
        return String.format(
                        "%s [storageAccessCapacity=%s, storageAmountCapacity=%s, ioThroughputCapacity=%s, redundancyType=%s, snapshotId=%s, files=%s, encryption=%s, attachmentState=%s, ephemeral=%s]",
                        getClass().getSimpleName(), this.storageAccessCapacity,
                        this.storageAmountCapacity, ioThroughputCapacity, this.redundancyType,
                        this.snapshotId, this.files, this.encryption, this.attachmentState,
                        this.ephemeral);
    }
}
