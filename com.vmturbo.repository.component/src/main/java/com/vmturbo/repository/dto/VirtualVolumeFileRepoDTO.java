package com.vmturbo.repository.dto;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineFileType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.VirtualVolumeFileDescriptor;

/**
 * Class to hold contents of file information from TopologyEntityDTO for a VirtualVolume.  Holds the
 * information that comes from VirtualVolumeFileDescriptor.
 */
@JsonInclude(Include.NON_EMPTY)
public class VirtualVolumeFileRepoDTO {
    // the path for the file
    private String path;

    // size in KB
    private long sizeKb;

    // file type.
    private int virtualMachineFileType;

    // Modifiation time in UTC milliseconds since epoch
    private long modificationTimeMs;

    @Override
    public int hashCode() {
        return Objects.hash(path, sizeKb, virtualMachineFileType, modificationTimeMs);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        VirtualVolumeFileRepoDTO that = (VirtualVolumeFileRepoDTO) obj;
        return Objects.equals(path, that.path)
            && (modificationTimeMs == that.modificationTimeMs)
            && (virtualMachineFileType == that.virtualMachineFileType)
            && (sizeKb == that.sizeKb);
    }

    @Override
    public String toString() {
        return "VirtualVolumeFileRepoDTO{" +
                "path='" + path + '\'' +
                ", sizeKb=" + sizeKb +
                ", virtualMachineFileType=" + virtualMachineFileType +
                ", modificationTimeMs=" + modificationTimeMs +
                '}';
    }

    public String getPath() {
        return path;
    }

    public void setPath(final String path) {
        this.path = path;
    }

    public long getSizeKb() {
        return sizeKb;
    }

    public void setSizeKb(final long sizeKb) {
        this.sizeKb = sizeKb;
    }

    public int getVirtualMachineFileType() {
        return virtualMachineFileType;
    }

    public void setVirtualMachineFileType(int virtualMachineFileType) {
        this.virtualMachineFileType = virtualMachineFileType;
    }

    public long getModificationTimeMs() {
        return modificationTimeMs;
    }

    public void setModificationTimeMs(final long modificationTimeMs) {
        this.modificationTimeMs = modificationTimeMs;
    }

    public static VirtualVolumeFileRepoDTO createFromFileDescriptor(VirtualVolumeFileDescriptor
                                                                    fileDescriptor) {
        VirtualVolumeFileRepoDTO retVal = new VirtualVolumeFileRepoDTO();
        retVal.setPath(fileDescriptor.getPath());
        retVal.setVirtualMachineFileType(fileDescriptor.getType().getNumber());
        retVal.setModificationTimeMs(fileDescriptor.getModificationTimeMs());
        retVal.setSizeKb(fileDescriptor.getSizeKb());
        return retVal;
    }

    /**
     * Convert a file to a {@link VirtualVolumeFileDescriptor}
     *
     * @return {@link VirtualVolumeFileDescriptor} representing this file.
     */
    public VirtualVolumeFileDescriptor toFileDescriptor() {
        VirtualVolumeFileDescriptor.Builder fileDBuilder = VirtualVolumeFileDescriptor.newBuilder();
        VirtualMachineFileType type = VirtualMachineFileType.forNumber(virtualMachineFileType);
        fileDBuilder.setModificationTimeMs(modificationTimeMs)
            .setSizeKb(sizeKb)
            .setPath(path)
            .setType(type == null ? VirtualMachineFileType.DISK : type);
        return fileDBuilder.build();
    }
}
