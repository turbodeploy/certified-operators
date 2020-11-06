package com.vmturbo.topology.processor.conversions.typespecific;

import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

/**
 * Populate the {@link TypeSpecificInfo} unique to a VirtualVolume - i.e. {@link VirtualVolumeInfo}
 **/
public class VirtualVolumeInfoMapper extends TypeSpecificInfoMapper {

    @Override
    public TypeSpecificInfo mapEntityDtoToTypeSpecificInfo(
            @Nonnull final EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        if (!sdkEntity.hasVirtualVolumeData()) {
            return TypeSpecificInfo.getDefaultInstance();
        }
        final VirtualVolumeData vvData = sdkEntity.getVirtualVolumeData();
        VirtualVolumeInfo.Builder vvInfo = VirtualVolumeInfo.newBuilder();
        final TypeSpecificInfo.Builder retBuilder = TypeSpecificInfo.newBuilder();
        if (vvData.hasSnapshotId()) {
            vvInfo.setSnapshotId(vvData.getSnapshotId());
        }
        if (vvData.hasRedundancyType()) {
            vvInfo.setRedundancyType(vvData.getRedundancyType());
        }
        if (vvData.hasAttachmentState()) {
            vvInfo.setAttachmentState(vvData.getAttachmentState());
        }
        if (vvData.hasEncrypted()) {
            vvInfo.setEncryption(vvData.getEncrypted());
        }
        if (vvData.hasIsEphemeral()) {
            vvInfo.setIsEphemeral(vvData.getIsEphemeral());
        }
        if (vvData.hasHourlyBilledOps()) {
            vvInfo.setHourlyBilledOps(vvData.getHourlyBilledOps());
        }
        vvInfo.addAllFiles(vvData.getFileList());
        retBuilder.setVirtualVolume(vvInfo.build());
        return retBuilder.build();
    }
}
