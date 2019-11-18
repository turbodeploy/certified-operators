package com.vmturbo.mediation.aws;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.amazonaws.services.ec2.model.VolumeState;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.aws.util.AwsConstants;
import com.vmturbo.mediation.conversion.cloud.converter.StorageConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * In addition to all the StorageConverter's functionality, populate VirtualVolumeData fields using
 * certain AWS-specific EntityProperties.
 */
public class AwsStorageConverter extends StorageConverter {

    private static final Map<String, BiConsumer<VirtualVolumeData.Builder, String>>
        PROPERTIES_TO_SETTERS = ImmutableMap.of(
            AwsConstants.IO_THROUGHPUT_CAPACITY_PROPERTY,
                ((VirtualVolumeData.Builder builder, String capacityString) ->
                    builder.setIoThroughputCapacity(Double.valueOf(capacityString).doubleValue())),
            AwsConstants.IO_THROUGHPUT_USED_PROPERTY,
                ((VirtualVolumeData.Builder builder, String usageString) ->
                    builder.setIoThroughputUsage(Double.valueOf(usageString).doubleValue())),
            AwsConstants.STATE, AwsStorageConverter::setVolumeAttachmentState,
            AwsConstants.ENCRYPTED,
            ((VirtualVolumeData.Builder builder, String isEncrypted) ->
                    builder.setEncrypted(Boolean.valueOf(isEncrypted)))
        );

    private static final Map<String, AttachmentState> STRING_TO_ATTACHMENT_STATE = ImmutableMap.of(
        VolumeState.InUse.toString(), AttachmentState.ATTACHED,
        VolumeState.Available.toString(), AttachmentState.UNATTACHED
    );

    AwsStorageConverter() {
        super(SDKProbeType.AWS);
    }

    @Override
    protected VirtualVolumeData updateVirtualVolumeData(@Nonnull final VirtualVolumeData preexistingVolumeData,
                                                        @Nonnull final List<EntityProperty> volumeFieldUpdateProperties) {
        final VirtualVolumeData.Builder updatedVolumeDataBuilder = preexistingVolumeData.toBuilder();
        for (final EntityProperty propertyContainingUpdate : volumeFieldUpdateProperties) {
            if (!propertyContainingUpdate.getNamespace().equals(SDKUtil.VC_TAGS_NAMESPACE) &&
                    PROPERTIES_TO_SETTERS.containsKey(propertyContainingUpdate.getName())) {
                PROPERTIES_TO_SETTERS.get(propertyContainingUpdate.getName())
                    .accept(updatedVolumeDataBuilder, propertyContainingUpdate.getValue());
            }
        }
        return updatedVolumeDataBuilder.build();
    }

    private static void setVolumeAttachmentState(@Nonnull final VirtualVolumeData.Builder builder,
                                                 @Nonnull final String stateString) {
        final AttachmentState attachmentState =
            STRING_TO_ATTACHMENT_STATE.getOrDefault(stateString, AttachmentState.ATTACHED);
        builder.setAttachmentState(attachmentState);
    }
}
