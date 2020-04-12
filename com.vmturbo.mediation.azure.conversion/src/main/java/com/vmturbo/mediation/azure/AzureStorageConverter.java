package com.vmturbo.mediation.azure;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.mediation.conversion.cloud.converter.StorageConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;

/**
 * In addition to all the StorageConverter's functionality, populate VirtualVolumeData fields using
 * certain Azure-specific EntityProperties.
 */
public class AzureStorageConverter extends StorageConverter {

    private static final Map<String, BiConsumer<VirtualVolumeData.Builder, String>>
        PROPERTIES_TO_SETTERS = ImmutableMap.of(
            AzureConstants.VOLUME_IS_ATTACHED_PROPERTY, AzureStorageConverter::setVolumeAttachmentState
        );

    /**
     * Create an Azure storage converter, specifying what type of Azure probe.
     *
     * @param probeType specific type of Azure probe
     */
    public AzureStorageConverter(@Nonnull final SDKProbeType probeType) {
        super(probeType);
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
                                                 @Nonnull final String isAttachedString) {
        final AttachmentState state = Boolean.valueOf(isAttachedString) ?
            AttachmentState.ATTACHED : AttachmentState.UNATTACHED;
        builder.setAttachmentState(state);
    }
}
