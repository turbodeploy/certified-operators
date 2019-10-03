package com.vmturbo.mediation.aws;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.amazonaws.services.ec2.model.VolumeType;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.mediation.aws.control.VirtualDiskMoveActionExecutor;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Execute Move Volume actions the same way as parent class, but with different suppliers of
 * the necessary properties.
 */
public class MoveVolumeExecutor extends VirtualDiskMoveActionExecutor {

    private static final int INDEX_OF_VOLUME_ID = 3;
    private static final int INDEX_OF_REGION_NAME = 1;

    @VisibleForTesting
    static final String ASSOCIATED_VIRTUAL_MACHINE_PROPERTY = "AssociatedVirtualMachine";
    static final String VM_UNAVAILABLE_MESSAGE = "[Associated VM ID unavailable]";
    static final String PROBE_ID_SEPARATOR = "::";
    static final double MEBIBYTES_PER_GIBIBYTE = 1024;

    MoveVolumeExecutor() {
        super(EntityType.VIRTUAL_VOLUME, ActionType.MOVE);
    }

    /**
     * Placeholder for property not yet implemented (only used for logging purposes).
     * TODO: if volume has an associated VM, store the VM ID as EntityProperty of volume
     */
    @Override
    protected String getVirtualMachineInstanceId(@Nonnull final ActionItemDTO actionDTO) {
        return actionDTO.getTargetSE().getEntityPropertiesList().stream()
            .filter(property -> property.getName().equals(ASSOCIATED_VIRTUAL_MACHINE_PROPERTY))
            .map(EntityProperty::getValue)
            .findAny().orElse(VM_UNAVAILABLE_MESSAGE);
    }

    @Override
    protected String getRegionName(@Nonnull final ActionItemDTO actionDTO) {
        // Retrieve a volume's region name from its original probe ID
        // (e.g. "aws::us-east-1::VL::vol-01d5be57079034488" -> "us-east-1")
        return extractInfoFromOriginalProbeId(actionDTO.getTargetSE().getId(), INDEX_OF_REGION_NAME);
    }

    @Override
    protected Optional<String> getVolumeId(@Nonnull ActionItemDTO action) {
        // Retrieve a volume's true ID from its original probe ID
        // (e.g. "aws::us-east-1::VL::vol-01d5be57079034488" -> "vol-01d5be57079034488")
        return Optional.of(
            extractInfoFromOriginalProbeId(action.getTargetSE().getId(), INDEX_OF_VOLUME_ID));
    }

    @Override
    protected Optional<VolumeType> getVolumeType(EntityDTO tier) {
        final String tierName = tier.getDisplayName().toLowerCase();
        return Optional.of(VolumeType.fromValue(tierName));
    }

    @Override
    protected Optional<Integer> getVolumeSize(@Nonnull ActionItemDTO action) {
        final float storageAmountMb =
            action.getTargetSE().getVirtualVolumeData().getStorageAmountCapacity();
        final int storageAmountGib = (int)(storageAmountMb / MEBIBYTES_PER_GIBIBYTE);
        return Optional.of(storageAmountGib);
    }

    /**
     * Extract relevant information from a volume's original probe-assigned ID.
     *
     * @param originalProbeId original probe-assigned ID
     * @param indexOfDesiredPortion section of ID to extract
     * @return relevant section of ID
     */
    private static String extractInfoFromOriginalProbeId(@Nonnull final String originalProbeId,
            final int indexOfDesiredPortion) {
        return originalProbeId.split(PROBE_ID_SEPARATOR)[indexOfDesiredPortion];
    }


}
