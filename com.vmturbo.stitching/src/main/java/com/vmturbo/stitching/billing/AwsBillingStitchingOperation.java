package com.vmturbo.stitching.billing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.Builder;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A stitching operation to be used by targets discovered by AWS billing probe.
 *
 * Matching:
 * - Matches proxy billing VMs with the ones discovered by AWS probe.
 * - A match is based on the VM id
 */

public class AwsBillingStitchingOperation implements StitchingOperation<String, String> {

    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
        @Nonnull final StitchingScopeFactory stitchingScopeFactory) {
        return Optional.of(stitchingScopeFactory.probeEntityTypeScope(
            SDKProbeType.AWS.getProbeType(), EntityType.VIRTUAL_MACHINE));
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.VIRTUAL_MACHINE;
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.VIRTUAL_MACHINE);
    }

    @Override
    public Collection<String> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        return Collections.singleton(internalEntity.getEntityBuilder().getId());
    }

    @Override
    public Collection<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Collections.singleton(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> stitch(
            @Nonnull final Collection<StitchingPoint> stitchingPoints,
                @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> stitch(stitchingPoint, resultBuilder));
        return resultBuilder.build();
    }

    /**
     * Stitch internal and external entities by updating OS info and license access commodity keys.
     *
     * @param stitchingPoint    The {@link StitchingPoint} containing entities that should be stitched.
     * @param resultBuilder A {@link TopologicalChangelog} that describes the result of stitching
     */
    private void stitch(@Nonnull final StitchingPoint stitchingPoint,
                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The VM discovered by the billing probe
        StitchingEntity billingEntity = stitchingPoint.getInternalEntity();

        // The VM discovered by the main AWS probe
        Collection<? extends StitchingEntity> externalMatches = stitchingPoint.getExternalMatches();
        if (externalMatches.size() > 1) {
            logger.warn("Found more than 1 external match for the VM {}, proceeding with the " +
                "first match", billingEntity.getDisplayName());
        }
        StitchingEntity awsEntity = externalMatches.iterator().next();

        final String osName = billingEntity.getEntityBuilder().getVirtualMachineData().getGuestName();
        resultBuilder.queueUpdateEntityAlone(awsEntity, entityToUpdate -> {
            final Builder vmInfo = entityToUpdate.getEntityBuilder().getVirtualMachineDataBuilder();
            final String currentOsName = vmInfo.getGuestName();
            // the os info from the internal entity is accurate
            if (!osName.equals(currentOsName)) {
                logger.debug("Setting VM {} guest os name from {} to {}",
                    entityToUpdate.getDisplayName(), currentOsName, osName);
                vmInfo.setGuestName(osName);
                entityToUpdate.getCommodityBoughtListByProvider().values()
                    .stream()
                    .flatMap(List::stream)
                    .flatMap(e -> e.getBoughtList()
                            .stream())
                            .filter(this::isLicenseAccessCommodity)
                            .forEach(licenseCommBought -> {
                                    logger.debug("Setting License Access commodity to {} " +
                                            "on entity {}",
                                        osName, entityToUpdate.getDisplayName());
                                    licenseCommBought
                                        .setKey(osName);
                            }
                    );
            }
        });

        //Remove proxy VM
        resultBuilder.queueEntityRemoval(billingEntity);
    }

    /**
     * Check if specified commodity is of LICENSE ACCESS type.
     *
     * @param commodity Commodity yo check
     * @return  true if the specified commodity is of LICENSE ACCESS type
     */
    private boolean isLicenseAccessCommodity(final CommodityDTO.Builder commodity) {
        return CommodityType.LICENSE_ACCESS == commodity.getCommodityType();
    }
}
