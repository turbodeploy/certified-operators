package com.vmturbo.stitching.fabric;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * Stitch a physical machine discovered by a fabric probe with the corresponding physical machine
 * from a hypervisor probe.  Match is made using the PM_UUID property of the physical machine
 * discovered by the fabric probe.  This is a comma separated list of UUIDs that is parsed and
 * matched against UUIDs from physical machines discovered by hypervisor probes.
 * Once matched, we check if the fabric physical machine has a datacenter provider.  If so, we
 * remove it and its commodities as this is a replaceable datacenter and we will use the datacenter
 * from the hypervisor probe as the provider.  We then copy over all other commodities from the
 * fabric physical machine to the hypervisor physical machine.  Finally, we delete the fabric
 * physical machine as it is a proxy.
 */
public class FabricPMStitchingOperation extends FabricStitchingOperation {
    @Override
    protected String getEntityPropertyName() {
        return "PM_UUID";
    }

    @Override
    protected void stitch(@Nonnull final StitchingPoint stitchingPoint,
                          @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The PM discovered by the fabric probe
        final StitchingEntity fabricPM = stitchingPoint.getInternalEntity();

        // The PM discovered by the hypervisor probe
        final StitchingEntity hypervisorPM = stitchingPoint.getExternalMatches().iterator().next();

        // We need to handle both UCS-B and UCS-C cases.  In UCS-B case the fabric PM will be
        // hosted by a Chassis, but in UCS-C it will be hosted by a fake Datacenter.  In that case
        // we need to remove the fake Datacenter as a provider and use the hypervisor Datacenter
        // instead.
        // TODO we need to figure out if this is a more general use case or only UCS specific.  If
        // it is the latter we should abstract this logic out to a UCS specific class.
        final Optional<StitchingEntity> fabricDatacenter = fabricPM.getProviders().stream()
                .filter(entity -> entity.getEntityType() == EntityType.DATACENTER)
                .findAny();

        fabricDatacenter.ifPresent(fabricDC ->
            resultBuilder.queueChangeRelationships(fabricPM,
                    toUpdate -> toUpdate.removeProvider(fabricDC)));

        logger.debug("Stitching UCS PM {} with hypervisor PM {}",
                fabricPM.getDisplayName(), hypervisorPM.getDisplayName());

        resultBuilder
                // Update the commodities bought on the hypervisor PM to buy from the
                // fabric-probe discovered providers
                .queueChangeRelationships(hypervisorPM,
                        toUpdate -> CopyCommodities.copyCommodities().from(fabricPM).to(toUpdate))
                // Merge the fabric-probe discovered PM (required for USC-D action execution)
                // ignore the sold commodities from fabricPM, and only keep those from hypervisorPM
                .queueEntityMerger(MergeEntities.mergeEntity(fabricPM).onto(hypervisorPM,
                    MergeEntities.DROP_ALL_FROM_COMMODITIES_STRATEGY));
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.PHYSICAL_MACHINE;
    }
}
