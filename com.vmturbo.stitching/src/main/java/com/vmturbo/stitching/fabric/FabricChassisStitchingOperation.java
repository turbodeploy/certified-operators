package com.vmturbo.stitching.fabric;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * Class for stitching a chassis from a fabric probe with the physical machines from the hypervisor
 * probe.  Physical machine UUIDs that match the chassis are specified in the PM_UUID_LIST entity
 * property of the chassis.  For each PM that matches the chassis, we remove the datacenter as a
 * provider for that PM.  Separately, PM->PM stitching will add the chassis, iomodules, and switches
 * as providers to this physical machine by copying over the commodities bought.
 */
public class FabricChassisStitchingOperation extends FabricStitchingOperation{
    @Override
    protected String getEntityPropertyName() {
        return "PM_UUID_LIST";
    }

    @Nonnull
    @Override
    public EntityType getInternalEntityType() {
        return EntityType.CHASSIS;
    }

    /**
     * Remove the relationship between the physical machines matched to this chassis and
     * their datacenters.  Physical machine stitching will separately connect the physical
     * machines to the chassis.
     *
     * @param stitchingPoint The point at which the chassis graph should be stitched with
     *                       the graphs discovered by external probes.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                      relationships made by the stitching operation should be noted
     *                      in these results.
     */
    @Override
    protected void stitch(@Nonnull final StitchingPoint stitchingPoint,
                          @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The chassis discovered by the fabric probe
        final StitchingEntity fabricChassis = stitchingPoint.getInternalEntity();

        // Iterate through physical machine discovered by the hypervisor probe that match this
        // Chassis.  Find the corresponding Fabric PMs.  Then do the stitching.
        for (StitchingEntity hypervisorPM : stitchingPoint.getExternalMatches()) {
            // Find the datacenter discovered by the hypervisor probe by finding the provider
            // of the hypervisor datacenter
            final Optional<StitchingEntity> hypervisorDatacenter = hypervisorPM.getProviders().stream()
                    .filter(entity -> entity.getEntityType() == EntityType.DATACENTER)
                    .findFirst();
            if (!hypervisorDatacenter.isPresent())    {
                logger.error("No datacenter provider present for host '{}'.", hypervisorPM.getDisplayName());
                continue;
            }

            logger.debug("Stitching Chassis {} with Physical Machine {}",
                    fabricChassis.getDisplayName(), hypervisorPM.getDisplayName());
            logger.debug("Replacing Datacenter {} with Chassis {}",
                    hypervisorDatacenter.get().getDisplayName(), fabricChassis.getDisplayName());

            resultBuilder
                    // Update the commodities bought on the hypervisorPM to buy from the
                    // fabric-probe discovered chassis
                    .queueChangeRelationships(hypervisorPM,
                            toUpdate -> toUpdate.removeProvider(hypervisorDatacenter.get()));
        }
    }

}
