package com.vmturbo.stitching.compute;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.CopyCommodities;
import com.vmturbo.stitching.utilities.MergeEntities;

/**
 * A stitching operation appropriate for use by Container targets.
 *
 * Matching:
 * - Matches proxy VMs with discovered public or private cloud IaaS VMs.
 * - A match is determined when the ID or the IP address for an proxy VM
 *   matches the ID or the IP address of the discovered VM
 */

public class IaasVMStitchingOperation implements StitchingOperation<String, String> {
    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory,
            long targetId) {
        return Optional.empty();
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

    /**
     * Parse a comma delimited list of strings from the named entity property into a proper list of
     * Strings.
     *
     * @param stitchingEntity The entity that the property belongs to.
     * @param propertyName The name of the property.
     * @return an optional list of Strings which is Optional.empty if the property doesn't exist or
     * is empty and is the list of values parsed from the property if it does exist.
     */
    private Collection<String> getStringEntityProperty(@Nonnull StitchingEntity stitchingEntity,
                                                               @Nonnull String propertyName) {
        Optional<CommonDTO.EntityDTO.EntityProperty> rawProperty = stitchingEntity.getEntityBuilder()
                .getEntityPropertiesList()
                .stream()
                .filter(entityProperty -> entityProperty.getName().equals(propertyName))
                .findFirst();
        return rawProperty.map(entityProperty -> Collections.singleton(entityProperty.getValue()))
                        .orElse(Collections.emptySet());
    }

    @Override
    public Collection<String> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        return getStringEntityProperty(internalEntity, "Proxy_VM_UUID");
    }

    @Override
    public Collection<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Collections.singleton(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                     @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> stitch(stitchingPoint, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Merge the Container probe VM onto the IaaS VM.
     *
     * @param stitchingPoint The point at which the storage graph should be stitched with
     *                       the graphs discovered by external probes.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                      relationships made by the stitching operation should be noted
     *                      in these results.
     */
    private static void stitch(@Nonnull final StitchingPoint stitchingPoint,
                    @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        // The VM discovered by the container probe
        final StitchingEntity containerVM = stitchingPoint.getInternalEntity();

        // The VM discovered by the hypervisor probe
        final StitchingEntity hypervisorVM = stitchingPoint.getExternalMatches().iterator().next();

        logger.debug("Stitching {} with {}",
                hypervisorVM.getDisplayName(), containerVM.getDisplayName());

        // All the commodities sold by the container VM should now be sold by the hypervisor vm.
        resultBuilder
            .queueChangeRelationships(hypervisorVM, toUpdate -> {
                CopyCommodities.copyCommodities().from(containerVM).to(toUpdate);
            })
            .queueEntityMerger(MergeEntities.mergeEntity(containerVM).onto(hypervisorVM));
    }
}
