package com.vmturbo.stitching.fabric;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingPoint;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * A stitching operation appropriate for use by fabric targets.
 *
 * Matching:
 * - Matches Physical Machines from hypervisors to the Chassis, IOModules and Switches in fabric.
 * - A match is determined when the appropriate entity property (PM_UUID_LIST or PM_UUID depending
 * on the internal entity type) contains the UUID of a Physical Machine from a hypervisor.
 *
 * Subclasses of this class implement the actual processing by specifying the name of the property
 * to get the list of UUIDs from, the internal EntityType to match on, and the stitch method that
 * carries out the connection of and cleanup of entities.
 *
 */
public abstract class FabricStitchingOperation implements StitchingOperation<String, String> {
    protected static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<StitchingScope<StitchingEntity>> getScope(
            @Nonnull final StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.PHYSICAL_MACHINE);
    }

    /**
     * Return the name of the entity property of the Internal Entity Type that we are using to
     * identify the Physical Machine to stitch with.
     *
     * @return String giving the name of the property, e.g. PM_UUID_LIST
     */
    protected abstract String getEntityPropertyName();

    /**
     * Parse a comma delimited list of strings from the named entity property into a proper list of
     * Strings.
     *
     * @param stitchingEntity The entity that the property belongs to.
     * @param propertyName The name of the property.
     * @return an optional list of Strings which is Optional.empty if the property doesn't exist or
     * is empty and is the list of values parsed from the property if it does exist.
     */
    private Collection<String> getStringListEntityProperty(@Nonnull StitchingEntity stitchingEntity,
                                                               @Nonnull String propertyName) {
        final List<String> retVal = Lists.newArrayList();
        Optional<EntityProperty> rawProperty = stitchingEntity.getEntityBuilder()
                .getEntityPropertiesList()
                .stream()
                .filter(entityProperty -> entityProperty.getName().equals(propertyName))
                .findFirst();
        rawProperty.ifPresent(entityProperty ->
                retVal.addAll(Arrays.asList(entityProperty.getValue().split(","))));
        logger.trace("FabricStitchingOperation: Returning string list of size {}"
                        + " for property {}", retVal.size(), propertyName);
        return retVal.isEmpty() ? Collections.emptySet() : retVal;

    }

    @Override
    public Collection<String> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        return getStringListEntityProperty(internalEntity, getEntityPropertyName());
    }

    @Override
    public Collection<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Collections.singleton(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        stitchingPoints.forEach(stitchingPoint -> stitch(stitchingPoint, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Carry out the operations needed to stitch together the graphs of the internal and
     * external entities.
     *
     * @param stitchingPoint The point at which the internal graph should be stitched with
     *                       the graphs discovered by external probes.
     * @param resultBuilder The builder of the results of the stitching operation. Changes to
     *                      relationships made by the stitching operation should be noted
     *                      in these results.
     */
    protected abstract void stitch(@Nonnull final StitchingPoint stitchingPoint,
                        @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder);
}
