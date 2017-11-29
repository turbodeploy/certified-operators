package com.vmturbo.stitching.fabric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingIndex;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingResult;
import com.vmturbo.stitching.StitchingResult.Builder;
import com.vmturbo.stitching.StitchingPoint;

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
public abstract class FabricStitchingOperation implements StitchingOperation<List<String>, String> {
    protected static final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public Optional<EntityType> getExternalEntityType() {
        return Optional.of(EntityType.PHYSICAL_MACHINE);
    }

    /**
     * Return the name of the entity property of the Internal Entity Type that we are using to
     * identify the Physical Machine to stich with.
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
    private Optional<List<String>> getStringListEntityProperty(@Nonnull StitchingEntity stitchingEntity,
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
        return retVal.isEmpty() ? Optional.empty() : Optional.of(retVal);

    }

    @Override
    public Optional<List<String>> getInternalSignature(@Nonnull final StitchingEntity internalEntity) {
        return getStringListEntityProperty(internalEntity, getEntityPropertyName());
    }

    @Override
    public Optional<String> getExternalSignature(@Nonnull final StitchingEntity externalEntity) {
        return Optional.of(externalEntity.getEntityBuilder().getId());
    }

    @Nonnull
    @Override
    public StitchingResult stitch(@Nonnull final Collection<StitchingPoint> stitchingPoints,
                                           @Nonnull final Builder resultBuilder) {
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
                        @Nonnull final StitchingResult.Builder resultBuilder);

    @Nonnull
    @Override
    public StitchingIndex<List<String>, String> createIndex(final int expectedSize) {
        return new FabricStitchingOperation.FabricStitchingIndex(expectedSize);
    }

    /**
     * An index that permitting match identification for the uuid of Physical Machine.
     * The rule for identifying a fabric match by physical machine uuid is as follows:
     *
     * The internal entity to be matched specifies a list of uuids representing physical machines
     * that it matches with (the internal entity may be a chassis or another physical machine).
     * If the external physical machine's uuid appears on the list, it is considered a match.
     * Multiple external physical machine's may match.  In practice, this will often be the case
     * for chassese but will not be the case for internal physical machines.
     *
     * This index maintains a map of each uuid in the list to the entire list.
     */
    public static class FabricStitchingIndex implements StitchingIndex<List<String>, String> {

        private final Multimap<String, List<String>> index;

        public FabricStitchingIndex(final int expectedSize) {
            index = Multimaps.newListMultimap(new HashMap<>(expectedSize), ArrayList::new);
        }

        @Override
        public void add(@Nonnull List<String> internalSignature) {
            internalSignature.forEach(pmUUID -> index.put(pmUUID, internalSignature));
        }

        @Override
        public Stream<List<String>> findMatches(@Nonnull String externalSignature) {
            return index.get(externalSignature).stream();
        }
    }
}
