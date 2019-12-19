package com.vmturbo.stitching.prestitching;

import static com.vmturbo.stitching.utilities.MergeEntities.mergeEntity;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;
import com.vmturbo.stitching.utilities.EntityScopeFilters;

/**
 * Merge entities of a particular type with the same OID that are discovered by a given probe type.
 * We assume that all data in the entities is the same and pick an arbitrary one to keep.
 */
public class SharedCloudEntityPreStitchingOperation implements PreStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    // the probe type that this operation applies to
    private final String probeType;

    // the type of entity to merge
    private final EntityType entityType;

    /**
     * Create a SharedCloudEntityPreStitchingOperation that operates on the given probe type and
     * entity type.
     *
     * @param probeType String giving the name of the probe type.
     * @param entityType {@link EntityType} to merge.
     */
    public SharedCloudEntityPreStitchingOperation(@Nonnull String probeType,
                                                  @Nonnull EntityType entityType) {
        this.probeType = Objects.requireNonNull(probeType);
        this.entityType = Objects.requireNonNull(entityType);
    }

    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
        @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        // Apply this calculation to all entities of the defined type that come from the defined
        // probe type.
        return stitchingScopeFactory.probeEntityTypeScope(probeType, entityType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(@Nonnull Stream<StitchingEntity> entities,
                                                                  @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        EntityScopeFilters.sharedEntitiesByOid(entities).forEach(sharedEntities ->
            mergeSharedEntities(sharedEntities, resultBuilder));

        return resultBuilder.build();
    }

    /**
     * Merge multiple instances of the shared entity down to a single instance.
     * We retain one entity at random and merge the relationships of the other entities onto that
     * one.
     *
     * @param sharedEntityInstances The shared entities. Note that there must be at least 2.
     * @param resultBuilder The builder for the result of the stitching calculation.
     */
    private void mergeSharedEntities(@Nonnull final List<StitchingEntity> sharedEntityInstances,
                                    @Nonnull final StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        Preconditions.checkArgument(sharedEntityInstances.size() > 1, "There must be multiple instances of a " +
            "shared cloud entity.");
        logger.debug("Merging shared entities: {}", sharedEntityInstances);

        // Pick an entity to keep.
        final StitchingEntity entityToKeep = sharedEntityInstances.get(0);

        // Remove the duplicate instances of this object after merging connectedTo and connectedFrom
        // because we know the data they have is identical to that in entityToKeep.
        for (int i = 1; i < sharedEntityInstances.size(); i++) {
            resultBuilder.queueEntityMerger(mergeEntity(sharedEntityInstances.get(i))
                .onto(entityToKeep));
        }
    }
}
