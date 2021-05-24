package com.vmturbo.stitching.prestitching;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity;
import com.vmturbo.platform.common.dto.CommonDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.PreStitchingOperation;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.StitchingChangesBuilder;

/**
 * This pre-stitching operation removes Cloud Commitments if they belong to regions that haven't
 * been discovered as real entities. E.g. it removes AWS Cloud Commitments belonging to GovCloud
 * regions when no AWS GovCloud targets have been added.
 */
public class CloudCommitmentPreStitchingOperation implements PreStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The scope contains Cloud Commitments and entities that Cloud Commitments can be aggregated by.
     */
    private static final List<EntityType> SCOPE = ImmutableList.of(
            EntityType.CLOUD_COMMITMENT,
            EntityType.REGION,
            EntityType.SERVICE_PROVIDER);

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public StitchingScope<StitchingEntity> getScope(
            @Nonnull StitchingScopeFactory<StitchingEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.multiEntityTypesScope(SCOPE);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public TopologicalChangelog<StitchingEntity> performOperation(
            @Nonnull Stream<StitchingEntity> entities,
            @Nonnull StitchingChangesBuilder<StitchingEntity> resultBuilder) {
        final List<StitchingEntity> cloudCommitments = new ArrayList<>();
        final Set<String> aggregatedByIds = new HashSet<>();
        entities.forEach(entity -> {
            if (entity.getEntityType() == EntityType.CLOUD_COMMITMENT) {
                cloudCommitments.add(entity);
            } else if (!isProxyRegion(entity)) {
                aggregatedByIds.add(entity.getLocalId());
            }
        });
        cloudCommitments.stream()
                .filter(cloudCommitment -> hasMissingAggregatedBy(cloudCommitment, aggregatedByIds))
                .forEach(resultBuilder::queueEntityRemoval);
        return resultBuilder.build();
    }

    private static boolean isProxyRegion(@Nonnull final StitchingEntity entity) {
        return entity.getEntityType() == EntityType.REGION
                && entity.getEntityBuilder().getOrigin() == EntityOrigin.PROXY;
    }

    private static boolean hasMissingAggregatedBy(
            @Nonnull final StitchingEntity cloudCommitment,
            @Nonnull final Set<String> aggregatedByIds) {
        final Set<String> aggregatedBySet = cloudCommitment.getEntityBuilder()
                .getConnectedEntitiesList().stream()
                .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.AGGREGATED_BY_CONNECTION)
                .map(ConnectedEntity::getConnectedEntityId)
                .collect(Collectors.toSet());
        for (final String aggregatedBy : aggregatedBySet) {
            if (!aggregatedByIds.contains(aggregatedBy)) {
                logger.debug(
                        "Removing Cloud Commitment \"{}\" (ID: {}) due to missing Aggregated By: \"{}\"",
                        cloudCommitment.getDisplayName(), cloudCommitment.getLocalId(), aggregatedBy);
                return true;
            }
        }
        return false;
    }
}
