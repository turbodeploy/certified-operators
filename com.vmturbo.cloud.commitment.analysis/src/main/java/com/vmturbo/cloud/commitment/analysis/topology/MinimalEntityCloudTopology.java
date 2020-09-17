package com.vmturbo.cloud.commitment.analysis.topology;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

/**
 * An implementation of {@link MinimalCloudTopology} supporting {@link MinimalEntity} as the entity
 * class type.
 */
public class MinimalEntityCloudTopology implements MinimalCloudTopology<MinimalEntity> {

    private final Map<Long, MinimalEntity> minimalEntitiesByOid;

    private final BillingFamilyRetriever billingFamilyRetriever;

    /**
     * Construct a minimal cloud topology. The provided entity stream will be filtered, based on the
     * environment type of each entity.
     *
     * @param minimalEntities A stream of what is presumed to be all entities within a given topology.
     * @param billingFamilyRetriever A {@link BillingFamilyRetriever}, used to query the billing family
     *                               associated with an entity.
     */
    MinimalEntityCloudTopology(@Nonnull final Stream<MinimalEntity> minimalEntities,
                               @Nonnull final BillingFamilyRetriever billingFamilyRetriever) {

        this.billingFamilyRetriever = Objects.requireNonNull(billingFamilyRetriever);
        this.minimalEntitiesByOid = minimalEntities
                .filter(e -> e.getEnvironmentType() == EnvironmentType.CLOUD)
                .collect(
                        ImmutableMap.toImmutableMap(
                                MinimalEntity::getOid,
                                Function.identity()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, MinimalEntity> getEntities() {
        return minimalEntitiesByOid;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<MinimalEntity> getEntity(final long entityId) {
        return Optional.ofNullable(minimalEntitiesByOid.get(entityId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean entityExists(final long entityOid) {
        return minimalEntitiesByOid.containsKey(entityOid);
    }

    @Nonnull
    @Override
    public Optional<Boolean> isEntityPoweredOn(final long entityOid) {
        return getEntity(entityOid)
                .map(entity -> entity.getEntityState() == EntityState.POWERED_ON);
    }


    /**
     * The default implementation of a factory for {@link MinimalEntityCloudTopology}.
     */
    public static class DefaultMinimalEntityCloudTopologyFactory implements MinimalCloudTopologyFactory<MinimalEntity> {

        private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory;

        /**
         * Constructs a new instance of the factory.
         *
         * @param billingFamilyRetrieverFactory The factory for creating {@link BillingFamilyRetriever} instances.
         *                                      A new retriever will be created for each cloud topology instance.
         */
        public DefaultMinimalEntityCloudTopologyFactory(@Nonnull BillingFamilyRetrieverFactory billingFamilyRetrieverFactory) {
            this.billingFamilyRetrieverFactory = Objects.requireNonNull(billingFamilyRetrieverFactory);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public MinimalCloudTopology<MinimalEntity> createCloudTopology(@Nonnull final Stream<MinimalEntity> entities) {
            return new MinimalEntityCloudTopology(entities, billingFamilyRetrieverFactory.newInstance());
        }
    }
}
