package com.vmturbo.history.stats.projected;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.stats.projected.ProjectedPriceIndexSnapshot.PriceIndexSnapshotFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * The {@link TopologyCommoditiesSnapshot} contains information about bought and sold commodities in
 * a topology, other than the price index (which arrives separately).
 *
 * <p>It's constructed from the projected {@link TopologyEntityDTO}s broadcast by the market,
 * and is immutable after construction.</p>
 */
@Immutable
public
class TopologyCommoditiesSnapshot implements MemReporter {

    /**
     * 0 is not a valid id, so we use it to represent "no provider".
     */
    static final long NO_PROVIDER_ID = 0L;

    private final SoldCommoditiesInfo soldCommoditiesInfo;
    private final BoughtCommoditiesInfo boughtCommoditiesInfo;
    private final EntityCountInfo entityCountInfo;
    private final ProjectedPriceIndexSnapshot projectedPriceIndexSnapshot;
    private final int topologySize;
    private final IDataPack<Long> oidPack;
    private final IDataPack<String> keyPack;

    /**
     * Create a new default factory for instances of {@link TopologyCommoditiesSnapshot}.
     *
     * @param excludedCommodityTypes commodity types to exclude from snapshot
     * @param oidPack                data pack for entity oids
     * @param keyPack                datapack for commodity keys
     * @return The factory to use to create instances.
     */
    static TopologyCommoditiesSnapshotFactory newFactory(
            Set<CommodityType> excludedCommodityTypes, IDataPack<Long> oidPack, IDataPack<String> keyPack) {
        return new TopologyCommoditiesSnapshotFactory() {
            @Nonnull
            @Override
            public TopologyCommoditiesSnapshot createSnapshot(
                    @Nonnull final RemoteIterator<ProjectedTopologyEntity> entities,
                    @Nonnull final PriceIndexSnapshotFactory priceIndexSnapshotFactory)
                    throws InterruptedException, TimeoutException, CommunicationException {
                return new TopologyCommoditiesSnapshot(
                        entities, excludedCommodityTypes, priceIndexSnapshotFactory, oidPack, keyPack);
            }
        };
    }

    private TopologyCommoditiesSnapshot(@Nonnull final RemoteIterator<ProjectedTopologyEntity> entities,
            @Nonnull final Set<CommodityDTO.CommodityType> excludedCommodityTypes,
            @Nonnull final PriceIndexSnapshotFactory priceIndexSnapshotFactory,
            @Nonnull final IDataPack<Long> oidPack,
            @Nonnull final IDataPack<String> keyPack)
            throws InterruptedException, TimeoutException, CommunicationException {
        IDataPack<String> commodityNamePack = new DataPack<>();
        Builder builder = new Builder(excludedCommodityTypes, commodityNamePack, oidPack, keyPack);
        while (entities.hasNext()) {
            entities.nextChunk().forEach(builder::addProjectedEntity);
        }
        final TopologyCommoditiesSnapshot newSnapshot = builder.build(priceIndexSnapshotFactory,
                oidPack, keyPack);
        this.soldCommoditiesInfo = newSnapshot.soldCommoditiesInfo;
        this.boughtCommoditiesInfo = newSnapshot.boughtCommoditiesInfo;
        this.entityCountInfo = newSnapshot.entityCountInfo;
        this.topologySize = newSnapshot.topologySize;
        this.projectedPriceIndexSnapshot = newSnapshot.projectedPriceIndexSnapshot;
        this.oidPack = oidPack;
        this.keyPack = keyPack;
    }

    @VisibleForTesting
    TopologyCommoditiesSnapshot(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo,
            @Nonnull final BoughtCommoditiesInfo boughtCommoditiesInfo,
            @Nonnull final EntityCountInfo entityCountInfo,
            @Nonnull final ProjectedPriceIndexSnapshot projectedPriceIndexSnapshot,
            @Nonnull final IDataPack<Long> oidPack,
            @Nonnull final IDataPack<String> keyPack,
            final int numEntities) {
        this.soldCommoditiesInfo = soldCommoditiesInfo;
        this.boughtCommoditiesInfo = boughtCommoditiesInfo;
        this.entityCountInfo = entityCountInfo;
        this.topologySize = numEntities;
        this.projectedPriceIndexSnapshot = projectedPriceIndexSnapshot;
        this.oidPack = oidPack;
        this.keyPack = keyPack;
    }

    long getTopologySize() {
        return topologySize;
    }

    /**
     * Get accumulated statistics records for a set of commodities names over a set of entities in
     * the topology.
     *
     * @param commodityNames The names of the commodities - must not be empty.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @param providerIds    ids of the potential commodity providers.
     * @return A stream of {@link StatRecord}s, with at most two {@link StatRecord}s for each
     * commodity (bought, sold). Stream may be empty if no entities in the target set are buying or
     * selling any of the commodities.
     */
    @Nonnull
    Stream<StatRecord> getRecords(@Nonnull final Set<String> commodityNames,
            @Nonnull final Set<Long> targetEntities,
            @Nonnull final Set<Long> providerIds) {
        if (commodityNames.isEmpty()) {
            throw new IllegalArgumentException("Must specify commodity names.");
        }

        return commodityNames.stream()
                .flatMap(commodityName ->
                        getCommodityRecords(commodityName, targetEntities, providerIds).stream());
    }

    /**
     * Get a comparator that can be used to compare entity IDs according to the passed-in pagination
     * parameters.
     *
     * @param paginationParams The {@link EntityStatsPaginationParams} used to order entities.
     * @param entitiesMap      mapping from seed entity to derived entities, the value can either
     *                         contain one single seed entity if this is the entity to get stats
     *                         for, or derived entities if these are the members to aggregate stats
     *                         on. The value must not be empty.
     * @return A {@link Comparator} that can be used to compare entity IDs according to the {@link
     * EntityStatsPaginationParams}. If an entity ID is not in this snapshot, or does not buy/sell
     * the commodity, it will be considered smaller than any entity ID that is in the snapshot and
     * does buy/sell.
     * @throws IllegalArgumentException If the sort commodity is invalid (e.g. price index, or a
     *                                  global count statistic like numVMs).
     */
    @Nonnull
    Comparator<Long> getEntityComparator(@Nonnull final EntityStatsPaginationParams paginationParams,
            @Nonnull final Map<Long, Set<Long>> entitiesMap)
            throws IllegalArgumentException {
        final String sortCommodity = paginationParams.getSortCommodity();
        if (entityCountInfo.isCountStat(sortCommodity)) {
            throw new IllegalArgumentException("Can't order by count commodity: "
                    + sortCommodity);
        } else if (sortCommodity.equals(StringConstants.PRICE_INDEX)) {
            return projectedPriceIndexSnapshot.getEntityComparator(paginationParams);
        }

        return (id1, id2) -> {
            // For each entity, the commodity should either be sold or bought. An entity
            // shouldn't buy and sell the same commodity.
            final double id1StatValue = entitiesMap.get(id1).stream()
                    .mapToDouble(id1Sub -> soldCommoditiesInfo.getValue(id1Sub, sortCommodity)
                            + boughtCommoditiesInfo.getValue(id1Sub, sortCommodity))
                    .sum();
            final double id2StatValue = entitiesMap.get(id2).stream()
                    .mapToDouble(id2Sub -> soldCommoditiesInfo.getValue(id2Sub, sortCommodity)
                            + boughtCommoditiesInfo.getValue(id2Sub, sortCommodity))
                    .sum();
            final int valComparisonResult = paginationParams.isAscending()
                    ? Double.compare(id1StatValue, id2StatValue)
                    : Double.compare(id2StatValue, id1StatValue);
            if (valComparisonResult == 0) {
                // In order to have a stable sort, we use the entity ID as the secondary sorting
                // parameter.
                return paginationParams.isAscending()
                        ? Long.compare(id1, id2)
                        : Long.compare(id2, id1);
            } else {
                return valComparisonResult;
            }
        };
    }

    @Nonnull
    private List<StatRecord> getCommodityRecords(@Nonnull final String commodityName,
            @Nonnull final Set<Long> targetEntities,
            @Nonnull final Set<Long> providerOids) {
        if (entityCountInfo.isCountStat(commodityName)) {
            return entityCountInfo.getCountRecord(commodityName)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else if (commodityName.equals("priceIndex")) {
            return projectedPriceIndexSnapshot.getRecord(targetEntities)
                    .map(Collections::singletonList)
                    .orElse(Collections.emptyList());
        } else {
            // This is probably a "regular" commodity.

            List<StatRecord> soldAccumulatedRecords =
                    soldCommoditiesInfo.getAccumulatedRecords(commodityName, targetEntities);
            final List<StatRecord> retList = new ArrayList<>(soldAccumulatedRecords);

            boughtCommoditiesInfo.getAccumulatedRecord(commodityName, targetEntities, providerOids)
                    .ifPresent(retList::add);

            return retList;
        }
    }

    /**
     * Builder to construct a {@link TopologyCommoditiesSnapshot} entity-by-entity.
     */
    public static class Builder implements MemReporter {
        final SoldCommoditiesInfo.Builder soldCommoditiesBuilder;
        final BoughtCommoditiesInfo.Builder boughtCommoditiesBuilder;
        final EntityCountInfo.Builder entityCountBuilder = EntityCountInfo.newBuilder();
        int numEntities = 0;

        final Long2FloatMap projectedPriceIndexByEntity = new Long2FloatOpenHashMap();

        /**
         * Create a new snapshot {@link Builder}.
         *
         * @param excludedCommodityTypes commodity types not to be captured
         * @param commodityNamePack      data pack that will contain all encoutnered commodity type
         *                               names
         * @param oidPack                data pack that will contain all encountered entity ids
         * @param keyPack                data pack for commodity keys
         */
        public Builder(@Nonnull final Set<CommodityDTO.CommodityType> excludedCommodityTypes,
                IDataPack<String> commodityNamePack, IDataPack<Long> oidPack, IDataPack<String> keyPack) {
            soldCommoditiesBuilder = SoldCommoditiesInfo.newBuilder(
                    excludedCommodityTypes, oidPack, keyPack);
            boughtCommoditiesBuilder = BoughtCommoditiesInfo.newBuilder(excludedCommodityTypes,
                    commodityNamePack, oidPack);
        }

        /**
         * Incorporated an entity into the snapshot under construction.
         *
         * @param entity the entity to add
         */
        public void addProjectedEntity(ProjectedTopologyEntity entity) {
            // add the entity to all the snapshot's internal components
            entityCountBuilder.addEntity(entity.getEntity());
            soldCommoditiesBuilder.addEntity(entity.getEntity());
            boughtCommoditiesBuilder.addEntity(entity.getEntity());
            projectedPriceIndexByEntity.put(entity.getEntity().getOid(), (float)entity.getProjectedPriceIndex());
            numEntities += 1;
        }

        /**
         * Finish the build and return the new snapshot object.
         *
         * @param priceIndexSnapshotFactory factory to obtain a projected price index snapshot
         * @param oidPack                   data pack for entity oids
         * @param keyPack                   data pack for commodity keys
         * @return the newly built snapshot
         */
        public TopologyCommoditiesSnapshot build(PriceIndexSnapshotFactory priceIndexSnapshotFactory,
                IDataPack<Long> oidPack, IDataPack<String> keyPack) {
            SoldCommoditiesInfo soldCommoditiesInfo = soldCommoditiesBuilder.build();
            BoughtCommoditiesInfo boughtCommoditiesInfo
                    = boughtCommoditiesBuilder.build(soldCommoditiesInfo);
            EntityCountInfo entityCountInfo = entityCountBuilder.build();
            ProjectedPriceIndexSnapshot projectedPriceIndexSnapshot
                    = priceIndexSnapshotFactory.createSnapshot(projectedPriceIndexByEntity);
            return new TopologyCommoditiesSnapshot(
                    soldCommoditiesInfo,
                    boughtCommoditiesInfo,
                    entityCountInfo,
                    projectedPriceIndexSnapshot,
                    oidPack, keyPack, numEntities);
        }

        @Override
        public Long getMemSize() {
            return null;
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return Arrays.asList(soldCommoditiesBuilder, boughtCommoditiesBuilder, entityCountBuilder);
        }
    }

    /**
     * A factory for {@link TopologyCommoditiesSnapshot}, used for dependency injection for unit
     * tests. We don't really need a factory otherwise, since all of these classes are private to
     * the {@link ProjectedStatsStore} implementation.
     */
    interface TopologyCommoditiesSnapshotFactory {

        @Nonnull
        TopologyCommoditiesSnapshot createSnapshot(
                @Nonnull RemoteIterator<ProjectedTopologyEntity> entities,
                @Nonnull PriceIndexSnapshotFactory priceIndexSnapshotFactory)
                throws InterruptedException, TimeoutException, CommunicationException;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                boughtCommoditiesInfo,
                soldCommoditiesInfo,
                entityCountInfo,
                projectedPriceIndexSnapshot,
                new SimpleMemReporter("oidPack", oidPack),
                new SimpleMemReporter("keyPack", keyPack)
        );
    }
}
