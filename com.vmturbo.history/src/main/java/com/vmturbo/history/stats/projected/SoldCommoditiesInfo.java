package com.vmturbo.history.stats.projected;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.utils.DataPacks;
import com.vmturbo.components.common.utils.DataPacks.CommodityTypeDataPack;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * This class contains information about commodities sold by entities in a topology.
 *
 * <p>It's immutable, because for any given topology the sold commodities don't change.</p>
 */
@Immutable
class SoldCommoditiesInfo implements MemReporter {
    private static final Logger logger = LogManager.getLogger();

    /**
     * (commodity name) -> ( (entity id) -> (DTO describing commodity sold) )
     *
     * <p>Each entity should only have one {@link CommoditySoldDTO} for a given commodity name.
     * This may not be true in general, because the commodity name string doesn't take into account
     * the commodity spec keys. But the stats API doesn't currently support commodity spec keys
     * anyway.
     */
    private final Map<Integer, Map<Integer, List<Integer>>> soldCommodities;
    private final IDataPack<Long> oidPack;
    private final IDataPack<SoldCommodity> scPack;
    private final IDataPack<String> keyPack;

    private SoldCommoditiesInfo(
            @Nonnull final Map<Integer, Map<Integer, List<Integer>>> soldCommodities,
            @Nonnull final IDataPack<Long> oidPack,
            @Nonnull final IDataPack<String> keyPack,
            @Nonnull final IDataPack<SoldCommodity> scPack) {
        this.soldCommodities = Collections.unmodifiableMap(soldCommodities);
        this.oidPack = oidPack;
        this.keyPack = keyPack;
        this.scPack = scPack;
    }

    @Nonnull
    static Builder newBuilder(Set<CommodityDTO.CommodityType> excludedCommodityTypes,
            IDataPack<Long> oidPack, IDataPack<String> keyPack) {
        return new Builder(excludedCommodityTypes, oidPack, keyPack);
    }

    /**
     * Get the value of a particular commodity sold by a particular entity.
     *
     * @param entity            The ID of the entity. It's a {@link Long} instead of a base type to
     *                          avoid autoboxing.
     * @param commodityTypeName commodity type name
     * @return The average used amount of all commodities matching the name sold by the entity. This
     * is the same formula we use to calculate "currentValue" for stat records. Returns 0 if the
     * entity does not sell the commodity.
     */
    double getValue(final long entity,
            @Nonnull final String commodityTypeName) {
        double sum = 0.0;
        int count = 0;
        final Map<Integer, List<Integer>> soldByEntityId =
                soldCommodities.get(toCommodityTypeNo(commodityTypeName));
        if (soldByEntityId != null && !soldByEntityId.isEmpty()) {
            final List<SoldCommodity> soldByEntity =
                    soldByEntityId.getOrDefault(oidPack.toIndex(entity), Collections.emptyList()).stream()
                            .map(scPack::fromIndex)
                            .collect(Collectors.toList());
            for (final SoldCommodity soldCommodity : soldByEntity) {
                sum += soldCommodity.getUsed();
                count += 1;
            }
        }
        return count > 0 ? sum / count : 0.0;
    }

    private int toCommodityTypeNo(String commodityTypeName) {
        return UICommodityType.fromString(commodityTypeName).sdkType().getNumber();
    }

    /**
     * Get a list of {@link StatRecord}s which have accumulated information about a particular
     * commodity sold by a set of entities. The commodities with the same name but different
     * keys won't be accumulated: different {@link StatRecord} for each of them.
     *
     * @param commodityName The name of the commodity. The names are derived from
     *           {@link CommodityType}. This is not ideal - we should consider using the
     *           {@link CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return A list of the accumulated {@link StatRecord}, or an empty list
     *         if there is no information for the commodity over the target entities
     *         or this commodity is not sold.
     */
    @Nonnull
    List<StatRecord> getAccumulatedRecords(@Nonnull final String commodityName,
                                               @Nonnull final Set<Long> targetEntities) {
        Map<String, AccumulatedSoldCommodity> accumulatedSoldCommodities = new HashMap<>();
        final Map<Integer, List<Integer>> soldByEntityId =
                soldCommodities.get(toCommodityTypeNo(commodityName));
        if (soldByEntityId == null) {
            return Collections.emptyList();
        }
        final Collection<Integer> targetIndexes = targetEntities.isEmpty()
                ? soldByEntityId.keySet()
                : targetEntities.stream().map(oidPack::toIndex).collect(Collectors.toList());
        targetIndexes.forEach(targetIndex -> {
            final List<SoldCommodity> soldCommodityValues =
                    soldByEntityId.getOrDefault(targetIndex, Collections.emptyList()).stream()
                            .map(scPack::fromIndex)
                            .peek(sc -> {
                                if (sc == null) {
                                    logger.warn("Requested commodity {} not sold by entity {}",
                                            commodityName, oidPack.fromIndex(targetIndex));
                                }
                            })
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            accumulateCommoditiesByKey(soldCommodityValues, commodityName, accumulatedSoldCommodities);
        });
        return accumulatedSoldCommodities.values()
                .stream()
                .map(AccumulatedSoldCommodity::toStatRecord)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Accumulate sold commodities with the same name by key.
     *
     * @param commoditySoldList sold commodity list
     * @param commodityName commodity name
     * @param accumulatedSoldCommodities the map to collect accumulated commodities by key
     */
    private void accumulateCommoditiesByKey(Collection<SoldCommodity> commoditySoldList, String commodityName,
            Map<String, AccumulatedSoldCommodity> accumulatedSoldCommodities) {
        commoditySoldList.forEach(commoditySold -> {
            final String commodityKey = commoditySold.getKey(keyPack);
            AccumulatedSoldCommodity accumulatedSoldCommodity =
                    accumulatedSoldCommodities.computeIfAbsent(
                            commodityKey == null ? "empty_key" : commodityKey,
                            k -> new AccumulatedSoldCommodity(commodityName, commodityKey));
            accumulatedSoldCommodity.recordSoldCommodity(commoditySold);
        });
    }

    /**
     * Compute the total capacity over all commodities sold by a given provider.
     *
     * @param commodityName name of the commodity to average over
     * @param providerId the ID of the provider of these commodities
     * @return the total capacity over all commodities sold by this provider
     */
    @Nonnull
    public Optional<Double> getCapacity(@Nonnull final String commodityName,
                                        final long providerId) {
        return Optional.ofNullable(soldCommodities.get(toCommodityTypeNo(commodityName)))
                .map(providers -> providers.get(oidPack.toIndex(providerId)))
                .map(scIndexes -> scIndexes.stream()
                        .map(scPack::fromIndex)
                        .mapToDouble(SoldCommodity::getCapacity)
                        .sum());
    }

    /**
     * Utility class to capture the commodity information we need for projected stats. This
     * saves a LOT of memory compared to keeping the full {@link CommodityBoughtDTO} around in
     * large topologies.
     *
     * <p>This class overrides {@link #equals(Object)} and {@link #hashCode()} so it can be used
     * as the value type in a {@link DataPack}.</p>
     */
    static class SoldCommodity {
        private final float used;
        private final float peak;
        private final float capacity;
        private final float usedPercentile;
        private final boolean hasUsedPercentile;
        private final int keyIndex;

        SoldCommodity(CommoditySoldDTO sold, IDataPack<String> keyPack) {
            // we save object ref overhead for the commodity key by using the data pack that's
            // included in the shared ingester state
            this.keyIndex = keyPack.toIndex(sold.getCommodityType().getKey());
            this.used = (float)sold.getUsed();
            this.peak = (float)sold.getPeak();
            this.capacity = (float)sold.getCapacity();
            this.usedPercentile = (float)sold.getHistoricalUsed().getPercentile();
            this.hasUsedPercentile = sold.getHistoricalUsed().hasPercentile();
        }

        public String getKey(IDataPack<String> keyPack) {
            return keyPack.fromIndex(keyIndex);
        }

        public double getUsed() {
            return used;
        }

        public double getPeak() {
            return peak;
        }

        public double getCapacity() {
            return capacity;
        }

        public double getPercentile() {
            return usedPercentile;
        }

        public boolean hasPercentile() {
            return hasUsedPercentile;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SoldCommodity that = (SoldCommodity)o;
            return Double.compare(that.used, used) == 0
                    && Double.compare(that.peak, peak) == 0
                    && Double.compare(that.capacity, capacity) == 0
                    && Double.compare(that.usedPercentile, usedPercentile) == 0
                    && hasUsedPercentile == that.hasUsedPercentile
                    && keyIndex == that.keyIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(used, peak, capacity, usedPercentile, hasUsedPercentile, keyIndex);
        }
    }

    /**
     * A builder to construct the {@link SoldCommoditiesInfo}.
     */
    static class Builder implements MemReporter {

        // commodity type -> seller oid -> sold Commodities
        private final Map<Integer, Map<Integer, List<Integer>>> soldCommodities =
                new Int2ObjectOpenHashMap<>();

        /**
         * Track duplicate commodities sold by the same seller. We keep this map separate from the
         * main "soldCommodities" map because we only need the {@link CommodityType} during {@link
         * SoldCommoditiesInfo} construction.
         */
        private final Set<Long> previouslySeenCommodities = new LongOpenHashSet();

        private final Set<Integer> excludedCommodityTypeNos;
        private final IDataPack<Long> oidPack;
        private final IDataPack<SoldCommodity> scPack = new DataPack<>();
        private final CommodityTypeDataPack commodityTypePack = new CommodityTypeDataPack();
        private final IDataPack<String> keyPack;

        private Builder(Set<CommodityDTO.CommodityType> excludedCommodityTypes,
                IDataPack<Long> oidPack, IDataPack<String> keyPack) {
            this.excludedCommodityTypeNos = excludedCommodityTypes.stream()
                    .mapToInt(CommodityDTO.CommodityType::getNumber)
                    .collect(IntOpenHashSet::new, IntOpenHashSet::add, IntOpenHashSet::addAll);
            this.oidPack = oidPack;
            this.keyPack = keyPack;
        }

        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entity.getCommoditySoldListList().forEach(commoditySold -> {
                // Enforce commodity exclusions.
                final int commType = commoditySold.getCommodityType().getType();
                if (!excludedCommodityTypeNos.contains(commType)) {
                    final Map<Integer, List<Integer>> entitySellers =
                            soldCommodities.computeIfAbsent(commType, k -> new Int2ObjectOpenHashMap<>());
                    saveIfNoCollision(entity, commoditySold, entitySellers);
                }
            });
            return this;
        }

        @Nonnull
        SoldCommoditiesInfo build() {
            previouslySeenCommodities.clear();
            ((LongOpenHashSet)previouslySeenCommodities).trim();
            Stream.of(soldCommodities)
                    // trim the top-level map
                    .peek(map -> ((Int2ObjectOpenHashMap<?>)map).trim())
                    // trim the per-entity maps
                    .flatMap(map -> map.values().stream())
                    .peek(map -> ((Int2ObjectOpenHashMap<?>)map).trim())
                    // trim the SoldCommodity lists
                    .flatMap(map -> map.values().stream())
                    .forEach(list -> ((IntArrayList)list).trim());
            scPack.freeze(true);
            commodityTypePack.clear();
            return new SoldCommoditiesInfo(soldCommodities, oidPack, keyPack, scPack);
        }

        /**
         * Check to see if the given commodity type (including key) for the given seller is already
         * listed in the commoditiesSoldMap.
         *
         * @param sellerEntity       the ServiceEntity of the seller
         * @param commodityToAdd     the new commodity to check for "already listed"
         * @param commoditiesSoldMap the map from Seller OID to Collection of Commodities
         */
        // TODO Should we change the logic so that we accumulate data across commodity keys?
        private void saveIfNoCollision(@Nonnull TopologyEntityDTO sellerEntity,
                CommoditySoldDTO commodityToAdd,
                Map<Integer, List<Integer>> commoditiesSoldMap) {
            // check if a previous commodity for this seller has same CommodityType (type & key)
            final int oidIndex = oidPack.toIndex(sellerEntity.getOid());
            int commTypeIndex = commodityTypePack.toIndex(commodityToAdd.getCommodityType());
            if (previouslySeenCommodities.add(DataPacks.packInts(oidIndex, commTypeIndex))) {
                commoditiesSoldMap.computeIfAbsent(oidIndex, _oid -> new IntArrayList())
                        .add(scPack.toIndex(new SoldCommodity(commodityToAdd, keyPack)));
            } else {
                // previous commodity with the same type & key; print a warning and don't save it
                logger.warn("Entity {} selling commodity type { {} } more than once.",
                        sellerEntity.getOid(), commodityTypePack.fromIndex(commTypeIndex));
            }
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return Arrays.asList(
                    new SimpleMemReporter("soldCommodities", soldCommodities),
                    new SimpleMemReporter("previouslySeenCommodities", previouslySeenCommodities),
                    new SimpleMemReporter("commodityTypePack", commodityTypePack),
                    new SimpleMemReporter("scPack", scPack)
            );
        }

        @Override
        public Collection<Object> getMemExclusions() {
            return Arrays.asList(oidPack, keyPack);
        }
    }

    @Override
    public Collection<Object> getMemExclusions() {
        return Arrays.asList(oidPack, keyPack);
    }
}
