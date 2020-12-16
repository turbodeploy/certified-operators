package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.TopologyCommoditiesSnapshot.NO_PROVIDER_ID;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.google.protobuf.TextFormat;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.utils.DataPacks;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedBoughtCommodity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * This class contains information about commodities bought by entities in a topology.
 *
 * <p>It's immutable, because for any given topology the bought commodities don't change.</p>
 */
// TODO 63613 Use commodity type enums for exclusion, drop comm type data pack
@Immutable
class BoughtCommoditiesInfo implements MemReporter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Bought commodity info for each commodity.
     *
     * <pre>
     * commodity name (as an index into a string pack)
     *   -> buyer entity (as an index into the entity pack)
     *      -> seller entity (as an index into the entity pack)
     *         -> one or more {@link BoughtCommodity} with commodity details
     * </pre>
     *
     * <p>Please note that there can be multiple {@link CommodityBoughtDTO} for a given
     * (commodity name, provider id) tuple. This is because the commodity name string doesn't take
     * into account the commodity key. In addition, an entity can buy from the same provider, but in
     * multiple separate commodities sets. In this case, the same commodities will we bought by same
     * provider. Example: VM1 buys from ST1 and ST2. Market is generating a move action, from ST1 to
     * ST2. Now VM1 is buying 2 sets for commodities, both from ST2. This data structure is
     * flattening those separations, but the stats API doesn't currently support commodity keys
     * anyway.</p>
     */
    private final Map<Integer, Map<Integer, List<Long>>> boughtCommodities;

    /**
     * The {@link SoldCommoditiesInfo} for the topology. This is required to look up capacities.
     */
    private final SoldCommoditiesInfo soldCommoditiesInfo;
    // data pack of entity oids
    private final IDataPack<Long> oidPack;
    private final IDataPack<BoughtCommodity> cbPack;

    private BoughtCommoditiesInfo(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo,
            @Nonnull final Map<Integer, Map<Integer, List<Long>>> boughtCommodities,
            IDataPack<Long> oidPack, @Nonnull final IDataPack<BoughtCommodity> cbPack) {
        this.boughtCommodities = Collections.unmodifiableMap(boughtCommodities);
        this.soldCommoditiesInfo = soldCommoditiesInfo;
        this.oidPack = oidPack;
        this.cbPack = cbPack;
    }

    @Nonnull
    static Builder newBuilder(Set<CommodityType> excludedCommodityTypes,
            IDataPack<String> commodityNamePack, IDataPack<Long> oidPack) {
        return new Builder(excludedCommodityTypes, commodityNamePack, oidPack);
    }

    /**
     * Get the value of a particular commodity bought by a particular entity.
     *
     * @param entity        The ID of the entity.
     * @param commodityName The name of the commodity.
     * @return The average used amount of all commodities matching the name bought by the entity.
     * This is the same formula we use to calculate "currentValue" for stat records. Returns 0 if
     * the entity does not buy the commodity.
     */
    double getValue(final long entity,
            @Nonnull final String commodityName) {
        final int commmodityTypeNo = UICommodityType.fromString(commodityName).sdkType().getNumber();
        final Map<Integer, List<Long>> boughtByEntities = boughtCommodities.get(commmodityTypeNo);
        final int entityIndex = oidPack.toIndex(entity);
        if (boughtByEntities != null) {
            final List<Long> boughtByEntity = boughtByEntities.get(entityIndex);
            if (boughtByEntity != null) {
                double value = boughtByEntity.stream()
                        .mapToInt(key -> DataPacks.unpackInts(key)[1])
                        .mapToObj(cbPack::fromIndex)
                        .mapToDouble(BoughtCommodity::getUsed)
                        .sum();
                return value / boughtByEntity.size();
            }
        }
        return 0.0;
    }

    /**
     * Get the accumulated information about a particular commodity bought by a set of entities.
     *
     * @param commodityName  The name of the commodity. The names are derived from {@link
     *                       CommodityType}. This is not ideal - we should consider using the {@link
     *                       CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @param providerOids   oids of the potential commodity providers.
     * @return An optional containing the accumulated {@link StatRecord}, or an empty optional if
     * there is no information for the commodity over the target entities.
     */
    Optional<StatRecord> getAccumulatedRecord(@Nonnull final String commodityName,
            @Nonnull final Set<Long> targetEntities,
            @Nonnull final Set<Long> providerOids) {
        final int commodityTypeNo = UICommodityType.fromString(commodityName).sdkType().getNumber();
        final Map<Integer, List<Long>> boughtByEntities =
                boughtCommodities.get(commodityTypeNo);
        // allocate return record; it'll be returned with zeros if there are no buys
        final AccumulatedBoughtCommodity overallCommoditiesBought =
                new AccumulatedBoughtCommodity(commodityName);
        if (boughtByEntities != null) {
            Collection<Integer> targetIndexes = targetEntities.isEmpty() ? boughtByEntities.keySet()
                    : getEntityIndexes(targetEntities);
            Collection<Integer> providerIndexes = providerOids.isEmpty() ? Collections.emptyList()
                    : getEntityIndexes(providerOids);
            for (final Integer targetIndex : targetIndexes) {
                final List<Long> boughtByEntity = boughtByEntities.get(targetIndex);
                if (boughtByEntity == null) {
                    logger.debug("Entity {} not buying {}", oidPack.fromIndex(targetIndex), commodityName);
                    continue;
                }
                for (long packed : boughtByEntity) {
                    final int[] unpacked = DataPacks.unpackInts(packed);
                    if (!providerIndexes.isEmpty() && !providerIndexes.contains(unpacked[0])) {
                        continue;
                    }
                    long providerId = oidPack.fromIndex(unpacked[0]);
                    BoughtCommodity commodityBought = cbPack.fromIndex(unpacked[1]);
                    final Optional<Double> capacity = providerId == NO_PROVIDER_ID ? Optional.empty()
                            : soldCommoditiesInfo.getCapacity(commodityName, providerId);
                    if (providerId == NO_PROVIDER_ID || capacity.isPresent()) {
                        overallCommoditiesBought.recordBoughtCommodity(commodityBought, providerId,
                                capacity.orElse(0.0));
                    } else {
                        logger.warn("Entity {} buying commodity {} from provider {}, "
                                        + "but provider is not selling it!",
                                oidPack.fromIndex(targetIndex), commodityName, providerId);

                    }
                }
            }
        }
        return overallCommoditiesBought.toStatRecord();
    }

    private List<Integer> getEntityIndexes(Collection<Long> providerOids) {
        return providerOids.stream()
                .map(oidPack::toIndex)
                .collect(Collectors.toList());
    }

    /**
     * Utility class to capture the commodity information we need for projected stats. This saves a
     * LOT of memory compared to keeping the full {@link CommodityBoughtDTO} around in large
     * topologies.
     *
     * <p>This class implements {@link #equals(Object)} and {@link #hashCode()} so it can be used
     * as
     * the value type in a {@link DataPack}.</p>
     */
    static class BoughtCommodity {
        private final float used;
        private final float peak;
        private final float usedPercentile;
        private final boolean hasUsedPercentile;

        BoughtCommodity(CommodityBoughtDTO commBought) {
            this.used = (float)commBought.getUsed();
            this.peak = (float)commBought.getPeak();
            this.usedPercentile = (float)commBought.getHistoricalUsed().getPercentile();
            this.hasUsedPercentile = commBought.getHistoricalUsed().hasPercentile();
        }

        public double getUsed() {
            return used;
        }

        public double getPeak() {
            return peak;
        }

        public boolean hasPercentile() {
            return hasUsedPercentile;
        }

        public double getPercentile() {
            return usedPercentile;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final BoughtCommodity that = (BoughtCommodity)o;
            return Double.compare(that.used, used) == 0
                    && Double.compare(that.peak, peak) == 0
                    && Double.compare(that.usedPercentile, usedPercentile) == 0
                    && hasUsedPercentile == that.hasUsedPercentile;
        }

        @Override
        public int hashCode() {
            return Objects.hash(used, peak, usedPercentile, hasUsedPercentile);
        }
    }


    /**
     * A builder to construct an immutable {@link BoughtCommoditiesInfo}.
     */
    static class Builder implements MemReporter {
        private final Map<Integer, Map<Integer, List<Long>>> boughtCommodities
                = new Int2ObjectOpenHashMap<>();

        private final Map<Integer, MutableInt> duplicateCommoditiesBought
                = new Int2ObjectOpenHashMap<>();

        private final IDataPack<Long> oidPack;
        private final Set<Integer> excludedCommodityTypeNos;
        IDataPack<BoughtCommodity> bcPack = new DataPack<>();

        private Builder(Set<CommodityType> excludedCommodityTypes,
                IDataPack<String> commodityNamePack, IDataPack<Long> oidPack) {
            this.excludedCommodityTypeNos = excludedCommodityTypes.stream()
                    .map(CommodityType::getNumber)
                    .collect(Collectors.toSet());
            this.oidPack = oidPack;
        }

        /**
         * Record the commodities bought by the given entity in the {@link #boughtCommodities}. The
         * first-level key is the entity's OID. The second-level key is the seller's OID.
         *
         * @param entity the entity that is buying the commodities
         * @return the builder
         */
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            // iterate over the different commodities sets (comm set).
            // each one represents a Set<CommodityBoughtDTO> that the consumer needs to buy from the
            // same provider.
            // Please note that multiple comm sets can exists, and even more than one of them can
            // buy from the same provider.
            entity.getCommoditiesBoughtFromProvidersList()
                    .forEach(cbfp -> addCBFP(cbfp, entity));
            return this;
        }

        private void addCBFP(CommoditiesBoughtFromProvider cbfp, TopologyEntityDTO entity) {
            long providerId = cbfp.hasProviderId() ? cbfp.getProviderId() : NO_PROVIDER_ID;
            Set<TopologyDTO.CommodityType> seenCommTypes = new HashSet<>();
            cbfp.getCommodityBoughtList().stream()
                    .filter(cb -> !excludedCommodityTypeNos.contains(cb.getCommodityType().getType()))
                    .forEach(cb -> {
                        final TopologyDTO.CommodityType commType = cb.getCommodityType();
                        if (seenCommTypes.add(commType)) {
                            addCB(cb, entity.getOid(), providerId);
                        } else {
                            // in this case we are buying duplicate commodity from same provider
                            // in the same commodity set. we print a message and we don't save it
                            logger.debug("Entity {}[{}] is buying the duplicate commodity {} "
                                            + "from the provider {}. Skipping saving stats all but first buy.",
                                    entity::getOid, entity::getDisplayName,
                                    () -> TextFormat.shortDebugString(commType), () -> providerId);
                            // count these for summary logging
                            duplicateCommoditiesBought.computeIfAbsent(commType.getType(), MutableInt::new)
                                    .increment();
                        }
                    });
        }

        private void addCB(CommodityBoughtDTO cb, long entityOid, long providerOid) {
            final Map<Integer, List<Long>> entitiesBought =
                    boughtCommodities.computeIfAbsent(cb.getCommodityType().getType(),
                            _type -> new Int2ObjectOpenHashMap<>());
            final List<Long> entityBought =
                    entitiesBought.computeIfAbsent(oidPack.toIndex(entityOid),
                            _oid -> new LongArrayList());
            long packed = DataPacks.packInts(oidPack.toIndex(providerOid),
                    bcPack.toIndex(new BoughtCommodity(cb)));
            entityBought.add(packed);
        }

        /**
         * Construct the {@link BoughtCommoditiesInfo} once all entities have been added.
         *
         * @param soldCommoditiesInfo The {@link SoldCommoditiesInfo} constructed using the same
         *                            topology.
         * @return a new {@link BoughtCommoditiesInfo} containing the current boughCommodities
         */
        @Nonnull
        BoughtCommoditiesInfo build(@Nonnull final SoldCommoditiesInfo soldCommoditiesInfo) {
            // provide summary logs of commodities that arose in duplicate buying scenarios
            duplicateCommoditiesBought.forEach((k, v) ->
                    logger.warn("Commodity was involved in {} duplicate buying relationships: "
                            + "type {}; log@DEBUG for details", v, k));
            ((Int2ObjectOpenHashMap<?>)boughtCommodities).trim();
            // trim all the fastutil structures we just assembled, since they're all complete
            Stream.of(boughtCommodities)
                    // trim the top-level map
                    .peek(map -> ((Int2ObjectOpenHashMap<?>)map).trim())
                    .flatMap(map -> map.values().stream())
                    // trim the by-entity maps
                    .peek(map -> ((Int2ObjectOpenHashMap<?>)map).trim())
                    .flatMap(map -> map.values().stream())
                    // trim the by-provider maps
                    .forEach(list -> ((LongArrayList)list).trim());
            bcPack.freeze(true);
            duplicateCommoditiesBought.clear();
            ((Int2ObjectOpenHashMap<?>)duplicateCommoditiesBought).trim();
            return new BoughtCommoditiesInfo(soldCommoditiesInfo, boughtCommodities, oidPack, bcPack);
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return Arrays.asList(
                    new SimpleMemReporter("boughtCommodities", boughtCommodities),
                    new SimpleMemReporter("duplicateCommoditiesBought", duplicateCommoditiesBought),
                    new SimpleMemReporter("bcPack", bcPack),
                    new SimpleMemReporter("oidPack", oidPack)
            );
        }

        @Override
        public Collection<Object> getMemExclusions() {
            return Collections.singletonList(oidPack);
        }
    }

    @Override
    public Collection<Object> getMemExclusions() {
        return Arrays.asList(oidPack, soldCommoditiesInfo);
    }
}
