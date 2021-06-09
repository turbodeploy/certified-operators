package com.vmturbo.extractor.action.commodity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2FloatOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ActionCommodity;

/**
 * Captures per-entity-per-commodity percentile data scraped from a source or projected topology.
 */
class TopologyActionCommodityData {
    private final Int2ObjectMap<CommoditiesOfType> commoditiesOfType = new Int2ObjectOpenHashMap<>();

    // entity id -> commType -> percentiles
    private final Long2ObjectOpenHashMap<Object2DoubleOpenHashMap<CommType>> soldPercentilesByEntity =
            new Long2ObjectOpenHashMap<>();

    // entity id -> UICommodityType -> ActionCommodityAggregator
    private final Long2ObjectOpenHashMap<Short2ObjectOpenHashMap<ActionCommodity>>
            soldCommodityByEntity = new Long2ObjectOpenHashMap<>();

    // bought: entity id -> commodity type -> percentiles
    private final Long2ObjectOpenHashMap<Object2DoubleOpenHashMap<CommType>> boughtPercentilesByEntity =
            new Long2ObjectOpenHashMap<>();
    // bought: entity id -> commodity type -> ActionCommodity
    private final Long2ObjectOpenHashMap<Short2ObjectOpenHashMap<ActionCommodity>>
            boughtCommodityByEntity = new Long2ObjectOpenHashMap<>();
    // bought: entity id -> provider id -> commodity type -> used
    private Long2ObjectOpenHashMap<Long2ObjectOpenHashMap<Short2FloatOpenHashMap>>
            boughtCommodityUsedByEntity = new Long2ObjectOpenHashMap<>();

    private static final double NO_PERCENTILE_VALUE = -1.0;

    private static final Short2ObjectOpenHashMap<ActionCommodity> EMPTY = new Short2ObjectOpenHashMap<>();

    Optional<Double> getSoldPercentile(long entityId, CommodityType commodityType) {
        Object2DoubleMap<CommType> percentileByCommodity = soldPercentilesByEntity.get(entityId);
        if (percentileByCommodity != null) {
            double percentile = percentileByCommodity.getDouble(getCommType(commodityType));
            if (percentile != NO_PERCENTILE_VALUE) {
                return Optional.of(percentile);
            }
        }
        return Optional.empty();
    }

    /**
     * Get all processed commodity types.
     *
     * @return all processed commodity types.
     */
    public IntSet getCommodityTypes() {
        IntSet ret = new IntOpenHashSet();
        soldCommodityByEntity.values().forEach(s -> s.keySet().forEach((IntConsumer)ret::add));
        boughtCommodityByEntity.values().forEach(s -> s.keySet().forEach((IntConsumer)ret::add));
        return ret;
    }

    @VisibleForTesting
    Short2ObjectMap<ActionCommodity> getSoldCommms(final long entityId) {
        return soldCommodityByEntity.getOrDefault(entityId, EMPTY);
    }

    @VisibleForTesting
    Short2ObjectOpenHashMap<ActionCommodity> getBoughtCommms(final long entityId) {
        return boughtCommodityByEntity.get(entityId);
    }

    /**
     * Helper interface to process differences in commodities between two
     * {@link TopologyActionCommodityData}s.
     */
    public interface ImpactedCommoditiesConsumer {
        /**
         * Process the differences in source/projected commodities.
         *
         * @param commType commodity type
         * @param mine commodity from source topology
         * @param other commodity from projected topology
         */
        void accept(short commType, @Nullable ActionCommodity mine, @Nullable ActionCommodity other);
    }

    void visitDifferentCommodities(final long entityId, TopologyActionCommodityData other, ImpactedCommoditiesConsumer consumer) {
        final Short2ObjectMap<ActionCommodity> mine = getSoldCommms(entityId);
        final Short2ObjectMap<ActionCommodity> theirs = other.getSoldCommms(entityId);

        // Loop over "mine", and consume any commodities that are different from "theirs."
        mine.short2ObjectEntrySet().forEach(e ->
                visitCommodity(e.getShortKey(), e.getValue(), theirs, consumer));

        // process bought commodities
        final Short2ObjectMap<ActionCommodity> mineBought = getBoughtCommms(entityId);
        final Short2ObjectMap<ActionCommodity> theirBought = other.getBoughtCommms(entityId);
        if (mineBought != null && theirBought != null) {
            mineBought.short2ObjectEntrySet().forEach(e ->
                    visitCommodity(e.getShortKey(), e.getValue(), theirBought, consumer));
        }
    }

    private void visitCommodity(short commType, ActionCommodity actionCommodity,
            Short2ObjectMap<ActionCommodity> theirs, ImpactedCommoditiesConsumer consumer) {
        final ActionCommodity myComm = actionCommodity;
        final ActionCommodity theirComm = theirs.get(commType);
        final boolean different;
        if (theirComm == null) {
            different = true;
        } else {
            float totalDelta = Math.abs(myComm.getCapacity() - theirComm.getCapacity()
                    + myComm.getUsed() - theirComm.getUsed());
            // We only care about commodities with a non-negligible difference.
            different = totalDelta > 0.001f;
        }

        if (different) {
            consumer.accept(commType, myComm, theirComm);
        }
    }

    /**
     * Set capacity for bought commodity (get capacity from the commodity sold by provider).
     */
    public void populateCapacityForBoughtCommodities() {
        boughtCommodityUsedByEntity.long2ObjectEntrySet().forEach(entry -> {
            Short2ObjectOpenHashMap<ActionCommodity> commodityByType =
                    boughtCommodityByEntity.computeIfAbsent(entry.getLongKey(),
                            k -> new Short2ObjectOpenHashMap<>());
            entry.getValue().long2ObjectEntrySet().forEach(e -> {
                // get same sold commodity from provider
                long providerId = e.getLongKey();
                Short2ObjectOpenHashMap<ActionCommodity> soldCommodityByType =
                        soldCommodityByEntity.get(providerId);
                if (soldCommodityByType != null) {
                    e.getValue().short2FloatEntrySet().forEach(typeUsed -> {
                        ActionCommodity soldCommodity = soldCommodityByType.get(typeUsed.getShortKey());
                        if (soldCommodity != null) {
                            ActionCommodity actionCommodity = commodityByType.computeIfAbsent(
                                    typeUsed.getShortKey(), k -> new ActionCommodity());
                            actionCommodity.addUsed(typeUsed.getFloatValue());
                            actionCommodity.addCapacity(soldCommodity.getCapacity());
                        }
                    });
                }
            });
        });
        // discard
        boughtCommodityUsedByEntity = null;
    }

    /**
     * Finish processing commodities and percentiles. Reduce memory by trimming the map.
     */
    public void finish() {
        soldCommodityByEntity.trim();
        soldCommodityByEntity.values().forEach(Short2ObjectOpenHashMap::trim);
        soldPercentilesByEntity.trim();
        soldPercentilesByEntity.values().forEach(Object2DoubleOpenHashMap::trim);

        boughtCommodityByEntity.trim();
        boughtCommodityByEntity.values().forEach(Short2ObjectOpenHashMap::trim);
        boughtPercentilesByEntity.trim();
        boughtPercentilesByEntity.values().forEach(Object2DoubleOpenHashMap::trim);
    }

    @Override
    public String toString() {
        final Set<UICommodityType> soldComms = new HashSet<>();
        soldPercentilesByEntity.values().forEach(commToPercentile -> commToPercentile.keySet()
                .forEach((CommType commType) -> soldComms.add(
                        UICommodityType.fromType(commType.type))));
        return FormattedString.format("SOLD: {} entities with percentiles for commodities {}",
                soldPercentilesByEntity.size(), soldComms.stream()
                        .map(UICommodityType::displayName)
                        .collect(Collectors.joining(", ")));
    }

    void processSoldCommodity(long entityId, CommoditySoldDTO commSold) {
        // cloud volumes may only have capacity set for some commodity like StorageAccess,
        // but not used, but we still want to record it since projected capacity may be different
        if (commSold.hasCapacity()) {
            putSoldCommodity(entityId, commSold.getCommodityType(), commSold.getUsed(), commSold.getCapacity());
        }

        if (commSold.getHistoricalUsed().hasPercentile()) {
            putSoldPercentile(entityId, commSold.getCommodityType(),
                    commSold.getHistoricalUsed().getPercentile());
        }
    }

    void putSoldCommodity(long entityId, @Nonnull CommodityType commodityType, double used, double capacity) {
        Short2ObjectOpenHashMap<ActionCommodity> actionCommodityMap = soldCommodityByEntity.computeIfAbsent(entityId, k -> new Short2ObjectOpenHashMap<>());
        ActionCommodity actionCommodity = actionCommodityMap.computeIfAbsent((short)commodityType.getType(), k -> new ActionCommodity());
        actionCommodity.addUsed((float)used);
        actionCommodity.addCapacity((float)capacity);
    }

    void putSoldPercentile(long entityId, CommodityType commodityType, double percentile) {
        soldPercentilesByEntity.computeIfAbsent(entityId, k -> {
            Object2DoubleOpenHashMap<CommType> newMap = new Object2DoubleOpenHashMap<>();
            newMap.defaultReturnValue(NO_PERCENTILE_VALUE);
            return newMap;
        }).put(getCommType(commodityType),
                // Convert to percent, and round to two decimals,
                // from something like "0.10324" to "10.32".
                Math.round(percentile * 10000) / 100.0);
    }

    /**
     * Process bought commodity in source topology. Only selected bought commodities for specific
     * providers are kept, so there should only be one commodity for each type.
     *
     * @param entityId entity oid
     * @param providerId the provider of the bought commodity
     * @param commodityBoughtDTO bought commodity
     */
    public void processBoughtCommodity(long entityId, long providerId, CommodityBoughtDTO commodityBoughtDTO) {
        if (commodityBoughtDTO.hasUsed()) {
            boughtCommodityUsedByEntity.computeIfAbsent(entityId, k -> new Long2ObjectOpenHashMap<>())
                    .computeIfAbsent(providerId, k -> new Short2FloatOpenHashMap())
                    .put((short)commodityBoughtDTO.getCommodityType().getType(), (float)commodityBoughtDTO.getUsed());
        }

        if (commodityBoughtDTO.getHistoricalUsed().hasPercentile()) {
            putBoughtPercentile(entityId, commodityBoughtDTO.getCommodityType(),
                    commodityBoughtDTO.getHistoricalUsed().getPercentile());
        }
    }

    /**
     * Add the given bought commodity for the given entity in projected topology. Only selected
     * bought commodities for specific providers are kept, so there should only be one commodity
     * for each type.
     *
     * @param entityId entity oid
     * @param commodityType type of the bought commodity
     * @param used used of bought commodity
     * @param capacity capacity of bought commodity
     */
    public void putBoughtCommodity(long entityId, @Nonnull CommodityType commodityType, float used, float capacity) {
        ActionCommodity actionCommodity = boughtCommodityByEntity
                .computeIfAbsent(entityId, k -> new Short2ObjectOpenHashMap<>())
                .computeIfAbsent((short)commodityType.getType(), k -> new ActionCommodity());
        actionCommodity.addUsed(used);
        actionCommodity.addCapacity(capacity);
    }

    /**
     * Add the percentile value for given bought commodity on the given entity.
     *
     * @param entityId entity oid
     * @param commodityType type of the bought commodity
     * @param percentile percentile value of bought commodity
     */
    public void putBoughtPercentile(long entityId, CommodityType commodityType, double percentile) {
        boughtPercentilesByEntity.computeIfAbsent(entityId, k -> {
            Object2DoubleOpenHashMap<CommType> newMap = new Object2DoubleOpenHashMap<>();
            newMap.defaultReturnValue(NO_PERCENTILE_VALUE);
            return newMap;
        }).put(getCommType(commodityType),
                // Convert to percent, and round to two decimals,
                // from something like "0.10324" to "10.32".
                Math.round(percentile * 10000) / 100.0);
    }

    /**
     * Get the {@link CommType} object that represents a {@link CommodityType}.
     *
     * @param commodityType The {@link CommodityType}.
     * @return The {@link CommType}.
     */
    @Nonnull
    private CommType getCommType(CommodityType commodityType) {
        return commoditiesOfType.computeIfAbsent(commodityType.getType(), CommoditiesOfType::new)
                .getCommType(commodityType);
    }

    /**
     * Contains the {@link CommType}s associated with a particular type of commodity (i.e.
     * all the key-specific objects). Used to reuse references to unique {@link CommType} objects.
     */
    private static class CommoditiesOfType {
        private final CommType noKeyComm;
        private final Map<String, CommType> commsByKey = new HashMap<>();

        private CommoditiesOfType(int commType) {
            this.noKeyComm = new CommType(commType, null);
        }

        @Nonnull
        public CommType getCommType(CommodityType commodityType) {
            if (!commodityType.hasKey()) {
                return noKeyComm;
            } else {
                return commsByKey.computeIfAbsent(commodityType.getKey(),
                    k -> new CommType(noKeyComm.type, k));
            }
        }
    }

    /**
     * The equivalent of a {@link CommodityType}. Takes up less space in memory than {@link CommodityType},
     * and allows us to reuse references to the same commodity type object across multiple entities.
     * Since we keep {@link TopologyActionCommodityData} cached across broadcasts this can be valuable.
     */
    @VisibleForTesting
    static class CommType {
        private final int type;
        private final String key;

        @VisibleForTesting
        CommType(int type, @Nullable String key) {
            this.type = type;
            this.key = key;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            if (other instanceof CommType) {
                return ((CommType)other).type == type
                    && Objects.equals(((CommType)other).key, key);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 31 * Integer.hashCode(type) + Objects.hashCode(key);
        }
    }
}
