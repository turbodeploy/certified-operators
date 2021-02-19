package com.vmturbo.extractor.action.percentile;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.api.FormattedString;

/**
 * Captures per-entity-per-commodity percentile data scraped from a source or projected topology.
 */
class TopologyPercentileData {
    private final Int2ObjectMap<CommoditiesOfType> commoditiesOfType = new Int2ObjectOpenHashMap<>();

    private final Long2ObjectOpenHashMap<Object2DoubleMap<CommType>> soldPercentilesByEntity =
            new Long2ObjectOpenHashMap<>();

    private static final double NO_PERCENTILE_VALUE = -1.0;

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

    @Override
    public String toString() {
        final Set<UICommodityType> soldComms = new HashSet<>();
        soldPercentilesByEntity.values().forEach(commToPercentile -> {
            commToPercentile.keySet().forEach((CommType commType) -> soldComms.add(UICommodityType.fromType(commType.type)));
        });
        return FormattedString.format("SOLD: {} entities with percentiles for commodities {}",
                soldPercentilesByEntity.size(), soldComms.stream()
                        .map(UICommodityType::displayName)
                        .collect(Collectors.joining(", ")));
    }

    void putSoldPercentile(long entityId, CommodityType commodityType, double percentile) {
        soldPercentilesByEntity.computeIfAbsent(entityId, k -> {
            Object2DoubleMap<CommType> newMap = new Object2DoubleOpenHashMap<>();
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
     * Since we keep {@link TopologyPercentileData} cached across broadcasts this can be valuable.
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
