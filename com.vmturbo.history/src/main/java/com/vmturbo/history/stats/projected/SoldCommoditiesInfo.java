package com.vmturbo.history.stats.projected;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.history.stats.projected.AccumulatedCommodity.AccumulatedSoldCommodity;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.reports.db.CommodityTypes;

/**
 * This class contains information about commodities sold by entities in a topology.
 * <p>
 * It's immutable, because for any given topology the sold commodities don't change.
 */
@Immutable
class SoldCommoditiesInfo {
    private static final Logger logger = LogManager.getLogger();

    /**
     * (commodity name) -> ( (entity id) -> (DTO describing commodity sold) )
     * <p>
     * Each entity should only have one {@link CommoditySoldDTO} for a given commodity name.
     * This may not be true in general, because the commodity name string doesn't take into
     * account the commodity spec keys. But the stats API doesn't currently support commodity
     * spec keys anyway.
     */
    private final Map<String, Map<Long, CommoditySoldDTO>> soldCommodities;

    private SoldCommoditiesInfo(
            @Nonnull final Map<String, Map<Long, CommoditySoldDTO>> soldCommodities) {
        this.soldCommodities = Collections.unmodifiableMap(soldCommodities);
    }

    @Nonnull
    static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Get the accumulated information about a particular commodity sold by a set of entities.
     *
     * @param commodityName The name of the commodity. The names are derived from
     *           {@link CommodityTypes}. This is not ideal - we should consider using the
     *           {@link CommodityType} enum directly.
     * @param targetEntities The entities to get the information from. If empty, accumulate
     *                       information from the whole topology.
     * @return An optional containing the accumulated {@link StatRecord}, or an empty optional
     *         if there is no information for the commodity over the target entities.
     */
    @Nonnull
    Optional<StatRecord> getAccumulatedRecord(@Nonnull final String commodityName,
                                              @Nonnull final Set<Long> targetEntities) {
        final Map<Long, CommoditySoldDTO> soldByEntityId =
                soldCommodities.get(commodityName);
        final AccumulatedSoldCommodity overallCommoditySold =
                new AccumulatedSoldCommodity(commodityName);
        if (soldByEntityId == null) {
            // If this commodity is not sold, we don't return anything.
        } else if (targetEntities.isEmpty()) {
            soldByEntityId.forEach((entityId, commoditySold) -> {
                overallCommoditySold.recordSoldCommodity(commoditySold);
            });
        } else {
            // We have some number of entities, and we accumulate the stats from all the entities.
            targetEntities.forEach(entityId -> {
                final CommoditySoldDTO commoditySoldDTO = soldByEntityId.get(entityId);
                if (commoditySoldDTO != null) {
                    overallCommoditySold.recordSoldCommodity(commoditySoldDTO);
                } else {
                    logger.warn("Requested commodity {} not sold by entity {}", commodityName, entityId);
                }
            });
        }
        return overallCommoditySold.toStatRecord();
    }

    @Nonnull
    public Optional<Double> getCapacity(@Nonnull final String commodityName,
                                        @Nonnull final Long providerId) {
        return Optional.ofNullable(soldCommodities.get(commodityName))
                .map(providers -> providers.get(providerId))
                .map(CommoditySoldDTO::getCapacity);
    }

    /**
     * A builder to construct the {@link SoldCommoditiesInfo}.
     */
    static class Builder {

        private final Map<String, Map<Long, CommoditySoldDTO>> soldCommodities =
                new HashMap<>();

        private Builder() {}

        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entity.getCommoditySoldListList().forEach(commoditySold -> {
                final String commodity = HistoryStatsUtils.formatCommodityName(
                        commoditySold.getCommodityType().getType());
                final Map<Long, CommoditySoldDTO> entitySellers =
                        soldCommodities.computeIfAbsent(commodity, k -> new HashMap<>());
                // An entity may sell each commodity more than once, if the
                // commodity key is different. At the time of this writing the stats
                // API doesn't specify commodity keys, so we ignore them.
                final CommoditySoldDTO prev = entitySellers.put(entity.getOid(), commoditySold);
                if (prev != null) {
                    logger.warn("Entity {} selling commodity {} more than once. Previous: {}",
                            entity.getOid(),
                            commoditySold.getCommodityType(),
                            prev.getCommodityType());
                }
            });
            return this;
        }

        @Nonnull
        SoldCommoditiesInfo build() {
            return new SoldCommoditiesInfo(soldCommodities);
        }
    }

}
