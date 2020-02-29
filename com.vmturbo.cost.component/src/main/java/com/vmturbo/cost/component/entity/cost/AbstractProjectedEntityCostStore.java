package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.StatValue;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.cost.component.util.EntityCostFilter;

/**
 * Abstract storage for projected per-entity costs.
 */
public class AbstractProjectedEntityCostStore {
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Returns logger.
     *
     * @return logger.
     */
    protected Logger getLogger() {
        return logger;
    }

    /**
     * Returned aggregated/merged {@link StatRecord} based on params.
     *
     * @param groupBys List of properties to groupBy.
     * @param statRecords list of {@link StatRecord} to merge.
     * @return List of merged {@link StatRecord}.
     */
    @Nonnull
    @VisibleForTesting
    public Collection<StatRecord> aggregateByGroup(@Nonnull final Collection<GroupBy> groupBys,
        @Nonnull final Collection<StatRecord> statRecords) {
        //creating a map indexed by group propertyNames.
        final Map<String, StatRecord> groupedStatRecord = Maps.newHashMap();
        statRecords.forEach(item -> {
            final List<String> keys = new ArrayList<>();
            if (groupBys.contains(GroupBy.COST_CATEGORY)) {
                keys.add(item.getCategory().name());
            }
            if (groupBys.contains(GroupBy.ENTITY_TYPE)) {
                keys.add(String.valueOf(item.getAssociatedEntityType()));
            }
            if (groupBys.contains(GroupBy.ENTITY)) {
                keys.add(String.valueOf(item.getAssociatedEntityId() ));
            }
            groupedStatRecord.compute(Strings.join(keys, '-'), (currentKey, currentVal) -> mergeStats(item, currentVal));
        });
        return groupedStatRecord.values();
    }

    /**
     * Merge 2 StatRecords values and other Arrtibs into one.
     *
     * @param newIncomingValue new StatRecord.
     * @param currentVal       current StatRecord.
     * @return Merged StatRecord with aggregated values.
     */
    @Nonnull
    @VisibleForTesting
    protected StatRecord mergeStats(@Nonnull final StatRecord newIncomingValue,
                                 @Nullable final StatRecord currentVal) {
        if (currentVal == null) {
            return newIncomingValue;
        }
        final Builder currentBuilder = currentVal.toBuilder();

        if (newIncomingValue.getAssociatedEntityId() != currentVal.getAssociatedEntityId()) {
            currentBuilder.clearAssociatedEntityId();
        }
        if (newIncomingValue.getAssociatedEntityType() != currentVal.getAssociatedEntityType()) {
            currentBuilder.clearAssociatedEntityType();
        }
        if (!newIncomingValue.getCategory().equals(currentVal.getCategory())) {
            currentBuilder.clearCategory();
        }
        if (!currentBuilder.hasAssociatedEntityId() &&
                !currentBuilder.hasAssociatedEntityType() &&
                !currentBuilder.hasCategory()) {
            getLogger().error("None of the common fields matched. Might lead to wrong merged cost entities. {},  {}",
                    currentVal, newIncomingValue);
        }
        return currentBuilder.setValues(StatValue.newBuilder()
                .setTotal(newIncomingValue.getValues().getTotal() + currentVal.getValues().getTotal())
                .setMax(Math.max(newIncomingValue.getValues().getMax(), currentVal.getValues().getMax()))
                .setMin(Math.min(newIncomingValue.getValues().getMin(), currentVal.getValues().getMin()))
                .setAvg((newIncomingValue.getValues().getAvg() + currentVal.getValues().getAvg()) / 2)
                .build())
                .build();
    }

    /**
     * Applies filter to entity cost.
     *
     * @param entityCost entity cost
     * @param filter filter to apply
     * @return {@link Optional} with filtered entity cost.
     */
    protected Optional<EntityCost> applyFilter(EntityCost entityCost, EntityCostFilter filter) {
        if (filter.getEntityFilters().isPresent() &&
                !filter.getEntityFilters().get().contains(entityCost.getAssociatedEntityId())) {
            return Optional.empty();
        }
        // If entity in question is not any of the requested types ignore it
        if (filter.getEntityTypeFilters().isPresent()
            && !filter.getEntityTypeFilters().get().contains(entityCost.getAssociatedEntityType())) {
            return Optional.empty();
        }

        final EntityCost.Builder builder = entityCost.toBuilder();

        final List<EntityCost.ComponentCost> filteredComponentCosts = entityCost.getComponentCostList()
                        .stream()
                        .filter(filter::filterComponentCost)
                        .collect(Collectors.toList());

        if (filteredComponentCosts.isEmpty()) {
            return Optional.empty();
        }

        builder.clearComponentCost()
                        .addAllComponentCost(filteredComponentCosts);

        return Optional.of(builder.build());
    }

}
