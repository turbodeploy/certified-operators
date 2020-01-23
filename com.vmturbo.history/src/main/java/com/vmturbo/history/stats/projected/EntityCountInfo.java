package com.vmturbo.history.stats.projected;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.stats.StatsAccumulator;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Information about entity counts and ratios in a topology.
 * <p>
 * It's immutable, because for any given topology the entity counts don't change.
 */
@Immutable
class EntityCountInfo {
    /**
     * (entity type) -> (number of active entities of the type in the topology).
     */
    private final Map<Integer, Long> entityCountsByType;

    /**
     * This constructor should only be used for testing. Otherwise, use
     * {@link EntityCountInfo#newBuilder()}.
     *
     * @param entityCountsByType The map of entity counts.
     */
    @VisibleForTesting
    EntityCountInfo(@Nonnull final Map<Integer, Long> entityCountsByType) {
        this.entityCountsByType = Collections.unmodifiableMap(entityCountsByType);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    @VisibleForTesting
    static StatRecord constructPostProcessRecord(@Nonnull final String statName,
                                                 final float value) {
        final StatValue val = StatValue.newBuilder()
                .setAvg(value)
                .setMax(value)
                .setMin(value)
                .setTotal(value)
                .build();
        return StatRecord.newBuilder()
                .setName(statName)
                .setCapacity(StatsAccumulator.singleStatValue(value))
                .setCurrentValue(value)
                .setValues(val)
                .setUsed(val)
                .setPeak(val)
                .setRelation(RelationType.METRICS.getLiteral())
                .build();
    }

    @VisibleForTesting
    long entityCount(@Nonnull final EntityType entityType) {
        return entityCountsByType.getOrDefault(entityType.getNumber(), 0L);
    }

    @Nonnull
    private StatRecord entityRatio(@Nonnull final String statName,
                                   @Nonnull final EntityType numeratorType,
                                   @Nonnull final EntityType denominatorType) {
        long numerator = entityCount(numeratorType);
        long denominator = entityCount(denominatorType);
        return constructPostProcessRecord(statName, denominator == 0 ? 0 : (float)numerator / denominator);
    }

    boolean isCountStat(@Nonnull final String statName) {
        return HistoryStatsUtils.countPerSEsMetrics.contains(statName) ||
                HistoryStatsUtils.countSEsMetrics.containsValue(statName);
    }

    @Nonnull
    @VisibleForTesting
    Optional<StatRecord> getCountRecord(@Nonnull final String statName) {
        switch (statName) {
            case StringConstants.NUM_VMS_PER_HOST:
                return Optional.of(entityRatio(statName, EntityType.VIRTUAL_MACHINE,
                        EntityType.PHYSICAL_MACHINE));
            case StringConstants.NUM_VMS_PER_STORAGE:
                return Optional.of(entityRatio(statName, EntityType.VIRTUAL_MACHINE,
                        EntityType.STORAGE));
            case StringConstants.NUM_CNT_PER_HOST:
                return Optional.of(entityRatio(statName, EntityType.CONTAINER,
                        EntityType.PHYSICAL_MACHINE));
            case StringConstants.NUM_CNT_PER_STORAGE:
                return Optional.of(entityRatio(statName, EntityType.CONTAINER,
                        EntityType.STORAGE));
            case StringConstants.NUM_HOSTS:
                return Optional.of(constructPostProcessRecord(statName,
                        entityCount(EntityType.PHYSICAL_MACHINE)));
            case StringConstants.NUM_VMS:
                return Optional.of(constructPostProcessRecord(statName,
                        entityCount(EntityType.VIRTUAL_MACHINE)));
            case StringConstants.NUM_STORAGES:
                return Optional.of(constructPostProcessRecord(statName,
                        entityCount(EntityType.STORAGE)));
            case StringConstants.NUM_CONTAINERS:
                return Optional.of(constructPostProcessRecord(statName,
                        entityCount(EntityType.CONTAINER)));
            default:
                return Optional.empty();
        }
    }

    /**
     * Builder class to construct an immutable {@link EntityCountInfo}.
     */
    static class Builder {
        private final Map<Integer, Long> entityCountsByType = new HashMap<>();

        private Builder() {}

        @Nonnull
        Builder addEntity(@Nonnull final TopologyEntityDTO entity) {
            entityCountsByType.put(entity.getEntityType(),
                    entityCountsByType.getOrDefault(entity.getEntityType(), 0L) + 1);
            return this;
        }

        @Nonnull
        EntityCountInfo build() {
            return new EntityCountInfo(entityCountsByType);
        }
    }
}
