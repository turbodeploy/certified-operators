package com.vmturbo.cost.component.savings;

import java.sql.Timestamp;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;

/**
 * Keeps stats (like REALIZED_SAVINGS) of a particular type, for an entity, at a given timestamp.
 */
public class EntitySavingsStats {
    /**
     * VM/DB/Volume id for which stats is stored.
     */
    private final long entityId;

    /**
     * Stats timestamp, e.g 14:00:00 for hourly stats.
     */
    private final long timestamp;

    /**
     * Type of stats, e.g CUMULATIVE_REALIZED_SAVINGS.
     */
    private final EntitySavingsStatsType type;

    /**
     * Value (cost price) of stats.
     */
    private final Double value;

    /**
     * Creates a new one.
     *
     * @param entityId VM/DB/Volume id for which stats is stored.
     * @param timestamp Stats timestamp, e.g 14:00:00 for hourly stats.
     * @param statsType Type of stats, e.g REALIZED_INVESTMENTS.
     * @param statsValue Value of stats field.
     */
    public EntitySavingsStats(long entityId, long timestamp, EntitySavingsStatsType statsType,
            @Nonnull Double statsValue) {
        this.entityId = entityId;
        this.timestamp = timestamp;
        this.type = statsType;
        this.value = statsValue;
    }

    /**
     * VM/DB/Volume id for which stats is stored.
     *
     * @return VM/DB/Volume id for which stats is stored.
     */
    public long getEntityId() {
        return entityId;
    }

    /**
     * Stats timestamp, e.g 14:00:00 for hourly stats.
     *
     * @return Stats timestamp, e.g 14:00:00 for hourly stats.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Gets type of the stats being stored.
     *
     * @return Stats type.
     */
    public EntitySavingsStatsType getType() {
        return type;
    }

    /**
     * Gets the stats value.
     *
     * @return Value of stats.
     */
    @Nonnull
    public Double getValue() {
        return value;
    }

    /**
     * Checking equality, entityId and timestamp together make up unique key.
     *
     * @param o Other stats object.
     * @return Whether this is equal to other.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntitySavingsStats that = (EntitySavingsStats)o;
        return entityId == that.entityId && timestamp == that.timestamp && type == that.type;
    }

    /**
     * Hash of this instance.
     *
     * @return Hash value.
     */
    @Override
    public int hashCode() {
        return Objects.hash(entityId, timestamp, type);
    }

    /**
     * To string for this instance.
     *
     * @return To string value.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityOID", entityId)
                .append("timestamp", new Timestamp(timestamp).toLocalDateTime())
                .append("type", type)
                .append("value", value)
                .toString();
    }
}
